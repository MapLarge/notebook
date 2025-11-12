import io
import time
import json
import zmq
import logging
import uuid
from threading import Timer
from multiprocessing import Process, Manager, Queue, Event as MP_Event
import pandas as pd
import queue
import hashlib
import sys
import os
import tempfile
import signal

_recordCounts = {}
_REQUEST_TRIES = 5

class ColumnInfo:
	def __repr__(self):
		return "ColumnInfo({0}, {1})".format(self.name, self.type)

	def __init__(self, name, type):
		self.name = name
		self.type = type

class TableInfo:
	def __repr__(self):
		return "TableInfo({0}, {1} columns])".format(self.name, len(self.columns))

	def __init__(self, name, columns):
		self.name = name
		self.columns = columns

class Event(object):
	def __init__(self):
		self.__callback = []
	def __iadd__(self, handler):
		self.__callback.append(handler)
		return self
	def __isub__(self, handler):
		self.__callback.remove(handler)
		return self
	def __call__(self, *args, **kwargs):
		for handler in self.__callback:
			handler(*args, **kwargs)

class ProxyResponse:
	def __init__(self, response: io.BytesIO):
		self.response = response

	def text(self, encoding='utf-8'):
		return self.response.getvalue().decode(encoding)

	def json(self):
		from json import loads
		return loads(self.response.getvalue())

	def bytes(self):
		return self.response.getvalue()

	def save(self, file_path: str):
		with open(file_path, 'wb') as f:
			f.write(self.response.getvalue())

class MapLargeClient(object):
	def __init__(self, port: int, hbReqPort: int, hbRepPort: int):
		logging.info(f"Creating MapLargeClient instance: ports main={port}, hbReq={hbReqPort}, hbRep={hbRepPort}, process PID={os.getpid()}")

		self._runProcess: Process | None = None

		# Create ZMQ context and socket for sending requests (reused across all requests)
		self._zmq_context = zmq.Context()
		self._queuePushSocket = None
		self._queueSocketConnected = False

		self._manager = Manager()
		self._shared_running = self._manager.Value('b', False)
		self._shared_connectedSelf = self._manager.Value('b', False)
		self._shared_connectedRemote = self._manager.Value('b', False)
		self._shared_exception = self._manager.Value('c', "")

		self._should_continue_event = MP_Event()

		# Queue for receiving responses from poller process
		self._response_queue = Queue()

		self._port = port
		self._hbReqPort = hbReqPort
		self._hbRepPort = hbRepPort

		ipc_path = os.path.join(tempfile.gettempdir(), f"maplarge-queue-{os.getpid()}-{id(self)}")
		if os.path.exists(ipc_path):
			try:
				os.remove(ipc_path)
			except:
				pass

		self._queue_address = f"ipc://{ipc_path}"
		self._requests = {}

		signal.signal(signal.SIGTERM, self._handle_sigterm)

	@property
	def connected(self):
		return self._shared_connectedSelf.value or self._shared_connectedRemote.value

	@property
	def running(self):
		return self._shared_running.value

	def _request(self, action, message, dataBytes=None, attempt=0, timeoutSeconds=90):
		if attempt > _REQUEST_TRIES:
			raise Exception('Request failed')

		logging.debug(f"Sending request {action}")
		jsonMessage = json.dumps(message)

		id = str(uuid.uuid4())

		messageParts = [str.encode("2"), str.encode(id), str.encode(action), str.encode(jsonMessage)]

		if dataBytes is not None:
			chunkSize = 2**20
			chunks = []
			while True:
				chunk = dataBytes.read(chunkSize)
				if not chunk:
					break
				chunks.append(chunk)
			messageParts.extend(chunks)

		self._requests[id] = { "complete": False, "result": None, "error": None }

		if self._queuePushSocket is None:
			self._queuePushSocket = self._zmq_context.socket(zmq.PUSH)
			self._queuePushSocket.setsockopt(zmq.LINGER, 0)
			self._queuePushSocket.setsockopt(zmq.SNDTIMEO, 5000)
			self._queueSocketConnected = False

		if not self._queueSocketConnected:
			max_connect_retries = 10
			for retry in range(max_connect_retries):
				try:
					self._queuePushSocket.connect(self._queue_address)
					self._queueSocketConnected = True
					break
				except Exception:
					if retry < max_connect_retries - 1:
						time.sleep(0.1)
					else:
						self._queuePushSocket = None
						raise

		try:
			self._queuePushSocket.send_multipart(messageParts, copy=False)
		except Exception:
			try:
				self._queuePushSocket.close()
			except:
				pass
			self._queuePushSocket = None
			self._queueSocketConnected = False
			raise

		timeout = Timer(timeoutSeconds, self._clearRequest, args=[id])
		timeout.start()

		while(id in self._requests and self._requests[id]["complete"] == False):
			# Check for responses from the poller process
			try:
				response = self._response_queue.get_nowait()
				response_id, messageType, message_data = response
				if response_id in self._requests:
					self._handleReceipt(response_id, messageType, message_data)
			except queue.Empty:
				pass
			time.sleep(0.25)

		timeout.cancel()

		# Check for exceptions from poller process
		if self._shared_exception.value:
			raise Exception(self._shared_exception.value)
		elif not id in self._requests:
			raise Exception(f"Request {action} timed out")
		elif self._requests[id]["complete"] == False:
			raise Exception(f"No response from server")
		elif self._requests[id]["error"] is not None:
			raise Exception("Response from server: " + self._requests[id]["error"])

		logging.debug(f"Request {action} successful")
		return self._requests[id]["result"]

	def _clearRequest(self, id):
		if id in self._requests:
			del self._requests[id]

	def stop(self):
		logging.debug("Stopping poll process")

		self._should_continue_event.set()
		self._shared_running.value = False

		if self._runProcess is not None and self._runProcess.is_alive():
			logging.info(f"Joining process PID {self._runProcess.pid}")
			self._runProcess.join(timeout=5)
			if self._runProcess.is_alive():
				logging.warning(f"Process {self._runProcess.pid} did not terminate, terminating forcefully")
				self._runProcess.terminate()
				self._runProcess.join(timeout=2)
				if self._runProcess.is_alive():
					logging.error(f"Process {self._runProcess.pid} still alive, killing")
					self._runProcess.kill()
					self._runProcess.join()

		logging.debug("Poll process stopped")

		# Clean up ZMQ socket and context
		if self._queuePushSocket is not None:
			try:
				self._queuePushSocket.close()
				logging.debug("Queue PUSH socket closed")
			except:
				pass
			self._queuePushSocket = None
			self._queueSocketConnected = False

		if self._zmq_context is not None:
			try:
				self._zmq_context.term()
				logging.debug("ZMQ context terminated")
			except:
				pass
			self._zmq_context = None

		# Clean up IPC socket file if using IPC
		if self._queue_address is not None and self._queue_address.startswith('ipc://'):
			# Extract path from ipc:// format (handles both ipc://path and ipc:///path)
			ipc_path = self._queue_address.replace('ipc://', '').lstrip('/')
			if ipc_path and os.path.exists(ipc_path):
				try:
					os.remove(ipc_path)
					logging.debug(f"Removed IPC socket file: {ipc_path}")
				except Exception as e:
					logging.debug(f"Could not remove IPC socket file {ipc_path}: {e}")
					pass

		self._shared_running.value = False
		self._shared_connectedSelf.value = False
		self._shared_connectedRemote.value = False

		if self._shared_exception.value:
			temp_exception = self._shared_exception.value
			self._shared_exception.value = ""
			raise Exception(temp_exception)

	def start(self):
		# Check if already running and process is actually alive
		if self._shared_running.value and self._runProcess is not None:
			if self._runProcess.is_alive():
				return
			else:
				logging.warning("Process was marked as running but is not alive, cleaning up")
				try:
					self.stop()
				except:
					pass
		elif not self._shared_running.value and self._runProcess is not None:
			if self._runProcess.is_alive():
				self._shared_running.value = True
				return
			try:
				self.stop()
			except:
				pass

		# Recreate ZMQ context if it was terminated
		if self._zmq_context is None or self._zmq_context.closed:
			logging.debug("Recreating ZMQ context")
			self._zmq_context = zmq.Context()
			self._queuePushSocket = None
			self._queueSocketConnected = False

		self._should_continue_event.clear()
		self._shared_connectedSelf.value = False
		self._shared_connectedRemote.value = False
		self._shared_running.value = True
		self._shared_exception.value = ""

		logging.info(f"Starting poll process with ports: main={self._port}, hbReq={self._hbReqPort}, hbRep={self._hbRepPort}, queue={self._queue_address}")
		self._runProcess = Process(target=_run_poll_loop, args=(
			self._port, self._hbReqPort, self._hbRepPort,
			self._queue_address, self._shared_connectedSelf,
			self._shared_connectedRemote, self._shared_exception,
			self._should_continue_event, self._response_queue,
			self._shared_running
		))
		self._runProcess.start()
		logging.info(f"Poll process started with PID {self._runProcess.pid}")

	def _ping(self):
		self._request()

	def _handleReceipt(self, id, messageType, message):
		if messageType == "ServerError":
			if id in self._requests:
				self._requests[id]["error"] = message
		else:
			if id in self._requests:
				self._requests[id]["result"] = message

		if id in self._requests:
			self._requests[id]["complete"] = True

	def _handle_sigterm(self, signum, frame):
		try:
			logging.info(f"Received signal {signum}, stopping poll process")
			self.stop()
		except:
			pass

		raise SystemExit(0)

	def SetOutputSchema(self, copyFromSource=None, columns=None):
		request = {}

		if copyFromSource is None and columns is None:
			raise Exception("copyFromSource or columns is required")

		if copyFromSource is not None:
			request["copyFromSource"] = copyFromSource
		if columns is not None:
			request["newColumns"] = [c["name"] for c in columns]
			request["newColumnTypes"] = [c["type"] for c in columns]

		self._request("SetOutputSchema", request)

	def GetInputSchema(self, inputs=None):
		message = self._request("GetInputSchema", { "sources": inputs })

		resp = json.loads(message)

		output = {}

		for table in resp["sources"]:
			columns = [ColumnInfo(name, type) for name, type in zip(resp["sources"][table]["columnNames"], resp["sources"][table]["columnTypes"])]

			output[table] = TableInfo(table, columns)

		return output

	def GetData(self, source, start, take, columns=None, filter=None):
		logging.debug("GetData called on {source} with start {start} and take {take}".format(source = source, start = start, take = take))
		request = { "source": source, "start": start, "take": take }

		if columns is not None:
			request["columns"] = columns

		if filter is not None:
			if isinstance(filter, str):
				request["filter"] = filter
			else:
				request["filter"] = json.dumps(filter)

		message = self._request("GetData", request)

		logging.debug("Data Received")

		resp = json.loads(message)
		df = pd.DataFrame(resp["data"])

		logging.debug("Data Loaded")

		# save record count for later
		_setRecordCountsCache(source, resp["totals"]["Records"], filter)

		return df

	def WriteData(self, table, data, append=False, append_mode=None, primary_key=None):
		if append and primary_key is None and (append_mode is not None or "default"):
			raise Exception("Primary key is required in order to use append_mode=" + append_mode)

		request = {
			"table": table,
			"data": { column: [None if pd.isna(d) else d for d in data[column]] for column in data },
			"append": append or False,
			"appendMode": append_mode if append else None,
			"primaryKey": primary_key
		}

		self._request("WriteData", request)

	def GetRecordCount(self, source, filter=None):
		isCached, recordCount = _tryReadRecordCountsCache(source, filter)
		if isCached:
			return recordCount

		self.GetData(source, 0, 1, filter=filter)
		return _readRecordCountsCache(source, filter)

	def DataIterator(self, source, page_size=100000, columns=None, filter=None):
		logging.debug("DataIterator called on {source} with page_size {page_size}".format(source = source, page_size = page_size))
		start = 0
		total_records = self.GetRecordCount(source, filter)

		while start < total_records:
			df = self.GetData(source, start, page_size, columns = columns, filter = filter)

			for index, row in df.iterrows():
				yield row

			start += page_size

	def SetOutput(self, data, additive=False, strictSchema=False):
		try:
			import geopandas
		except ImportError:
			geopandas = None

		if geopandas and isinstance(data, geopandas.GeoDataFrame):
			data = data.to_wkt()

		request = {
			"data": { column: [None if pd.isna(d) else d for d in data[column]] for column in data },
			"additive": additive,
			"strictSchema": strictSchema
		}

		self._request("SetOutput", request, timeoutSeconds=180)

	def ClearOutput(self):
		self._request("ClearOutput", {})

	def SetBinaryData(self, column, rowIndex, data, format=None, copyGeoFrom=None, mediaType=None):
		request = {
			"column": column,
			"rowIndex": rowIndex,
			"format": format,
			"copyGeoFrom": copyGeoFrom,
			"mediaType": mediaType
		}

		self._request("SetBinaryData", request, data)

	def ListFiles(self, path, recursive=False):
		request = {
			"path": path,
			"recursive": recursive
		}

		message = self._request("ListFiles", request)

		resp = json.loads(message)
		return pd.DataFrame(resp["files"])

	def GetFileBytes(self, path):
		request = {
			"path": path
		}

		message = self._request("GetFileBytes", request)

		combined_bytes = bytearray()
		for m in message:
			combined_bytes += m

		handle = io.BytesIO(combined_bytes)

		return handle

	def GetBinaryData(self, source, column, rowIndex = -1, format = None, maxDimension = None, mediaKeyColumns = None, mediaKeyValues = None, filter=None):
		request = {
			"source": source,
			"column": column,
			"rowIndex": rowIndex,
			"mediaKeyColumns": None if mediaKeyColumns is None else (mediaKeyColumns if isinstance(mediaKeyColumns, list) else [mediaKeyColumns]),
			"mediaKeyValues": None if mediaKeyValues is None else (mediaKeyValues if isinstance(mediaKeyValues, list) else [mediaKeyValues])
		}

		if format is not None:
			request["format"] = format

		if maxDimension is not None:
			request["maxDimension"] = maxDimension

		if filter is not None:
			if isinstance(filter, str):
				request["filter"] = filter
			else:
				request["filter"] = json.dumps(filter)

		message = self._request("GetBinaryData", request)

		combined_bytes = bytearray()
		for m in message:
			combined_bytes += m

		handle = io.BytesIO(combined_bytes)

		return handle

	def SaveFile(self, path, savePath):
		contents = self.GetFileBytes(path)

		with open(savePath, "wb") as f:
			f.write(contents.getbuffer())

	def GetWorkloadInfo(self, workloadName=None, workloadId=None):
		if workloadName is None and workloadId is None:
			raise Exception("workloadName or workloadId is required.")

		request = {
			"workloadId": workloadId,
			"workloadName": workloadName
		}

		message = self._request("GetWorkloadInfo", request)
		resp = json.loads(message)
		error = resp["Error"]
		if error is not None:
			raise Exception(error)

		return resp["Workload"]

	def GetUrl(self, timeoutSeconds=None):
		return self._request("GetUrl", {
			"timeoutSeconds": timeoutSeconds
		})

	def GetAuth(self, originalToken=None, durationSeconds=None):
		message = self._request("GetAuth", {
			"originalToken": originalToken,
			"durationSeconds": durationSeconds
		})

		return json.loads(message)

	def ProxyRequest(self, url, method='GET', headers={}, contentText=None, includeAuth=True):
		message = self._request("ProxyRequest", {
			"url": url,
			"method": method,
			"includeAuth": includeAuth,
			"headers": headers,
			"contentText": contentText
		})

		combined_bytes = bytearray()
		for m in message:
			combined_bytes += m

		return ProxyResponse(io.BytesIO(combined_bytes))

def _run_poll_loop(port, hbReqPort, hbRepPort, queue_address,
				   shared_connectedSelf, shared_connectedRemote,
				   shared_exception, should_continue_event, response_queue,
				   shared_running):
	context: zmq.Context | None = None
	socket = None
	hbReqSocket = None
	hbRepSocket = None
	queuePullSocket = None
	lost_heartbeat = False
	lastPongTime = time.time()
	lastPongId = ""
	ping = time.time()

	shared_running.value = True

	#Used to check if parent process is still alive, however this won't work on Windows.
	parent_pid = os.getppid()

	try:
		# Create new ZMQ context in child process
		context = zmq.Context()

		# BIND sockets in child process (this is where the polling happens)
		socket = context.socket(zmq.DEALER)
		socket.setsockopt(zmq.RCVTIMEO, 5000)
		socket.setsockopt(zmq.SNDTIMEO, 5000)
		socket.setsockopt(zmq.LINGER, 1000)
		socket.bind(f'tcp://*:{port}')

		hbReqSocket = context.socket(zmq.REQ)
		hbReqSocket.setsockopt(zmq.RCVTIMEO, 5000)
		hbReqSocket.setsockopt(zmq.SNDTIMEO, 5000)
		hbReqSocket.setsockopt(zmq.REQ_CORRELATE, 1)
		hbReqSocket.setsockopt(zmq.REQ_RELAXED, 1)
		hbReqSocket.bind(f'tcp://*:{hbReqPort}')

		hbRepSocket = context.socket(zmq.REP)
		hbRepSocket.setsockopt(zmq.RCVTIMEO, 5000)
		hbRepSocket.setsockopt(zmq.SNDTIMEO, 5000)
		hbRepSocket.bind(f'tcp://*:{hbRepPort}')

		# Bind the queue PULL socket (IPC)
		queuePullSocket = context.socket(zmq.PULL)
		queuePullSocket.setsockopt(zmq.RCVTIMEO, 5000)
		queuePullSocket.bind(queue_address)

		poller = zmq.Poller()
		poller.register(socket, zmq.POLLIN)
		poller.register(hbReqSocket, zmq.POLLIN)
		poller.register(hbRepSocket, zmq.POLLIN)
		poller.register(queuePullSocket, zmq.POLLIN)

		while not should_continue_event.is_set() and parent_pid == os.getppid():
			# Process incoming messages
			socks = dict(poller.poll(500))

			# Check for pending messages in the queue socket
			if queuePullSocket in socks and socks[queuePullSocket] == zmq.POLLIN:
				msg = queuePullSocket.recv_multipart()
				socket.send_multipart(msg, copy=False)

			if socket in socks and socks[socket] == zmq.POLLIN:
				message = socket.recv_multipart()

				version_part = message[0].decode()
				id_part = message[1].decode()
				header_part = message[2].decode()

				logging.debug(f'message received. {version_part} - {header_part}')

				if header_part == 'GetBinaryDataResponse' or header_part == 'GetFileBytesResponse' or header_part == 'ProxyRequestResponse' or header_part == 'MLFLOWDownloadFileResponse':
					# These types send a list of byte arrays
					# Serialize byte arrays for queue
					response_queue.put((id_part, header_part, message[3:]))
				else:
					# These types send a json message
					response_queue.put((id_part, header_part, message[3].decode()))

			# hbRepSocket receives PING requests from the client, this sends the PONG response to the client
			if hbRepSocket in socks and socks[hbRepSocket] == zmq.POLLIN:
				message = hbRepSocket.recv_multipart()

				version_part = message[0].decode()
				id_part = message[1].decode()
				header_part = message[2].decode()

				logging.debug(f'message ping received. {version_part} - {header_part}')

				if header_part == 'PING':
					shared_connectedRemote.value = True
					hbRepSocket.send_multipart([str.encode("2"), str.encode(id_part), str.encode("PONG"), str.encode("")])
					if lost_heartbeat:
						heartbeat_restored_message = "Heartbeat connection to server restored. Successfully received PING."
						logging.info(heartbeat_restored_message)
						print(heartbeat_restored_message, file=sys.stderr)
						lost_heartbeat = False

			# hbReqSocket sends PING requests to the client, this checks for the PONG response from the client
			if hbReqSocket in socks and socks[hbReqSocket] == zmq.POLLIN:
				message = hbReqSocket.recv_multipart()

				version_part = message[0].decode()
				id_part = message[1].decode()
				header_part = message[2].decode()

				logging.debug(f'message pong received. {version_part} - {header_part}')

				lastPongTime = time.time()

				if lastPongId == id_part:
					shared_connectedSelf.value = True
				else:
					logging.info(f"Received unknown or old PONG. Expected: {lastPongId} Received: {id_part}")
					shared_connectedSelf.value = False

				if lost_heartbeat:
					lost_heartbeat = False
					heartbeat_restored_message = "Heartbeat connection to server restored. Successfully sent PING."
					logging.info(heartbeat_restored_message)
					print(heartbeat_restored_message, file=sys.stderr)

			if time.time() > ping + 10:
				ping = time.time()
				try:
					lastPongId = str(uuid.uuid4())
					hbReqSocket.send_multipart([str.encode("2"), str.encode(lastPongId), str.encode("PING"), str.encode("")])
				except:
					pass

			if time.time() - lastPongTime > 300:
				shared_connectedSelf.value = False
				if not lost_heartbeat:
					lost_heartbeat = True
					lost_heartbeat_message = "Lost heartbeat connection to server"
					logging.error(lost_heartbeat_message)
					print(lost_heartbeat_message, file=sys.stderr)

	except Exception as e:
		shared_exception.value = str(e)
	finally:
		shared_running.value = False
		# Clean up sockets
		try:
			if socket:
				socket.close()
			if hbReqSocket:
				hbReqSocket.close()
			if hbRepSocket:
				hbRepSocket.close()
			if queuePullSocket:
				queuePullSocket.close()
			if context:
				context.term()
		except:
			pass

def _readRecordCountsCache(source, filter=None):
	key = source + _hashFilter(filter)
	return _recordCounts[key]

def _tryReadRecordCountsCache(source, filter=None):
	key = source + _hashFilter(filter)
	if key in _recordCounts:
		return True, _recordCounts[key]
	else:
		return False, None

def _setRecordCountsCache(source, value, filter=None):
	key = source + _hashFilter(filter)
	_recordCounts[key] = value

def _hashFilter(filter):
	if filter is None:
		return ''

	try:
		filterJson = json.dumps(filter, sort_keys=True, separators=(',',':'))
		return hashlib.sha256(filterJson.encode('utf-8')).hexdigest()
	except:
		return ''
