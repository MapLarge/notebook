import io
import time
import json
import zmq
import logging
import uuid
from threading import Timer, Thread, main_thread
import pandas as pd
import queue

_recordCounts = dict()
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

    def text(self, encoding="utf-8"):
        return self.response.getvalue().decode(encoding)

    def json(self):
        from json import loads

        return loads(self.response.getvalue())

    def bytes(self):
        return self.response.getvalue()

    def save(self, file_path: str):
        with open(file_path, "wb") as f:
            f.write(self.response.getvalue())


class MapLargeClient(object):
    def __init__(self, port, hbReqPort, hbRepPort):
        self.connectedSelf = False
        self.connectedRemote = False
        self.running = False
        self.onDataReceived = Event()
        self._context = zmq.Context()
        self._exception = None

        # Bind to the port specified

        logging.info("Establishing socket listener")
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.bind("tcp://*:" + port)
        logging.info(f"Bound socket to port {port}")
        self._socket.setsockopt(zmq.RCVTIMEO, 5000)
        self._socket.setsockopt(zmq.SNDTIMEO, 5000)
        self._socket.setsockopt(zmq.LINGER, 0)

        self._hbReqSocket = self._context.socket(zmq.REQ)
        self._hbReqSocket.bind("tcp://*:" + hbReqPort)
        self._hbReqSocket.setsockopt(zmq.RCVTIMEO, 5000)
        self._hbReqSocket.setsockopt(zmq.SNDTIMEO, 5000)
        self._hbReqSocket.setsockopt(zmq.REQ_CORRELATE, 1)
        self._hbReqSocket.setsockopt(zmq.REQ_RELAXED, 1)

        self._hbRepSocket = self._context.socket(zmq.REP)
        self._hbRepSocket.bind("tcp://*:" + hbRepPort)
        self._hbRepSocket.setsockopt(zmq.RCVTIMEO, 5000)
        self._hbRepSocket.setsockopt(zmq.SNDTIMEO, 5000)

        self._ping = time.time()
        self._lastPongTime = time.time()
        self._lastPongId = ""

        self._requests = dict()
        self._requestQueue = queue.Queue()
        self.onDataReceived += self._handleReceipt

    @property
    def connected(self):
        return self.connectedSelf or self.connectedRemote

    def _request(self, action, message, dataBytes=None, attempt=0):
        if attempt > _REQUEST_TRIES:
            raise Exception("Request failed")

        logging.debug(f"Sending request {action}")
        jsonMessage = json.dumps(message)

        id = str(uuid.uuid4())

        messageParts = [
            str.encode("2"),
            str.encode(id),
            str.encode(action),
            str.encode(jsonMessage),
        ]

        if dataBytes is not None:
            chunkSize = 2**20
            while True:
                chunk = dataBytes.read(chunkSize)
                if not chunk:
                    break
                messageParts.append(chunk)

        self._requests[id] = {"complete": False, "result": None, "error": None}
        self._requestQueue.put(messageParts)

        timeout = Timer(90, self._clearRequest, args=[id])
        timeout.start()

        while id in self._requests and self._requests[id]["complete"] == False:
            time.sleep(0.25)

        timeout.cancel()

        if self._exception:
            raise self._exception

        if not id in self._requests or self._requests[id]["complete"] == False:
            raise Exception(f"No response from server")

        if self._requests[id]["error"] is not None:
            raise Exception("Response from server: " + self._requests[id]["error"])

        logging.debug(f"Request {action} successful")
        return self._requests[id]["result"]

    def _clearRequest(self, id):
        del self._requests[id]

    def stop(self):
        logging.debug("Stopping poll thread")
        if not self.running:
            return
        self._should_continue = False
        self.running = False
        self._runThread.join()
        logging.debug("Poll thread stopped")

        if self._exception:
            raise self._exception

    def start(self):
        if self.running:
            return
        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
        self._poller.register(self._hbReqSocket, zmq.POLLIN)
        self._poller.register(self._hbRepSocket, zmq.POLLIN)
        self._should_continue = True
        self.connectedSelf = False
        self.connectedRemote = False
        self.running = True
        self._exception = None
        self._runThread = Thread(target=self.__run)
        self._runThread.start()

    def __run(self):
        try:
            while self._should_continue and main_thread().is_alive():
                # send pending messages
                while not self._requestQueue.empty():
                    msg = self._requestQueue.get_nowait()

                    self._socket.send_multipart(msg, copy=False)

                # process incoming messages
                socks = dict(
                    self._poller.poll(500),
                )  # TODO: Eventually it would be nice to move the pending queue into a zmq socket so this poll isn't blocking sends
                if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                    message = self._socket.recv_multipart()

                    version_part = message[0].decode()
                    id_part = message[1].decode()
                    header_part = message[2].decode()

                    logging.debug(f"message received. {version_part} - {header_part}")

                    if (
                        header_part == "GetBinaryDataResponse"
                        or header_part == "GetFileBytesResponse"
                        or header_part == "ProxyRequestResponse"
                        or header_part == "MLFLOWDownloadFileResponse"
                    ):
                        # These types send a list of byte arrays
                        self.onDataReceived(id_part, header_part, message[3:])
                    else:
                        # these types send a json message
                        self.onDataReceived(id_part, header_part, message[3].decode())

                if (
                    self._hbRepSocket in socks
                    and socks[self._hbRepSocket] == zmq.POLLIN
                ):
                    message = self._hbRepSocket.recv_multipart()

                    version_part = message[0].decode()
                    id_part = message[1].decode()
                    header_part = message[2].decode()

                    logging.debug(
                        f"message ping received. {version_part} - {header_part}",
                    )

                    if header_part == "PING":
                        self.connectedRemote = True
                        self._hbRepSocket.send_multipart(
                            [
                                str.encode("2"),
                                str.encode(id_part),
                                str.encode("PONG"),
                                str.encode(""),
                            ],
                        )

                if (
                    self._hbReqSocket in socks
                    and socks[self._hbReqSocket] == zmq.POLLIN
                ):
                    message = self._hbReqSocket.recv_multipart()

                    version_part = message[0].decode()
                    id_part = message[1].decode()
                    header_part = message[2].decode()

                    logging.debug(
                        f"message pong received. {version_part} - {header_part}",
                    )

                    self._lastPongTime = time.time()
                    if self._lastPongId == id_part:
                        self.connectedSelf = True
                    else:
                        logging.info(
                            f"Received unknown or old PONG. Expected: {self._lastPongId} Received: {id_part}",
                        )
                        self.connectedSelf = False

                if time.time() > self._ping + 5:
                    self._ping = time.time()
                    self.__pingPong()
                if time.time() - self._lastPongTime > 300:
                    self.connectedSelf = False
                    logging.error("Lost connection to server")
                    raise Exception("Lost connection to server")
        except Exception as e:
            self._exception = e
            self._requests.clear()
        finally:
            self.running = False

    def __pingPong(self):
        # perform a new ping
        logging.debug("Sending PING request")
        try:
            self._lastPongId = str(uuid.uuid4())
            self._hbReqSocket.send_multipart(
                [
                    str.encode("2"),
                    str.encode(self._lastPongId),
                    str.encode("PING"),
                    str.encode(""),
                ],
            )
        except:
            pass

    def _ping(self):
        pong = self._request()

    def _handleReceipt(self, id, messageType, message):
        if messageType == "ServerError":
            self._requests[id]["error"] = message
        else:
            self._requests[id]["result"] = message

        self._requests[id]["complete"] = True

    def SetOutputSchema(self, copyFromSource=None, columns=None):
        request = dict()

        if copyFromSource is None and columns is None:
            raise Exception("copyFromSource or columns is required")

        if copyFromSource is not None:
            request["copyFromSource"] = copyFromSource
        if columns is not None:
            request["newColumns"] = [c["name"] for c in columns]
            request["newColumnTypes"] = [c["type"] for c in columns]

        self._request("SetOutputSchema", request)

    def GetInputSchema(self, inputs=None):
        message = self._request("GetInputSchema", {"sources": inputs})

        resp = json.loads(message)

        output = dict()

        for table in resp["sources"]:
            columns = [
                ColumnInfo(name, type)
                for name, type in zip(
                    resp["sources"][table]["columnNames"],
                    resp["sources"][table]["columnTypes"],
                )
            ]

            output[table] = TableInfo(table, columns)

        return output

    def GetData(self, source, start, take, columns=None):
        logging.debug(
            "GetData called on {source} with start {start} and take {take}".format(
                source=source,
                start=start,
                take=take,
            ),
        )
        request = {"source": source, "start": start, "take": take}

        if columns is not None:
            request["columns"] = columns

        message = self._request("GetData", request)

        logging.debug("Data Received")

        resp = json.loads(message)
        df = pd.DataFrame(resp["data"])

        logging.debug("Data Loaded")

        # save record count for later
        _recordCounts[source] = resp["totals"]["Records"]

        return df

    def WriteData(self, table, data, append=False, append_mode=None, primary_key=None):
        if append and primary_key is None and (append_mode is not None or "default"):
            raise Exception(
                "Primary key is required in order to use append_mode=" + append_mode,
            )

        request = {
            "table": table,
            "data": {column: list(data[column]) for column in data},
            "append": append or False,
            "appendMode": append_mode if append else None,
            "primaryKey": primary_key,
        }

        self._request("WriteData", request)

    def GetRecordCount(self, source):
        if source not in _recordCounts:
            self.GetData(source, 0, 1)

        return _recordCounts[source]

    def DataIterator(self, source, page_size=100000, columns=None):
        logging.debug(
            "DataIterator called on {source} with page_size {page_size}".format(
                source=source,
                page_size=page_size,
            ),
        )
        start = 0
        total_records = self.GetRecordCount(source)

        while start < total_records:
            df = self.GetData(source, start, page_size, columns=columns)

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
            "data": {column: list(data[column]) for column in data},
            "additive": additive,
            "strictSchema": strictSchema,
        }

        self._request("SetOutput", request)

    def ClearOutput(self):
        self._request("ClearOutput", {})

    def SetBinaryData(
        self,
        column,
        rowIndex,
        data,
        format=None,
        copyGeoFrom=None,
        mediaType=None,
    ):
        request = {
            "column": column,
            "rowIndex": rowIndex,
            "format": format,
            "copyGeoFrom": copyGeoFrom,
            "mediaType": mediaType,
        }

        self._request("SetBinaryData", request, data)

    def ListFiles(self, path, recursive=False):
        request = {"path": path, "recursive": recursive}

        message = self._request("ListFiles", request)

        resp = json.loads(message)
        return pd.DataFrame(resp["files"])

    def GetFileBytes(self, path):
        request = {"path": path}

        message = self._request("GetFileBytes", request)

        combined_bytes = bytearray()
        for m in message:
            combined_bytes += m

        handle = io.BytesIO(combined_bytes)

        return handle

    def GetBinaryData(
        self,
        source,
        column,
        rowIndex=-1,
        format=None,
        maxDimension=None,
        mediaKeyColumns=None,
        mediaKeyValues=None,
    ):
        request = {
            "source": source,
            "column": column,
            "rowIndex": rowIndex,
            "mediaKeyColumns": None
            if mediaKeyColumns is None
            else (
                mediaKeyColumns
                if isinstance(mediaKeyColumns, list)
                else [mediaKeyColumns]
            ),
            "mediaKeyValues": None
            if mediaKeyValues is None
            else (
                mediaKeyValues if isinstance(mediaKeyValues, list) else [mediaKeyValues]
            ),
        }

        if format is not None:
            request["format"] = format

        if maxDimension is not None:
            request["maxDimension"] = maxDimension

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

        request = {"workloadId": workloadId, "workloadName": workloadName}

        message = self._request("GetWorkloadInfo", request)
        resp = json.loads(message)
        error = resp["Error"]
        if error is not None:
            raise Exception(error)

        return resp["Workload"]

    def GetUrl(self, timeoutSeconds=None):
        return self._request("GetUrl", {"timeoutSeconds": timeoutSeconds})

    def GetAuth(self, originalToken=None, durationSeconds=None):
        message = self._request(
            "GetAuth",
            {"originalToken": originalToken, "durationSeconds": durationSeconds},
        )

        return json.loads(message)

    def ProxyRequest(
        self,
        url,
        method="GET",
        headers={},
        contentText=None,
        includeAuth=True,
    ):
        message = self._request(
            "ProxyRequest",
            {
                "url": url,
                "method": method,
                "includeAuth": includeAuth,
                "headers": headers,
                "contentText": contentText,
            },
        )

        combined_bytes = bytearray()
        for m in message:
            combined_bytes += m

        return ProxyResponse(io.BytesIO(combined_bytes))
