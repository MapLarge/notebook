import os
import time
import json
import logging
from threading import Timer
from maplargeclient import MapLargeClient, __version__

class MODES:
	DESCRIBE = "Describe"
	EXECUTE = "Execute"
	INTERACTIVE = "Interactive"

class APPEND_MODE:
	DEFAULT = "default"
	INSERT_ONLY = "insert_only"
	UPDATE_ONLY = "update_only"

_config = json.load(open(os.environ['ML_CONFIG']))

logLevel = logging.DEBUG

if "logLevel" in _config:
	logLevel = _config["logLevel"]

MODE = _config["mode"]

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logLevel)

logging.info(_config)

Parameters = {}

if "userParameters" in _config:
	try:
		Parameters = json.loads(_config["userParameters"])
	except:
		logging.warning("Unable to load user parameters.")

Secrets = []

if "secrets" in _config:
	try:
		Secrets = json.loads(_config["secrets"])
	except:
		logging.warning("Unable to load secrets into a variable.")

ml = MapLargeClient(_config['port'], _config['hbReqPort'], _config['hbRepPort'])

def Start():
	ml.start()

def Stop():
	ml.stop()

def FinalizeOutput():
	if MODE == MODES.DESCRIBE or MODE == MODES.EXECUTE:
		ml.stop()

def _connectionCheck(timeoutSecs = 300):
	timer = Timer(timeoutSecs, ml.stop)
	timer.start()
	ml.start()
	while not ml.connected and ml.running:
		logging.debug("Waiting for server")
		time.sleep(5)
	if not ml.connected:
		raise Exception("Server never connected")
	timer.cancel()

def SetOutputSchema(copyFromSource=None, columns=None):
	_connectionCheck()
	ml.SetOutputSchema(copyFromSource, columns)
	#if server just needs the schema, stop execution
	if MODE == MODES.DESCRIBE:
		ml.stop()
		os._exit(0)

def GetInputSchema(inputs=None):
	_connectionCheck()
	return ml.GetInputSchema(inputs)

def WriteData(table, data, append=False, append_mode=APPEND_MODE.DEFAULT, primary_key=None):
	_connectionCheck()
	ml.WriteData(table, data, append, append_mode, primary_key)

def DeleteData(table, filter=None):
	_connectionCheck()
	ml.DeleteData(table, filter)

def GetData(source, start, take, columns=None, filter=None):
	_connectionCheck()
	return ml.GetData(source, start, take, columns, filter)

def GetRecordCount(source, filter=None):
	_connectionCheck()
	return ml.GetRecordCount(source, filter)

def DataIterator(source, page_size=100000, columns=None, filter=None):
	_connectionCheck()
	for value in ml.DataIterator(source, page_size, columns, filter):
		yield value

def SetOutput(data, additive=False, stop=True, strictSchema=False):
	_connectionCheck()
	ml.SetOutput(data, additive, strictSchema)
	if stop and MODE == MODES.EXECUTE:
		ml.stop()

def ClearOutput():
	_connectionCheck()
	ml.ClearOutput()

def ListFiles(path, recursive=False):
	_connectionCheck()
	return ml.ListFiles(path, recursive)

def SetBinaryData(column, rowIndex, data, format=None, copyGeoFrom=None, mediaType=None):
	_connectionCheck()
	return ml.SetBinaryData(column, rowIndex, data, format, copyGeoFrom, mediaType)

def GetFileBytes(path):
	_connectionCheck()
	return ml.GetFileBytes(path)

def GetBinaryData(source, column, rowIndex = -1, format = None, maxDimension = None, mediaKeyColumns = None, mediaKeyValues = None, filter = None):
	_connectionCheck()
	return ml.GetBinaryData(source, column, rowIndex, format, maxDimension = maxDimension, mediaKeyColumns = mediaKeyColumns, mediaKeyValues = mediaKeyValues, filter = filter)

def SaveFile(path, savePath):
	_connectionCheck()
	return ml.SaveFile(path, savePath)

def GetWorkloadInfo(workloadName=None, workloadId=None):
	_connectionCheck()
	return ml.GetWorkloadInfo(workloadName, workloadId)

def GetUrl(timeoutSeconds=None):
	_connectionCheck()
	return ml.GetUrl(timeoutSeconds)

def GetAuth(originalToken=None, durationSeconds=None):
	_connectionCheck()
	return ml.GetAuth(originalToken, durationSeconds)

def ProxyRequest(url, method='GET', headers={}, contentText=None, includeAuth=True):
	_connectionCheck()
	return ml.ProxyRequest(url, method, headers, contentText, includeAuth)

def _getSecret(name=None, path=None):
	field = None
	searchValue = None
	if name:
		field = 'name'
		searchValue = name
	elif path:
		if path.startswith("/"):
			path = path[1:]
		if path.startswith("maplarge/secrets/"):
			path = path[17:]
		field = 'path'
		searchValue = path
	
	if not field:
		return None
	
	for secret in Secrets:
		if secret.get(field) == searchValue:
			return secret
		
	return None

def GetSecret(name=None, path=None):
	secret = _getSecret(name, path)
	if secret:
		return os.environ.get(secret["path"], None)
	return None

def GetSecretText(name=None, path=None, encoding="utf-8"):
	secret = _getSecret(name, path)
	if not secret:
		return None
	
	env_var = os.environ.get(secret["path"], None)
	if env_var and secret["mountMethod"] == "file":
		with open(env_var, "r", encoding=encoding) as f:
			return f.read()
	
	return env_var

def GetSecretBytes(name=None, path=None, encoding="utf-8"):
	secret = _getSecret(name, path)
	if not secret:
		return None
	
	env_var = os.environ.get(secret["path"], None)
	if env_var and secret["mountMethod"] == "file":
		with open(env_var, "rb") as f:
			return f.read()
	
	return env_var.encode(encoding) if env_var else None
