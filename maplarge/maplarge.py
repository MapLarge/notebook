import os
import time
import json
import logging
from threading import Timer
from maplargeclient import MapLargeClient


class MODES:
    DESCRIBE = "Describe"
    EXECUTE = "Execute"
    INTERACTIVE = "Interactive"


class APPEND_MODE:
    DEFAULT = "default"
    INSERT_ONLY = "insert_only"
    UPDATE_ONLY = "update_only"


_config = json.load(open(os.environ["ML_CONFIG"]))

logLevel = logging.DEBUG

if "logLevel" in _config:
    logLevel = _config["logLevel"]

MODE = _config["mode"]

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logLevel,
)

logging.info(_config)

Parameters = {}

if "userParameters" in _config:
    try:
        Parameters = json.loads(_config["userParameters"])
    except:
        logging.warning("Unable to load user parameters.")

ml = MapLargeClient(_config["port"], _config["hbReqPort"], _config["hbRepPort"])


def Start():
    ml.start()


def Stop():
    ml.stop()


def FinalizeOutput():
    if MODE == MODES.DESCRIBE or MODE == MODES.EXECUTE:
        ml.stop()


def _connectionCheck(timeoutSecs=300):
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
    # if server just needs the schema, stop execution
    if MODE == MODES.DESCRIBE:
        ml.stop()
        os._exit(0)


def GetInputSchema(inputs=None):
    _connectionCheck()
    return ml.GetInputSchema(inputs)


def WriteData(
    table,
    data,
    append=False,
    append_mode=APPEND_MODE.DEFAULT,
    primary_key=None,
):
    _connectionCheck()
    ml.WriteData(table, data, append, append_mode, primary_key)


def GetData(source, start, take, columns=None):
    _connectionCheck()
    return ml.GetData(source, start, take, columns)


def GetRecordCount(source):
    _connectionCheck()
    return ml.GetRecordCount(source)


def DataIterator(source, page_size=100000, columns=None):
    _connectionCheck()
    for value in ml.DataIterator(source, page_size, columns):
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


def SetBinaryData(
    column,
    rowIndex,
    data,
    format=None,
    copyGeoFrom=None,
    mediaType=None,
):
    _connectionCheck()
    return ml.SetBinaryData(column, rowIndex, data, format, copyGeoFrom, mediaType)


def GetFileBytes(path):
    _connectionCheck()
    return ml.GetFileBytes(path)


def GetBinaryData(
    source,
    column,
    rowIndex=-1,
    format=None,
    maxDimension=None,
    mediaKeyColumns=None,
    mediaKeyValues=None,
):
    _connectionCheck()
    return ml.GetBinaryData(
        source,
        column,
        rowIndex,
        format,
        maxDimension=maxDimension,
        mediaKeyColumns=mediaKeyColumns,
        mediaKeyValues=mediaKeyValues,
    )


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


def ProxyRequest(url, method="GET", headers={}, contentText=None, includeAuth=True):
    _connectionCheck()
    return ml.ProxyRequest(url, method, headers, contentText, includeAuth)
