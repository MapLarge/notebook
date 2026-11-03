import json
import os
import io
from mlflow.exceptions import MlflowException
from maplargeclient import MapLargeClient
from mlflow.entities import ViewType, RunStatus, LifecycleStage, Metric, Param, Dataset, DatasetInput, InputTag, ExperimentTag, FileInfo
from mlflow.store.tracking import SEARCH_MAX_RESULTS_DEFAULT
from mlflow.store.entities import PagedList
from mlflow.entities import Experiment, Run, RunInfo, RunData, RunInputs
from mlflow.entities.model_registry import RegisteredModel, ModelVersion, RegisteredModelAlias
from mlflow.utils.search_utils import SearchModelUtils, SearchModelVersionUtils, SearchUtils, SearchExperimentsUtils

class Tag:
	def __init__(self, key, value):
		self.key = key
		self.value = value

	@staticmethod
	def to_json_object(obj):
		return None if obj is None else { "key": obj.key, "value": obj.value }

	@staticmethod
	def from_json_object(obj):
		return None if obj is None else Tag(obj["Key"], obj["Value"])

	def to_input_tag(self):
		return InputTag(self.key, self.value)

	def to_experiment_tag(self):
		return ExperimentTag(self.key, self.value)

	def to_param(self):
		return Param(self.key, self.value)

class MapLargeMLFlowClient(MapLargeClient):
	_default_instance = None

	def __init__(self, port, hbReqPort, hbRepPort):
		super().__init__(port, hbReqPort, hbRepPort)

	@classmethod
	def get_or_initialize_default_connection(cls):
		if cls._default_instance is None:
			config = json.load(open(os.environ['ML_CONFIG']))
			enable_mlflow = os.environ.get('ML_ENABLE_MLFLOW', '1') != '0'
			if not enable_mlflow:
				return None

			cls._default_instance = cls(config['mlflowApiPort'], config['mlflowApiHbReqPort'], config['mlflowApiHbRepPort'])
			cls._default_instance.start()
		return cls._default_instance

	def _request(self, action, message, dataBytes=None, attempt=0):
		self.start()
		return super()._request("MLFLOW" + action, message, dataBytes, attempt)

	def search_experiments(self, view_type=ViewType.ACTIVE_ONLY, max_results=SEARCH_MAX_RESULTS_DEFAULT, filter_string=None, order_by=None, page_token=None, fetch_tags=True, store_uri=None, artifact_uri=None):
		parsed_filters = SearchExperimentsUtils.parse_search_filter(filter_string)
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"viewType": ViewType.to_string(view_type),
			"maxResults": max_results,
			"filterString": filter_string,
			"filterObject": parsed_filters,
			"orderBy": order_by,
			"pageToken": page_token,
			"fetchTags": fetch_tags
		}

		message = self._request("SearchExperiments", request)
		resp = json.loads(message)

		self._throw_if_errors(resp["Errors"])

		experiments = [self._experiment_from_json(exp) for exp in resp["Data"]["List"] or [] if exp is not None]
		token = resp["Data"]["Token"]

		return PagedList(experiments, token)

	def get_experiment_by_name(self, experiment_name, fetch_tags=True, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"name": experiment_name,
			"fetchTags": fetch_tags
		}

		message = self._request("GetExperimentByName", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._experiment_from_json(resp["Data"])

	def create_experiment(self, name, artifact_location, tags, id=None, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"name": name,
			"artifactLocation": artifact_location,
			"tags": [Tag.to_json_object(t) for t in tags or [] if t is not None],
			"id": id
		}

		message = self._request("CreateExperiment", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return resp["Data"]

	def get_experiment(self, experiment_id, fetch_tags=True, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": experiment_id,
			"fetchTags": fetch_tags
		}

		message = self._request("GetExperiment", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._experiment_from_json(resp["Data"])

	def delete_experiment(self, experiment_id, permanent=False, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": experiment_id,
			"permanent": permanent
		}

		message = self._request("DeleteExperiment", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def restore_experiment(self, experiment_id, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": experiment_id
		}

		message = self._request("RestoreExperiment", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def rename_experiment(self, experiment_id, new_name, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": experiment_id,
			"name": new_name
		}

		message = self._request("RenameExperiment", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def get_run(self, run_id, only_fetch_run_info=False, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": run_id,
			"onlyFetchRunInfo": only_fetch_run_info
		}

		message = self._request("GetRun", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._run_from_json(resp["Data"])

	def update_run_info(self, run_id, run_status, end_time, run_name, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": run_id,
			"status": RunStatus.to_string(run_status),
			"endTime": end_time,
			"name": run_name
		}

		message = self._request("UpdateRunInfo", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._run_info_from_json(resp["Data"])

	def create_run(self, experiment_id, user_id, start_time, tags, run_name, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"experimentId": experiment_id,
			"startTime": start_time,
			"userId": user_id,
			"tags": [Tag.to_json_object(tag) for tag in tags or [] if tag is not None],
			"name": run_name
		}

		message = self._request("CreateRun", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._run_from_json(resp["Data"])

	def delete_run(self, run_id, permanent=False, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": run_id,
			"permanent": permanent
		}

		message = self._request("DeleteRun", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def restore_run(self, run_id, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"id": run_id
		}

		message = self._request("RestoreRun", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def set_experiment_tag(self, experiment_id, tag, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"experimentId": experiment_id,
			"tag": Tag.to_json_object(tag)
		}

		message = self._request("SetExperimentTag", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		tag = self._tag_from_json(resp["Data"])
		return tag.to_experiment_tag() if tag is not None else None

	def get_metric_history(self, run_id, metric_key, max_results=None, page_token=None, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"runId": run_id,
			"metricKey": metric_key,
			"maxResults": max_results,
			"pageToken": page_token
		}

		message = self._request("GetMetricHistory", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		token = resp["Data"]["Token"]
		metrics = [self._metric_from_json(metric) for metric in resp["Data"]["List"] or [] if metric is not None]
		return PagedList(metrics, token)

	def search_runs(self, experiment_ids, filter_string, run_view_type, max_results, order_by, page_token, store_uri=None, artifact_uri=None):
		parsed_filters = SearchUtils.parse_search_filter(filter_string)
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"experimentIds": experiment_ids,
			"filterString": filter_string,
			"filterObject": parsed_filters,
			"runViewType": ViewType.to_string(run_view_type or ViewType.ACTIVE_ONLY),
			"maxResults": max_results,
			"orderBy": order_by,
			"pageToken": page_token
		}

		message = self._request("SearchRuns", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		token = resp["Data"]["Token"]
		runs = [self._run_from_json(run) for run in resp["Data"]["List"] or [] if run is not None]
		return (runs, token)

	def log_batch(self, run_id, metrics, params, tags, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"runId": run_id,
			"metrics": [self._metric_to_json_object(m) for m in metrics or [] if m is not None],
			"params": [Tag.to_json_object(p) for p in params or [] if p is not None],
			"tags": [Tag.to_json_object(t) for t in tags or [] if t is not None]
		}

		message = self._request("LogBatch", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def record_logged_model(self, run_id, mlflow_model, store_uri=None, artifact_uri=None):
		#This is a NOOP because the real models are saved in the artifact repository.
		pass

	def log_inputs(self, run_id, datasets = None, store_uri=None, artifact_uri=None):
		request = {
			"storeUri": store_uri,
			"artifactUri": artifact_uri,
			"runId": run_id,
			"datasets": [self._dataset_input_to_json_object(d) for d in datasets or [] if d is not None]
		}

		message = self._request("LogInputs", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def log_artifact(self, dataBytes, artifact_path=None, artifact_uri=None):
		request = {
			"artifactUri": artifact_uri,
			"artifactPath": artifact_path
		}

		message = self._request("LogArtifact", request, dataBytes)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def list_artifacts(self, path, artifact_uri=None):
		request = {
			"artifactUri": artifact_uri,
			"path": path
		}

		message = self._request("ListArtifacts", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

		return [self._list_artifact_result_from_json(artifact) for artifact in resp["Data"] or [] if artifact is not None]

	def download_file(self, remote_file_path, artifact_uri=None):
		request = {
			"artifactUri": artifact_uri,
			"remoteFilePath": remote_file_path
		}

		message = self._request("DownloadFile", request)
		combined_bytes = bytearray()
		for m in message:
			combined_bytes += m

		return io.BytesIO(combined_bytes)

	def delete_artifacts(self, artifact_path=None, artifact_uri=None):
		request = {
			"artifactUri": artifact_uri,
			"artifactPath": artifact_path
		}

		message = self._request("DeleteArtifacts", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def create_registered_model(self, name, tags=None, description=None, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"tags": [Tag.to_json_object(t) for t in tags or [] if t is not None],
			"description": description,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("CreateRegisteredModel", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._registered_model_from_json(resp["Data"])

	def update_registered_model(self, name, description, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"description": description,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("UpdateRegisteredModel", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._registered_model_from_json(resp["Data"])

	def rename_registered_model(self, name, new_name, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"newName": new_name,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("RenameRegisteredModel", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._registered_model_from_json(resp["Data"])

	def delete_registered_model(self, name, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("DeleteRegisteredModel", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def search_registered_models(self, filter_string=None, max_results=None, order_by=None, page_token=None, store_uri=None, tracking_uri=None):
		parsed_filters = SearchModelUtils.parse_search_filter(filter_string)

		request = {
			"filterString": filter_string,
			"filterObject": parsed_filters,
			"maxResults": max_results,
			"orderBy": order_by,
			"pageToken": page_token,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("SearchRegisteredModels", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		token = resp["Data"]["Token"]
		models = [self._registered_model_from_json(model) for model in resp["Data"]["List"] or [] if model is not None]
		return PagedList(models, token)

	def get_registered_model(self, name, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("GetRegisteredModel", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._registered_model_from_json(resp["Data"])

	def get_latest_versions(self, name, stages=None, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"stages": stages,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("GetLatestVersions", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return [self._model_version_from_json(model_version) for model_version in resp["Data"] or [] if model_version is not None]

	def set_registered_model_tag(self, name, tag, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"tag": Tag.to_json_object(tag),
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("SetRegisteredModelTag", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def delete_registered_model_tag(self, name, key, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"key": key,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("DeleteRegisteredModelTag", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def create_model_version(self, name, source, run_id=None, tags=None, run_link=None, description=None, local_model_path=None, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"source": source,
			"runId": run_id,
			"tags": [Tag.to_json_object(t) for t in tags or [] if t is not None],
			"runLink": run_link,
			"description": description,
			"localModelPath": local_model_path,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("CreateModelVersion", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._model_version_from_json(resp["Data"])

	def update_model_version(self, name, version, description, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"description": description,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("UpdateModelVersion", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._model_version_from_json(resp["Data"])

	def transition_model_version_stage(self, name, version, stage, archive_existing_versions, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"stage": stage,
			"archiveExistingVersions": archive_existing_versions,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("TransitionModelVersionStage", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._model_version_from_json(resp["Data"])

	def delete_model_version(self, name, version, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("DeleteModelVersion", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def get_model_version(self, name, version, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("GetModelVersion", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._model_version_from_json(resp["Data"])

	def get_model_version_download_uri(self, name, version, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("GetModelVersionDownloadUri", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return resp["Data"]

	def search_model_versions(self, filter_string=None, max_results=None, order_by=None, page_token=None, store_uri=None, tracking_uri=None):
		parsed_filters = SearchModelVersionUtils.parse_search_filter(filter_string)

		request = {
			"filterString": filter_string,
			"filterObject": parsed_filters,
			"maxResults": max_results,
			"orderBy": order_by,
			"pageToken": page_token,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("SearchModelVersions", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		token = resp["Data"]["Token"]
		models = [self._model_version_from_json(model_version) for model_version in resp["Data"]["List"] or [] if model_version is not None]
		return PagedList(models, token)

	def set_model_version_tag(self, name, version, tag, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"tag": Tag.to_json_object(tag),
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("SetModelVersionTag", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def delete_model_version_tag(self, name, version, key, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"key": key,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("DeleteModelVersionTag", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def set_registered_model_alias(self, name, alias, version, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"version": version,
			"alias": alias,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("SetRegisteredModelAlias", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def delete_registered_model_alias(self, name, alias, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"alias": alias,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("DeleteRegisteredModelAlias", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])

	def get_model_version_by_alias(self, name, alias, store_uri=None, tracking_uri=None):
		request = {
			"name": name,
			"alias": alias,
			"storeUri": store_uri,
			"trackingUri": tracking_uri
		}

		message = self._request("GetModelVersionByAlias", request)
		resp = json.loads(message)
		self._throw_if_errors(resp["Errors"])
		return self._model_version_from_json(resp["Data"])

	#JSON Converters

	def _experiment_from_json(self, exp):
		return None if exp is None else Experiment(
			exp["ExperimentId"],
			exp["Name"],
			exp["ArtifactLocation"],
			exp["LifecycleStage"],
			[self._tag_from_json(tag) for tag in exp["Tags"] or [] if tag is not None],
			exp["CreationTime"],
			exp["LastUpdateTime"]
		)

	def _tag_from_json(self, tag):
		return None if tag is None else Tag(tag["Key"], tag["Value"])

	def _run_from_json(self, run):
		return None if run is None else Run(
			self._run_info_from_json(run["Info"]),
			self._run_data_from_json(run["Data"]),
			self._run_inputs_from_json(run["Inputs"])
		)

	def _run_info_from_json(self, run_info):
		return None if run_info is None else RunInfo(
			run_info["RunUUID"],
			run_info["ExperimentId"],
			run_info["UserId"],
			RunStatus.from_string(run_info["Status"]),
			run_info["StartTime"],
			run_info["EndTime"],
			self._lifecycle_stage_from_string(run_info["LifecycleStage"]),
			run_info["ArtifactUri"],
			run_info["RunId"],
			run_info["RunName"]
		)

	def _run_data_from_json(self, run_data):
		return None if run_data is None else RunData(
			[self._metric_from_json(metric) for metric in run_data["Metrics"] or [] if metric is not None],
			[self._tag_from_json(param).to_param() for param in run_data["Params"] or [] if param is not None],
			[self._tag_from_json(tag) for tag in run_data["Tags"] or [] if tag is not None]
		)

	def _run_inputs_from_json(self, run_inputs):
		return None if run_inputs is None else RunInputs(
			[self._dataset_input_from_json(dataset_input) for dataset_input in run_inputs["DatasetInputs"] or [] if dataset_input is not None]
		)

	def _metric_from_json(self, metric):
		return None if metric is None else Metric(
			metric["Key"],
			metric["Value"],
			metric["Timestamp"],
			metric["Step"]
		)

	def _metric_to_json_object(self, metric):
		return None if metric is None else {
			"key": metric.key,
			"value": metric.value,
			"timestamp": metric.timestamp,
			"step": metric.step
		}

	def _dataset_input_from_json(self, dataset_input):
		return None if dataset_input is None else DatasetInput(
			self._dataset_from_json(dataset_input["Dataset"]),
			[self._tag_from_json(tag).to_input_tag() for tag in dataset_input["Tags"] or [] if tag is not None]
		)

	def _dataset_input_to_json_object(self, dataset_input):
		return None if dataset_input is None else {
			"dataset": self._dataset_to_json_object(dataset_input.dataset),
			"tags": [Tag.to_json_object(t) for t in dataset_input.tags or [] if t is not None]
		}

	def _dataset_from_json(self, dataset):
		return None if dataset is None else Dataset(
			dataset["Name"],
			dataset["Digest"],
			dataset["SourceType"],
			dataset["Source"],
			dataset["Schema"],
			dataset["Profile"]
		)

	def _dataset_to_json_object(self, dataset: Dataset):
		return None if dataset is None else {
			"name": dataset.name,
			"digest": dataset.digest,
			"sourceType": dataset.source_type,
			"source": dataset.source,
			"schema": dataset.schema,
			"profile": dataset.profile
		}

	def _list_artifact_result_from_json(self, artifact):
		return None if artifact is None else FileInfo(
			artifact["Path"],
			artifact["IsDirectory"],
			artifact["FileSize"]
		)

	def _registered_model_from_json(self, model):
		return None if model is None else RegisteredModel(
			model["Name"],
			model["CreationTimestamp"],
			model["LastUpdatedTimestamp"],
			model["Description"],
			[self._model_version_from_json(model_version) for model_version in model["LatestVersions"] or [] if model_version is not None],
			[Tag.from_json_object(tag) for tag in model["Tags"] or [] if tag is not None],
			[RegisteredModelAlias(alias["Alias"], str(alias["Version"])) for alias in model["Aliases"] or [] if alias is not None]
		)

	def _model_version_from_json(self, model_version):
		return None if model_version is None else ModelVersion(
			model_version["Name"],
			model_version["Version"],
			model_version["CreationTime"],
			model_version["LastUpdatedTime"],
			model_version["Description"],
			model_version["UserId"],
			model_version["CurrentStage"],
			model_version["Source"],
			model_version["RunId"],
			model_version["Status"],
			model_version["StatusMessage"],
			[Tag.from_json_object(tag) for tag in model_version["Tags"] or [] if tag is not None],
			model_version["RunLink"],
			model_version["Aliases"]
		)

	def _lifecycle_stage_from_string(self, lifecycle_stage):
		if lifecycle_stage is not None and lifecycle_stage.lower() == LifecycleStage.DELETED:
			return LifecycleStage.DELETED
		else:
			return LifecycleStage.ACTIVE

	def _throw_if_errors(self, errors):
		if errors is not None and len(errors) > 0:
			raise MlflowException(errors[0]["Message"], errors[0]["Code"])
