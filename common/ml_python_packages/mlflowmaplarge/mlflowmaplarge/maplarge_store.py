from mlflow.protos import databricks_pb2
from mlflow.exceptions import MlflowException
from mlflow.store.tracking.abstract_store import AbstractStore
from mlflow.entities import ViewType, RunTag
from mlflow.store.tracking import SEARCH_MAX_RESULTS_DEFAULT
from mlflow.utils import mlflow_tags
from mlflow.utils.mlflow_tags import _get_run_name_from_tags
from mlflow.utils.name_utils import _generate_random_name
from mlflow.utils.uri import resolve_uri_if_local
from mlflow.utils.validation import (
	_validate_experiment_name, _validate_experiment_tag, _validate_batch_log_data,
	_validate_batch_log_limits, _validate_param_keys_unique, _validate_tag,
	_validate_dataset_inputs
)

from .maplarge_mlflow_client import MapLargeMLFlowClient

"""
Tracking store that connects to MapLarge's database.
"""
class MapLargeStore(AbstractStore):
	def __init__(self, store_uri=None, artifact_uri=None):
		super().__init__()
		self.maplarge = MapLargeMLFlowClient.get_or_initialize_default_connection()
		self.store_uri = store_uri
		self.artifact_uri = artifact_uri

	def search_experiments(self, view_type=ViewType.ACTIVE_ONLY, max_results=SEARCH_MAX_RESULTS_DEFAULT, filter_string=None, order_by=None, page_token=None):
		return self.maplarge.search_experiments(view_type, max_results, filter_string, order_by, page_token, True, self.store_uri, self.artifact_uri)

	def create_experiment(self, name, artifact_location, tags):
		_validate_experiment_name(name)
		if artifact_location:
			artifact_location = resolve_uri_if_local(artifact_location)

		return self.maplarge.create_experiment(name, artifact_location, tags, None, self.store_uri, self.artifact_uri)

	def get_experiment(self, experiment_id):
		return self.maplarge.get_experiment(experiment_id, True, self.store_uri, self.artifact_uri)

	def get_experiment_by_name(self, experiment_name):
		return self.maplarge.get_experiment_by_name(experiment_name, True, self.store_uri, self.artifact_uri)

	def delete_experiment(self, experiment_id):
		self.maplarge.delete_experiment(experiment_id, False, self.store_uri, self.artifact_uri)

	def restore_experiment(self, experiment_id):
		self.maplarge.restore_experiment(experiment_id, self.store_uri, self.artifact_uri)

	def rename_experiment(self, experiment_id, new_name):
		_validate_experiment_name(new_name)
		self.maplarge.rename_experiment(experiment_id, new_name, self.store_uri, self.artifact_uri)

	def get_run(self, run_id):
		return self.maplarge.get_run(run_id, False, self.store_uri, self.artifact_uri)

	def update_run_info(self, run_id, run_status, end_time, run_name):
		return self.maplarge.update_run_info(run_id, run_status, end_time, run_name, self.store_uri, self.artifact_uri)

	def create_run(self, experiment_id, user_id, start_time, tags, run_name):
		tags = tags or []
		run_name_tag = _get_run_name_from_tags(tags)
		if run_name and run_name_tag and (run_name != run_name_tag):
			raise MlflowException(
				"Both 'run_name' argument and 'mlflow.runName' tag are specified, but with "
				f"different values (run_name='{run_name}', mlflow.runName='{run_name_tag}').",
				databricks_pb2.INVALID_PARAMETER_VALUE,
			)

		run_name = run_name or run_name_tag or _generate_random_name()
		if not run_name_tag:
			tags.append(RunTag(key=mlflow_tags.MLFLOW_RUN_NAME, value=run_name))

		return self.maplarge.create_run(experiment_id, user_id, start_time, tags, run_name, self.store_uri, self.artifact_uri)

	def delete_run(self, run_id):
		self.maplarge.delete_run(run_id, False, self.store_uri, self.artifact_uri)

	def restore_run(self, run_id):
		self.maplarge.restore_run(run_id, self.store_uri, self.artifact_uri)

	def set_experiment_tag(self, experiment_id, tag):
		_validate_experiment_tag(tag.key, tag.value)
		return self.maplarge.set_experiment_tag(experiment_id, tag, self.store_uri, self.artifact_uri)

	def get_metric_history(self, run_id, metric_key, max_results=None, page_token=None):
		return self.maplarge.get_metric_history(run_id, metric_key, max_results, page_token, self.store_uri, self.artifact_uri)

	def _search_runs(self, experiment_ids, filter_string, run_view_type, max_results, order_by, page_token):
		return self.maplarge.search_runs(experiment_ids, filter_string, run_view_type, max_results, order_by, page_token, self.store_uri, self.artifact_uri)

	def log_batch(self, run_id, metrics, params, tags):
		if run_id is None or run_id == "":
			raise MlflowException("Missing run ID", error_code=databricks_pb2.INVALID_PARAMETER_VALUE)

		metrics, params, tags = _validate_batch_log_data(metrics or [], params or [], tags or [])
		_validate_batch_log_limits(metrics, params, tags)
		_validate_param_keys_unique(params)

		tags = [_validate_tag(t.key, t.value) for t in tags]

		self.maplarge.log_batch(run_id, metrics, params, tags, self.store_uri, self.artifact_uri)

	def record_logged_model(self, run_id, mlflow_model):
		self.maplarge.record_logged_model(run_id, mlflow_model, self.store_uri, self.artifact_uri)

	def log_inputs(self, run_id, datasets = None):
		if run_id is None or run_id == "":
			raise MlflowException("Missing run ID", databricks_pb2.INVALID_PARAMETER_VALUE)

		if datasets is not None:
			if not isinstance(datasets, list):
				raise TypeError(f"Argument 'datasets' should be a list, got '{type(datasets)}'")
			_validate_dataset_inputs(datasets)

		self.maplarge.log_inputs(run_id, datasets, self.store_uri, self.artifact_uri)
