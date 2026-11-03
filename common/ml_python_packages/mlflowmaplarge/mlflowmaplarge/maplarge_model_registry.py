from mlflow.store.model_registry.abstract_store import AbstractStore
from mlflow.utils.validation import _validate_model_alias_name, _validate_model_name, _validate_model_version, _validate_model_version_tag, _validate_registered_model_tag, _validate_tag_name
from mlflow.entities.model_registry.model_version_stages import DEFAULT_STAGES_FOR_GET_LATEST_VERSIONS, get_canonical_stage

from .maplarge_mlflow_client import MapLargeMLFlowClient

"""
Model registry store that connects to MapLarge's database.
"""
class MapLargeModelRegistry(AbstractStore):
	def __init__(self, store_uri=None, tracking_uri=None):
		super().__init__(store_uri, tracking_uri)
		self.store_uri = store_uri
		self.tracking_uri = tracking_uri
		self.maplarge = MapLargeMLFlowClient.get_or_initialize_default_connection()

	def create_registered_model(self, name, tags=None, description=None):
		_validate_model_name(name)
		for tag in tags or []:
			_validate_registered_model_tag(tag.key, tag.value)
		return self.maplarge.create_registered_model(name, tags, description, self.store_uri, self.tracking_uri)

	def update_registered_model(self, name, description):
		_validate_model_name(name)
		return self.maplarge.update_registered_model(name, description, self.store_uri, self.tracking_uri)

	def rename_registered_model(self, name, new_name):
		_validate_model_name(name)
		_validate_model_name(new_name)
		return self.maplarge.rename_registered_model(name, new_name, self.store_uri, self.tracking_uri)

	def delete_registered_model(self, name):
		_validate_model_name(name)
		self.maplarge.delete_registered_model(name, self.store_uri, self.tracking_uri)

	def search_registered_models(self, filter_string=None, max_results=None, order_by=None, page_token=None):
		return self.maplarge.search_registered_models(filter_string, max_results, order_by, page_token, self.store_uri, self.tracking_uri)

	def get_registered_model(self, name):
		_validate_model_name(name)
		return self.maplarge.get_registered_model(name, self.store_uri, self.tracking_uri)

	def get_latest_versions(self, name, stages=None):
		_validate_model_name(name)
		return self.maplarge.get_latest_versions(name, stages, self.store_uri, self.tracking_uri)

	def set_registered_model_tag(self, name, tag):
		_validate_model_name(name)
		_validate_registered_model_tag(tag.key, tag.value)
		self.maplarge.set_registered_model_tag(name, tag, self.store_uri, self.tracking_uri)

	def delete_registered_model_tag(self, name, key):
		_validate_model_name(name)
		_validate_tag_name(key)
		self.maplarge.delete_registered_model_tag(name, key, self.store_uri, self.tracking_uri)

	def create_model_version(self, name, source, run_id=None, tags=None, run_link=None, description=None, local_model_path=None):
		_validate_model_name(name)
		for tag in tags or []:
			_validate_model_version_tag(tag.key, tag.value)
		return self.maplarge.create_model_version(name, source, run_id, tags, run_link, description, local_model_path, self.store_uri, self.tracking_uri)

	def update_model_version(self, name, version, description):
		_validate_model_name(name)
		return self.maplarge.update_model_version(name, int(version), description, self.store_uri, self.tracking_uri)

	def transition_model_version_stage(self, name, version, stage, archive_existing_versions):
		_validate_model_name(name)
		is_active_stage = get_canonical_stage(stage) in DEFAULT_STAGES_FOR_GET_LATEST_VERSIONS
		if archive_existing_versions and not is_active_stage:
			from mlflow import MlflowException
			msg_tpl = "Model version transition cannot archive existing model versions because '{}' is not an Active stage. Valid stages are {}"
			raise MlflowException(msg_tpl.format(stage, DEFAULT_STAGES_FOR_GET_LATEST_VERSIONS))
		return self.maplarge.transition_model_version_stage(name, int(version), stage, archive_existing_versions, self.store_uri, self.tracking_uri)

	def delete_model_version(self, name, version):
		_validate_model_name(name)
		self.maplarge.delete_model_version(name, int(version), self.store_uri, self.tracking_uri)

	def get_model_version(self, name, version):
		_validate_model_name(name)
		return self.maplarge.get_model_version(name, int(version), self.store_uri, self.tracking_uri)

	def get_model_version_download_uri(self, name, version):
		_validate_model_name(name)
		return self.maplarge.get_model_version_download_uri(name, int(version), self.store_uri, self.tracking_uri)

	def search_model_versions(self, filter_string=None, max_results=None, order_by=None, page_token=None):
		return self.maplarge.search_model_versions(filter_string, max_results, order_by, page_token, self.store_uri, self.tracking_uri)

	def set_model_version_tag(self, name, version, tag):
		_validate_model_name(name)
		_validate_model_version(version)
		_validate_model_version_tag(tag.key, tag.value)
		self.maplarge.set_model_version_tag(name, int(version), tag, self.store_uri, self.tracking_uri)

	def delete_model_version_tag(self, name, version, key):
		_validate_model_name(name)
		_validate_model_version(version)
		_validate_tag_name(key)
		self.maplarge.delete_model_version_tag(name, int(version), key, self.store_uri, self.tracking_uri)

	def set_registered_model_alias(self, name, alias, version):
		_validate_model_name(name)
		_validate_model_alias_name(alias)
		_validate_model_version(version)
		self.maplarge.set_registered_model_alias(name, alias, int(version), self.store_uri, self.tracking_uri)

	def delete_registered_model_alias(self, name, alias):
		_validate_model_name(name)
		_validate_model_alias_name(alias)
		self.maplarge.delete_registered_model_alias(name, alias, self.store_uri, self.tracking_uri)

	def get_model_version_by_alias(self, name, alias):
		_validate_model_name(name)
		_validate_model_alias_name(alias)
		return self.maplarge.get_model_version_by_alias(name, alias, self.store_uri, self.tracking_uri)
