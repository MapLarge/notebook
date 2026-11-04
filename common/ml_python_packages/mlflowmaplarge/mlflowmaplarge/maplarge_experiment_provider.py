import json
import os

from mlflow.tracking.default_experiment.abstract_context import DefaultExperimentProvider
from mlflow.tracking._tracking_service.utils import get_tracking_uri
from mlflow.exceptions import MlflowException
from mlflow.protos import databricks_pb2
from mlflow.utils.name_utils import _generate_random_name
from mlflow.tracking.client import MlflowClient

class MapLargeDefaultExperimentProvider(DefaultExperimentProvider):
	_resolved_experiment_id = None
	_config = None
	_notebook_name = None

	def in_context(self):
		#According to the docs if somebody calls set_tracking_uri this may be incorrect.
		tracking_uri = get_tracking_uri()
		return tracking_uri != None and tracking_uri.startswith("maplarge:/")

	#Assume in_context() is True.
	def get_experiment_id(self):
		if MapLargeDefaultExperimentProvider._resolved_experiment_id:
			return MapLargeDefaultExperimentProvider._resolved_experiment_id

		experiment_id = None
		name_in_config = self.try_get_name_from_config()
		if name_in_config != None and name_in_config != "":
			experiment_id = self.try_create_with_name(name_in_config)

		if experiment_id == None:
			experiment_id = self.try_create_with_random_name()

		if experiment_id == None:
			raise MlflowException("Unable to create default experiment. Max attempts exceeded.", databricks_pb2.INTERNAL_ERROR)

		MapLargeDefaultExperimentProvider._resolved_experiment_id = experiment_id
		return experiment_id

	def try_get_name_from_config(self):
		if MapLargeDefaultExperimentProvider._notebook_name != None:
			return MapLargeDefaultExperimentProvider._notebook_name

		try:
			if MapLargeDefaultExperimentProvider._config == None:
				MapLargeDefaultExperimentProvider._config = json.load(open(os.environ['ML_CONFIG']))

			MapLargeDefaultExperimentProvider._notebook_name = MapLargeDefaultExperimentProvider._config["notebookName"]
			return MapLargeDefaultExperimentProvider._notebook_name
		except:
			return None

	def try_create_with_name(self, name_in_config):
		if name_in_config == None or name_in_config == "":
			return None

		for attemptNumber in range(10):
			experimentName = None
			if attemptNumber == 0:
				experimentName = name_in_config
			else:
				experimentName = name_in_config + " " + str(attemptNumber)

			try:
				return MlflowClient().create_experiment(experimentName, None, { "maplarge.defaultExperiment": "" })
			except MlflowException as e:
				if e.error_code == databricks_pb2.ErrorCode.Name(databricks_pb2.INVALID_PARAMETER_VALUE):
					return None
				elif e.error_code != databricks_pb2.ErrorCode.Name(databricks_pb2.RESOURCE_ALREADY_EXISTS):
					raise e

		return None

	def try_create_with_random_name(self):
		for _ in range(10):
			try:
				return MlflowClient().create_experiment(_generate_random_name(), None, { "maplarge.defaultExperiment": "" })
			except MlflowException as e:
				if e.error_code != databricks_pb2.ErrorCode.Name(databricks_pb2.RESOURCE_ALREADY_EXISTS):
					raise e

		return None
