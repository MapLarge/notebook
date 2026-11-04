import os
import posixpath
import urllib.parse

from mlflow.store.artifact.artifact_repo import ArtifactRepository, verify_artifact_path
from mlflow.utils.file_utils import relative_path_to_artifact_path
from mlflow.exceptions import MlflowException
from mlflow.protos import databricks_pb2

from .maplarge_mlflow_client import MapLargeMLFlowClient

"""
Artifact repository that connects to MapLarge's database.
"""
class MapLargeArtifact(ArtifactRepository):
	def __init__(self, artifact_uri):
		super().__init__(artifact_uri)
		self.maplarge = MapLargeMLFlowClient.get_or_initialize_default_connection()

	def log_artifact(self, local_file, artifact_path=None):
		dest_path = self._create_artifact_path(artifact_path)
		dest_path = posixpath.join(dest_path, os.path.basename(local_file))
		with open (local_file, "rb") as file:
			self.maplarge.log_artifact(file, dest_path, self.artifact_uri)

	#This is copied from the built-in http_artifact_repo (HttpArtifactRepository)
	def log_artifacts(self, local_dir, artifact_path=None):
		local_dir = os.path.abspath(local_dir)
		for root, _, filenames in os.walk(local_dir):
			if root == local_dir:
				artifact_dir = artifact_path
			else:
				rel_path = os.path.relpath(root, local_dir)
				rel_path = relative_path_to_artifact_path(rel_path)
				artifact_dir = (
					posixpath.join(artifact_path, rel_path) if artifact_path else rel_path
				)
			for f in filenames:
				self.log_artifact(os.path.join(root, f), artifact_dir)

	def list_artifacts(self, path):
		search_path = self._create_artifact_path(path)
		return self.maplarge.list_artifacts(search_path, self.artifact_uri)

	def _download_file(self, remote_file_path, local_path):
		path_prefix = self._create_artifact_path()
		if not remote_file_path.startswith(path_prefix):
			if remote_file_path.startswith("/"):
				remote_file_path = remote_file_path[1:]
			remote_file_path = posixpath.join(path_prefix, remote_file_path)
		fileContents = self.maplarge.download_file(remote_file_path, self.artifact_uri)
		with open(local_path, "wb") as f:
			f.write(fileContents.getbuffer())

	def delete_artifacts(self, artifact_path=None):
		delete_path = self._create_artifact_path(artifact_path)
		self.maplarge.delete_artifacts(delete_path, self.artifact_uri)

	def _create_artifact_path(self, path=None):
		verify_artifact_path(path)
		parsed = urllib.parse.urlparse(self.artifact_uri)
		if parsed.scheme != "maplarge":
			raise MlflowException(f"Not a maplarge uri: {self.artifact_uri}", databricks_pb2.INVALID_PARAMETER_VALUE)
		result = parsed.path
		if result.startswith("/"):
			result = result[1:]
		if path:
			result = posixpath.join(result, path)
		return result
