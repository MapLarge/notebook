from setuptools import find_packages, setup

setup(
	name="mlflowmaplarge",
	version="1.0.0",
	description="MapLarge plugin for MLFlow",
	packages=find_packages(),
	install_required=["mlflow"],
	entry_points={
		"mlflow.tracking_store": "maplarge=mlflowmaplarge.maplarge_store:MapLargeStore",
		"mlflow.artifact_repository": "maplarge=mlflowmaplarge.maplarge_artifact:MapLargeArtifact",
		"mlflow.model_registry_store": "maplarge=mlflowmaplarge.maplarge_model_registry:MapLargeModelRegistry",
		"mlflow.default_experiment_provider": "MapLargeDefaultExperimentProvider=mlflowmaplarge.maplarge_experiment_provider:MapLargeDefaultExperimentProvider"
	}
)
