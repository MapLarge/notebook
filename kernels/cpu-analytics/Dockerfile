# CPU Analytics kernel: the Base CPU kernel plus the spatial, raster, image and numerical packages.
# Build from the repository root (the parent builds first):
#   make build-cpu-analytics
ARG PARENT_IMAGE=maplarge/notebook:latest-slim
FROM ${PARENT_IMAGE}

# Same values as the parent. The user already exists; these only select it again after the installs.
ARG ML_UID=1000
ARG ML_GID=1000

# The parent runs as the kernel user. Package installs need root.
USER root

# libexpat1: needed by the rasterio wheel.
RUN apt-get update && apt-get install -y --no-install-recommends \
	libexpat1 \
	&& rm -rf /var/lib/apt/lists/*

# Full lock, resolved with the parent lock as constraints. pip skips the packages the parent already installed.
# Edit requirements.in, then regenerate requirements.txt with "make lock-cpu-analytics".
COPY kernels/cpu-analytics/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
	&& rm /tmp/requirements.txt

USER ${ML_UID}:${ML_GID}
