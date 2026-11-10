# AI/ML GPU kernel: the CPU Analytics kernel plus torch, torchvision, torchmetrics, onnx and scikit-image.
# torch installs from PyPI and brings CUDA and cuDNN as nvidia-* pip packages. The image has no CUDA
# toolkit; the host provides the NVIDIA driver and the NVIDIA container runtime.
# Build from the repository root (the parents build first):
#   make build-ai-ml-gpu
ARG PARENT_IMAGE=maplarge/notebook:latest
FROM ${PARENT_IMAGE}

# Same values as the parent. The user already exists; these only select it again after the installs.
ARG ML_UID=1000
ARG ML_GID=1000

# The parent runs as the kernel user. Package installs need root.
USER root

# Full lock, resolved with the parent lock as constraints. pip skips the packages the parent already installed.
# Edit requirements.in, then regenerate requirements.txt with "make lock-ai-ml-gpu".
COPY kernels/ai-ml-gpu/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
	&& rm /tmp/requirements.txt

USER ${ML_UID}:${ML_GID}
