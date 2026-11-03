# Kernel images. Run from the repository root.
#   make build-<kernel>   build the kernel and its parents for this machine as <image>:dev
#   make lock-<kernel>    regenerate kernels/<kernel>/requirements.txt from requirements.in
#   make test-<kernel>    smoke test the :dev image
# Kernels, in parent order: base-cpu -> cpu-analytics -> ai-ml-gpu
KERNELS=base-cpu cpu-analytics ai-ml-gpu
VERSION?=dev

# Image repository of each kernel (see docker-bake.hcl)
IMAGE_base-cpu=maplarge/ipython-kernel-base
IMAGE_cpu-analytics=maplarge/ipython-kernel
IMAGE_ai-ml-gpu=maplarge/ipython-kernel-gpu

# bake targets to build for each kernel: the kernel and its parents, so every image loads into Docker.
TARGETS_base-cpu=base-cpu
TARGETS_cpu-analytics=base-cpu cpu-analytics
TARGETS_ai-ml-gpu=base-cpu cpu-analytics ai-ml-gpu

# Local builds load into the Docker daemon, so they build for this machine only.
PLATFORM?=linux/$(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')

# Every kernel shares the Python of the base image, so every lock resolves in it on linux/amd64, the production platform.
LOCK_IMAGE=$(shell sed -n 's/^FROM //p' kernels/base-cpu/Dockerfile | head -1)

build-%:
	VERSION=$(VERSION) docker buildx bake $(TARGETS_$*) --load --set '*.platform=$(PLATFORM)'

# Lock the parent first when the parent changed: a derived lock reads the parent lock as constraints.
lock-%:
	docker run --rm --platform linux/amd64 -v $(PWD)/kernels:/work -w /work/$* $(LOCK_IMAGE) \
		sh -c "pip install -q pip-tools && pip-compile --quiet --strip-extras --allow-unsafe --output-file requirements.txt requirements.in"

test-%:
	scripts/smoke-test.sh $(IMAGE_$*):$(VERSION) kernels/$*

build-all: build-ai-ml-gpu
lock-all: lock-base-cpu lock-cpu-analytics lock-ai-ml-gpu
test-all: $(addprefix test-,$(KERNELS))

.PHONY: build-all lock-all test-all
