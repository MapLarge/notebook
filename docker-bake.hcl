// Build definition for the kernel images. The Makefile and CI both use this file.
// bake builds the parent of a kernel first and passes it to the child as the named context "parent".
//
//   docker buildx bake --print                 show the resolved build
//   docker buildx bake cpu-analytics --load    build one kernel and its parent for one platform
//   VERSION=4.142.0 LATEST=true docker buildx bake --push

variable "VERSION" {
  default = "dev"
}

// true also tags each image as :latest
variable "LATEST" {
  default = false
}

variable "REGISTRIES" {
  default = ["docker.io", "ghcr.io"]
}

variable "NAMESPACE" {
  default = "maplarge"
}

function "tags" {
  params = [name]
  result = flatten([
    for registry in REGISTRIES : concat(
      ["${registry}/${NAMESPACE}/${name}:${VERSION}"],
      LATEST ? ["${registry}/${NAMESPACE}/${name}:latest"] : []
    )
  ])
}

group "default" {
  targets = ["base-cpu", "cpu-analytics", "ai-ml-gpu"]
}

target "base-cpu" {
  context    = "."
  dockerfile = "kernels/base-cpu/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = tags("ipython-kernel-base")
}

target "cpu-analytics" {
  context    = "."
  dockerfile = "kernels/cpu-analytics/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  contexts   = { parent = "target:base-cpu" }
  args       = { PARENT_IMAGE = "parent" }
  tags       = tags("ipython-kernel")
}

// amd64 only: the CUDA libraries that torch pulls in are large, and MapLarge GPU hosts are amd64.
target "ai-ml-gpu" {
  context    = "."
  dockerfile = "kernels/ai-ml-gpu/Dockerfile"
  platforms  = ["linux/amd64"]
  contexts   = { parent = "target:cpu-analytics" }
  args       = { PARENT_IMAGE = "parent" }
  tags       = tags("ipython-kernel-gpu")
}
