// Build definition for the kernel images. The Makefile and CI both use this file.
// bake builds the parent of a kernel first and passes it to the child as the named context "parent".
//
//   docker buildx bake --print                 show the resolved build
//   docker buildx bake cpu-analytics --load    build one kernel and its parent for one platform
//   VERSION=4.143.0 RELEASE=true docker buildx bake --push

// Image version. A release uses the MapLarge server version that the images require, for example 4.143.0.
variable "VERSION" {
  default = "dev"
}

// true also tags each image with the minor version (4.143.0 -> 4.143) and latest.
// The MapLarge server defaults to the minor version tag, so only a full release sets this.
variable "RELEASE" {
  default = false
}

variable "REGISTRIES" {
  default = ["docker.io", "ghcr.io"]
}

// Every kernel publishes to this one repository. The tag suffix selects the kernel:
//   slim  4.143.0-slim  4.143-slim  latest-slim
//   cpu   4.143.0       4.143       latest
//   gpu   4.143.0-gpu   4.143-gpu   latest-gpu
variable "IMAGE" {
  default = "maplarge/notebook"
}

// 4.143.0 -> ["4.143"]. Anything that is not X.Y.Z (dev, 4.143.0-rc1) -> [].
function "minor" {
  params = [version]
  result = can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+$", version)) ? [regex_replace(version, "^([0-9]+\\.[0-9]+)\\..*$", "$1")] : []
}

function "tags" {
  params = [suffix]
  result = flatten([
    for registry in REGISTRIES : [
      for version in concat([VERSION], RELEASE ? concat(minor(VERSION), ["latest"]) : []) :
      "${registry}/${IMAGE}:${version}${suffix}"
    ]
  ])
}

group "default" {
  targets = ["base-cpu", "cpu-analytics", "ai-ml-gpu"]
}

target "base-cpu" {
  context    = "."
  dockerfile = "kernels/base-cpu/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = tags("-slim")
}

// The default kernel: no tag suffix. The MapLarge server pulls this one unless an image record says otherwise.
target "cpu-analytics" {
  context    = "."
  dockerfile = "kernels/cpu-analytics/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  contexts   = { parent = "target:base-cpu" }
  args       = { PARENT_IMAGE = "parent" }
  tags       = tags("")
}

// amd64 only: the CUDA libraries that torch pulls in are large, and MapLarge GPU hosts are amd64.
target "ai-ml-gpu" {
  context    = "."
  dockerfile = "kernels/ai-ml-gpu/Dockerfile"
  platforms  = ["linux/amd64"]
  contexts   = { parent = "target:cpu-analytics" }
  args       = { PARENT_IMAGE = "parent" }
  tags       = tags("-gpu")
}
