ARG BASE_REGISTRY=docker.io
ARG BASE_IMAGE=python
ARG BASE_TAG=3.12

FROM ${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}

RUN pip3 install ipython ipykernel pandas numpy

WORKDIR /maplarge

COPY maplarge/ .

RUN apt-get update -y

RUN pip3 install /maplarge/ml_python_packages/maplargeclient

# Create a non-root user and group to run the application
RUN groupadd -r maplarge && useradd -r -g maplarge -u 1000 -m maplarge

# MapLarge mounts the docker volume to /tmp/config but unless
# permissions are declared here, it will be mounted as root and
# this container will be unable to write to it. These other directories
# are modified so python can write packages to the preferred locations.
# A user is created and the chgrp and chmod commands are used to allow
# the container to run in OpenShift.
ARG DIRS="/tmp/config /maplarge /usr/local/lib/python*/site-packages /usr/local/bin /home/maplarge"

RUN for dir in $DIRS; do \
  mkdir -p $dir && \
  chgrp -R 0 $dir && \
  chmod -R g=u $dir && \
  chown -R 1000:maplarge $dir ; \
  done && \
  chmod a+x /maplarge/entrypoint.sh

# Change to a non-root user
USER 1000

# At runtime, mount the connection file to /tmp/connection_file.json
ENTRYPOINT ["/maplarge/entrypoint.sh"]