ARG BASE_REGISTRY=
ARG BASE_IMAGE=
ARG BASE_TAG=

FROM ${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}

RUN pip3 install ipython ipykernel pandas numpy

WORKDIR /maplarge

COPY maplarge/ .

RUN dnf update -y

RUN pip3 install /maplarge/ml_python_packages/maplargeclient

# MapLarge mounts the docker volume to /tmp/config but unless
# permissions are declared here, it will be mounted as root and
# this container will be unable to write to it
RUN mkdir -p /tmp/config && chgrp -R 0 /tmp/config && \
  chmod -R g=u /tmp/config && \
  chmod a+x /maplarge/entrypoint.sh && \
  chgrp -R 0 /maplarge && \
  chmod -R g=u /maplarge

# Change to a non-root user
USER 1000

# At runtime, mount the connection file to /tmp/connection_file.json
ENTRYPOINT [ "/maplarge/entrypoint.sh"]
