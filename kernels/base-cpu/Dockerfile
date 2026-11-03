# Base CPU kernel: the parent of every MapLarge notebook kernel image.
# Build from the repository root:
#   make build-base-cpu
FROM python:3.13.18-slim

# The kernel runs as this non-root user.
ARG ML_UID=1000
ARG ML_GID=1000
ARG ML_USER=maplarge
ARG ML_HOME=/home/maplarge

# Locked dependencies. Edit requirements.in, then regenerate requirements.txt with "make lock-base-cpu".
COPY kernels/base-cpu/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip \
	&& pip3 install --no-cache-dir -r /tmp/requirements.txt \
	&& rm /tmp/requirements.txt

RUN groupadd --gid ${ML_GID} ${ML_USER} \
	&& useradd --uid ${ML_UID} --gid ${ML_GID} --create-home --home-dir ${ML_HOME} --shell /bin/bash ${ML_USER}

# The entrypoint and the MapLarge server ("python3 mlgetfiles.py", "import maplarge") reference these files
# relative to the working directory.
WORKDIR ${ML_HOME}

COPY --chown=${ML_UID}:${ML_GID} common/entrypoint.sh entrypoint.sh
COPY --chown=${ML_UID}:${ML_GID} common/maplarge.py maplarge.py
COPY --chown=${ML_UID}:${ML_GID} common/mlgetfiles.py mlgetfiles.py
COPY --chown=${ML_UID}:${ML_GID} common/ml_python_packages ml_python_packages

# --no-deps: the dependencies of these two packages are already in the lock file.
RUN pip3 install --no-cache-dir --no-deps ./ml_python_packages/maplargeclient ./ml_python_packages/mlflowmaplarge

# Group 0 owns the home directory with the same rights as the user. Platforms that run containers
# with an arbitrary uid in group 0 (for example OpenShift) can then write to it.
RUN chgrp -R 0 ${ML_HOME} && chmod -R g=u ${ML_HOME}

# Hides the git warning from mlflow.
ENV GIT_PYTHON_REFRESH=quiet

# IPython writes under HOME, and pip installs to the user site under HOME when site-packages is not writable.
ENV HOME=${ML_HOME}

USER ${ML_UID}:${ML_GID}

# MapLarge passes the connection file path as the first argument.
ENTRYPOINT ["./entrypoint.sh"]
