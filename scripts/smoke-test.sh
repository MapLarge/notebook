#!/usr/bin/env bash
# Smoke test for a kernel image.
# Usage: scripts/smoke-test.sh <image> <kernel directory>
# Checks: non-root user and working directory, entrypoint starts the kernel, mlgetfiles.py runs,
# and every explicit package in requirements.in imports.
set -u
IMAGE=$1
KERNEL_DIR=$2
HERE=$(cd "$(dirname "$0")/.." && pwd)
FAIL=0

pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; FAIL=1; }

# Import name differs from the pip name for these packages.
import_name() {
	case "$1" in
		opencv-python-headless) echo cv2 ;;
		scikit-learn) echo sklearn ;;
		scikit-image) echo skimage ;;
		pillow) echo PIL ;;
		mlflow-skinny) echo mlflow ;;
		ipython) echo IPython ;;
		*) echo "$1" | tr '-' '_' ;;
	esac
}

# Explicit packages of a requirements.in, following "-r" includes of parent kernels. "-c" lines are skipped.
explicit_packages() {
	local f=$1 dir line
	dir=$(dirname "$f")
	grep -vE '^\s*(#|$|-c )' "$f" | while read -r line; do
		case "$line" in
			-r\ *) explicit_packages "$dir/${line#-r }" ;;
			*) echo "$line" | sed -E 's/[<>=!~\[ ].*//' ;;
		esac
	done
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/config"
cp "$HERE/scripts/sample_connection_file.json" "$WORK/config/connection-file.json"
echo '{"mode":"Interactive","port":"6000","hbReqPort":"6001","hbRepPort":"6002","logLevel":"WARN","userParameters":"{}","secrets":"[]"}' > "$WORK/config/maplarge-config.json"
RUN="docker run --rm -v $WORK/config:/tmp/config:ro -e ML_CONFIG=/tmp/config/maplarge-config.json"

# 1. user and working directory
OUT=$($RUN --entrypoint sh "$IMAGE" -c 'echo "$(id -u) $(pwd)"')
[ "$OUT" = "1000 /home/maplarge" ] && pass "runs as uid 1000 in /home/maplarge" || fail "user/cwd: got '$OUT'"

# 2. entrypoint starts ipykernel (killed after 15 s)
OUT=$($RUN --entrypoint sh "$IMAGE" -c 'timeout 15 ./entrypoint.sh 2>&1; true')
echo "$OUT" | grep -q "connect another client" && pass "entrypoint starts the kernel" || { fail "entrypoint"; echo "$OUT" | tail -15; }
echo "$OUT" | grep -qi "permission denied" && fail "entrypoint logged a permission error"

# 3. file listing helper used by the server
OUT=$($RUN --entrypoint python3 "$IMAGE" mlgetfiles.py /tmp/config)
echo "$OUT" | grep -q 'connection-file.json' && pass "mlgetfiles.py lists /tmp/config" || fail "mlgetfiles.py: $OUT"

# 4. every explicit package imports, plus the two local packages
PKGS=$(explicit_packages "$HERE/$KERNEL_DIR/requirements.in")
MODULES="maplargeclient mlflowmaplarge"
for p in $PKGS; do MODULES="$MODULES $(import_name "$p")"; done
OUT=$($RUN --entrypoint python3 "$IMAGE" -c "
import importlib, sys
bad = []
for m in '$MODULES'.split():
    try:
        importlib.import_module(m)
    except Exception as e:
        bad.append(f'{m}: {e}')
print('\n'.join(bad))
sys.exit(1 if bad else 0)
") && pass "all $(echo $MODULES | wc -w | tr -d ' ') modules import" || { fail "imports"; echo "$OUT"; }

# 5. GPU kernel: report CUDA availability (informational, not a failure on hosts without a GPU)
if [ -f "$HERE/$KERNEL_DIR/requirements.in" ] && grep -q '^torch' "$HERE/$KERNEL_DIR/requirements.in"; then
	$RUN --entrypoint python3 "$IMAGE" -c "import torch; print('INFO  torch', torch.__version__, 'cuda available:', torch.cuda.is_available())"
fi

exit $FAIL
