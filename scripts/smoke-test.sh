#!/usr/bin/env bash
# End-to-end smoke test of a dicom-router image against Orthanc, in cleartext:
# C-ECHO, C-STORE, C-FIND and C-GET through the router, then compare the
# retrieved pixel data with what was sent.
#
# Usage:
#   scripts/smoke-test.sh <router-image>
#
# Requires docker and DCMTK (echoscu, storescu, findscu, getscu, dump2dcm,
# dcmdump). Env: ORTHANC_IMAGE (default orthancteam/orthanc), PORT (default
# 11112, published on 127.0.0.1).
set -euo pipefail

IMAGE="${1:?usage: $0 <router-image>}"
ORTHANC_IMAGE="${ORTHANC_IMAGE:-orthancteam/orthanc}"
PORT="${PORT:-11112}"

ID="smoke-$$"
WORK="$(mktemp -d)"
STUDY_UID="1.2.826.0.1.3680043.10.1.$$"
SOP_UID="$STUDY_UID.1.1"

cleanup() {
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "--- router logs ---" >&2
    docker logs "$ID-router" >&2 || true
  fi
  docker rm -f "$ID-router" "$ID-orthanc" >/dev/null 2>&1 || true
  docker network rm "$ID" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $rc
}
trap cleanup EXIT

step() { echo "==> $*" >&2; }

# Retry a command for up to $1 seconds.
retry() {
  local timeout=$1
  shift
  for _ in $(seq "$timeout"); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  "$@"
}

# --- Test image: a 4x4 16-bit CT slice ---------------------------------------
cat >"$WORK/ct.dump" <<EOF
(0008,0016) UI [1.2.840.10008.5.1.4.1.1.2]
(0008,0018) UI [$SOP_UID]
(0008,0060) CS [CT]
(0010,0010) PN [Smoke^Test]
(0010,0020) LO [SMOKE]
(0020,000d) UI [$STUDY_UID]
(0020,000e) UI [$STUDY_UID.1]
(0028,0002) US 1
(0028,0004) CS [MONOCHROME2]
(0028,0010) US 4
(0028,0011) US 4
(0028,0100) US 16
(0028,0101) US 16
(0028,0102) US 15
(0028,0103) US 0
(7fe0,0010) OW 0000\0001\0002\0003\0010\0011\0012\0013\0100\0101\0102\0103\1000\1001\1002\1003
EOF
dump2dcm --write-xfer-little "$WORK/ct.dump" "$WORK/ct.dcm"

# The router runs as UID 10001 and must be able to read the mounted config.
chmod 755 "$WORK"
cat >"$WORK/config.yaml" <<EOF
listen_addr: "0.0.0.0:11112"
ae_title: "ROUTER"
queue_dir: "/var/lib/dicom-router/queue"
dead_letter_dir: "/var/lib/dicom-router/dead-letter"
tls:
  enabled: false
destinations:
  - name: orthanc
    ae_title: "ORTHANC"
    host: "$ID-orthanc"
    port: 4242
    tls: false
query_retrieve:
  destination: orthanc
  allowed_ae_titles: ["VIEWER"]
EOF
chmod 644 "$WORK/config.yaml"

# --- Start Orthanc and the router --------------------------------------------
step "starting Orthanc and $IMAGE"
docker network create "$ID" >/dev/null
docker run -d --name "$ID-orthanc" --network "$ID" \
  -e ORTHANC__AUTHENTICATION_ENABLED=false \
  -e ORTHANC__DICOM_MODALITIES="{\"router\": [\"ROUTER\", \"$ID-router\", 11112]}" \
  "$ORTHANC_IMAGE" >/dev/null
docker run -d --name "$ID-router" --network "$ID" -p "127.0.0.1:$PORT:11112" \
  -v "$WORK/config.yaml:/etc/dicom-router/config.yaml:ro" \
  "$IMAGE" >/dev/null

# --- DIMSE checks ------------------------------------------------------------
step "C-ECHO"
retry 30 echoscu -aet VIEWER -aec ROUTER 127.0.0.1 "$PORT"

step "C-STORE"
storescu -aet SMOKE -aec ROUTER 127.0.0.1 "$PORT" "$WORK/ct.dcm"

step "C-FIND (waits until the router has forwarded the image)"
find_instance() {
  findscu -aet VIEWER -aec ROUTER -S \
    -k QueryRetrieveLevel=IMAGE -k StudyInstanceUID="$STUDY_UID" -k SeriesInstanceUID="$STUDY_UID.1" \
    -k SOPInstanceUID 127.0.0.1 "$PORT" 2>&1 | grep -aq "$SOP_UID"
}
retry 60 find_instance

step "C-GET"
mkdir "$WORK/out"
getscu -aet VIEWER -aec ROUTER -S -k QueryRetrieveLevel=STUDY -k StudyInstanceUID="$STUDY_UID" \
  -od "$WORK/out" 127.0.0.1 "$PORT"
retrieved="$(find "$WORK/out" -type f | head -1)"
[[ -n "$retrieved" ]] || { echo "C-GET returned no images" >&2; exit 1; }

step "comparing pixel data"
cmp <(dcmdump -M +P 7fe0,0010 "$WORK/ct.dcm") <(dcmdump -M +P 7fe0,0010 "$retrieved")

step "smoke test passed"
