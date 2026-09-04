#!/usr/bin/env bash
# Generate a throwaway CA + server + client PKI for local development and manual testing of
# dicom-router's TLS listener/dialer. NOT FOR PRODUCTION USE — keys are written unencrypted to
# disk with no passphrase.
#
# Usage:
#   scripts/gen-dev-certs.sh [output-dir]
#
# Output dir defaults to ./dev-certs. Writes (all PEM):
#   ca.crt, ca.key                 - self-signed dev CA
#   server.crt, server.key         - server cert, SAN=DNS:localhost,IP:127.0.0.1
#   client.crt, client.key         - client cert, for mTLS destinations
set -euo pipefail

OUT_DIR="${1:-./dev-certs}"
DAYS="${DAYS:-825}"

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

echo "Generating dev PKI in $OUT_DIR" >&2

# --- CA -----------------------------------------------------------------
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 \
  -out "$OUT_DIR/ca.key"

openssl req -x509 -new -key "$OUT_DIR/ca.key" -sha256 -days "$DAYS" \
  -subj "/CN=dicom-router dev CA" \
  -out "$OUT_DIR/ca.crt"

# --- Server certificate (SAN: localhost) ---------------------------------
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$OUT_DIR/server.key"

openssl req -new -key "$OUT_DIR/server.key" \
  -subj "/CN=dicom-router dev server" \
  -out "$OUT_DIR/server.csr"

cat >"$OUT_DIR/server.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -in "$OUT_DIR/server.csr" \
  -CA "$OUT_DIR/ca.crt" -CAkey "$OUT_DIR/ca.key" -CAcreateserial \
  -days "$DAYS" -sha256 -extfile "$OUT_DIR/server.ext" \
  -out "$OUT_DIR/server.crt"

# --- Client certificate (for destinations requiring mTLS) ---------------
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$OUT_DIR/client.key"

openssl req -new -key "$OUT_DIR/client.key" \
  -subj "/CN=dicom-router dev client" \
  -out "$OUT_DIR/client.csr"

cat >"$OUT_DIR/client.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF

openssl x509 -req -in "$OUT_DIR/client.csr" \
  -CA "$OUT_DIR/ca.crt" -CAkey "$OUT_DIR/ca.key" -CAcreateserial \
  -days "$DAYS" -sha256 -extfile "$OUT_DIR/client.ext" \
  -out "$OUT_DIR/client.crt"

# --- Cleanup intermediates ------------------------------------------------
rm -f "$OUT_DIR/server.csr" "$OUT_DIR/client.csr" \
  "$OUT_DIR/server.ext" "$OUT_DIR/client.ext" "$OUT_DIR/ca.srl"

chmod 600 "$OUT_DIR/ca.key" "$OUT_DIR/server.key" "$OUT_DIR/client.key"

echo "Done. Files written to $OUT_DIR:" >&2
ls -1 "$OUT_DIR"
