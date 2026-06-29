#!/usr/bin/env bash
# Generate a throwaway TLS trust chain for verifying kacrab's SSL / SASL_SSL /
# mTLS client paths against a local Kafka broker (docker-compose.tls.yml).
#
# Outputs into $1 (default /tmp/kacrab_tls):
#   ca.crt              CA cert (PEM) -- kacrab truststore (ssl.truststore.certificates)
#   server.p12          broker keystore (server key + cert), SAN localhost+127.0.0.1
#   truststore.p12      broker truststore (CA only) -- to verify client certs (mTLS)
#   client.crt/.key     client cert + key (PEM) -- kacrab keystore for mTLS
#   *.password          fixed passwords below, echoed for reference
#
# Passwords are intentionally trivial; this material is for local tests only.
set -euo pipefail

OUT="${1:-/tmp/kacrab_tls}"
STORE_PASS="kacrab-store"
mkdir -p "$OUT"
cd "$OUT"

# CA
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt \
  -subj "/CN=kacrab-test-ca" -days 3650 >/dev/null 2>&1

# Server key + CSR + cert (signed by CA) with SAN for localhost and 127.0.0.1.
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1') >/dev/null 2>&1

# Client key + CSR + cert (signed by CA) for mTLS.
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "/CN=kacrab-client" >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 3650 >/dev/null 2>&1

# Broker keystore (server identity) and truststore (CA) as PKCS12.
openssl pkcs12 -export -name kafka -in server.crt -inkey server.key -certfile ca.crt \
  -out server.p12 -passout "pass:${STORE_PASS}" >/dev/null 2>&1
openssl pkcs12 -export -nokeys -name ca -in ca.crt \
  -out truststore.p12 -passout "pass:${STORE_PASS}" >/dev/null 2>&1

chmod 644 ./*.p12 ./*.crt ./*.key
printf '%s' "$STORE_PASS" > store.password
echo "TLS material written to $OUT (store password: ${STORE_PASS})"
ls -1 "$OUT"
