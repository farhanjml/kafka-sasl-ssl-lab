#!/bin/bash
###############################################################################
# generate-certs.sh — CA + broker + service certs (PEM) + PKCS12 keystores
# Run ONCE before first docker compose up
# Output: ../certs/
#
# Generates certs for: brokers, client, schema-registry, ksqldb, connect, ssl-only-client
# All signed by the same CA. Password: <SSL_KEYSTORE_PASSWORD>
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../certs"
BROKER_DIR="$CERT_DIR/broker"
CA_DIR="$CERT_DIR/ca"
CLIENT_DIR="$CERT_DIR/client"
SR_DIR="$CERT_DIR/schema-registry"
KSQL_DIR="$CERT_DIR/ksqldb"
CONNECT_DIR="$CERT_DIR/connect"
TMON_DIR="$CERT_DIR/tmon"
# NOTE: certs/keytabs/ is intentionally NOT wiped — keytabs are managed by setup-kerberos.sh
KEYTAB_DIR="$CERT_DIR/keytabs"

PASSWORD="<SSL_KEYSTORE_PASSWORD>"
VALIDITY=3650  # 10 years
CA_CN="Kafka-SSL-Lab-CA"

BROKERS=("kafka1" "kafka2" "kafka3")
BROKER_HOSTS=("kafka1" "kafka2" "kafka3")
HOST_IP="<KAFKA_HOST_IP>"

echo "=== kafka-sasl-ssl-v8 — Certificate Generation ==="
echo "Output dir: $CERT_DIR"

# Wipe only the cert subdirs, never keytabs/
rm -rf "$BROKER_DIR" "$CA_DIR" "$CLIENT_DIR" "$SR_DIR" "$KSQL_DIR" "$CONNECT_DIR" "$TMON_DIR"
mkdir -p "$BROKER_DIR" "$CA_DIR" "$CLIENT_DIR" "$SR_DIR" "$KSQL_DIR" "$CONNECT_DIR" "$TMON_DIR" "$KEYTAB_DIR"

# ─── 1. Generate CA ─────────────────────────────────────────────────────
echo ""
echo ">>> Generating CA key + certificate..."
openssl genrsa -out "$CA_DIR/ca-key.pem" 4096
chmod 600 "$CA_DIR/ca-key.pem"  # CA private key — root-only

openssl req -new -x509 \
  -key "$CA_DIR/ca-key.pem" \
  -out "$CA_DIR/ca-cert.crt" \
  -days $VALIDITY \
  -subj "/CN=$CA_CN/O=KafkaSSLLab/L=PutraHeights/ST=Selangor/C=MY"

echo "    CA cert: $CA_DIR/ca-cert.crt"

# ─── 2. Generate broker certs + PKCS12 keystores ────────────────────────
for i in "${!BROKERS[@]}"; do
  BROKER="${BROKERS[$i]}"
  FQDN="${BROKER_HOSTS[$i]}"
  KEY="$BROKER_DIR/${BROKER}.key"
  CSR="$BROKER_DIR/${BROKER}.csr"
  CERT="$BROKER_DIR/${BROKER}.crt"
  P12="$BROKER_DIR/${BROKER}.keystore.p12"
  EXT_FILE="$BROKER_DIR/${BROKER}.ext"

  echo ""
  echo ">>> Generating cert for $BROKER ($FQDN)..."

  # Create SAN extension file
  cat > "$EXT_FILE" <<EOF
[v3_ext]
subjectAltName = DNS:$FQDN,DNS:localhost,IP:$HOST_IP,IP:127.0.0.1
extendedKeyUsage = serverAuth,clientAuth
basicConstraints = CA:FALSE
EOF

  # Generate private key
  openssl genrsa -out "$KEY" 2048

  # Generate CSR
  openssl req -new \
    -key "$KEY" \
    -out "$CSR" \
    -subj "/CN=$FQDN/O=KafkaSSLLab/L=PutraHeights/ST=Selangor/C=MY"

  # Sign with CA
  openssl x509 -req -in "$CSR" \
    -CA "$CA_DIR/ca-cert.crt" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
    -out "$CERT" \
    -days $VALIDITY \
    -extfile "$EXT_FILE" -extensions v3_ext

  # Convert to PKCS12 keystore (what Confluent Docker image needs)
  openssl pkcs12 -export \
    -in "$CERT" \
    -inkey "$KEY" \
    -chain -CAfile "$CA_DIR/ca-cert.crt" \
    -name "$BROKER" \
    -out "$P12" \
    -password "pass:$PASSWORD"

  echo "    PEM cert:  $CERT"
  echo "    PEM key:   $KEY"
  echo "    Keystore:  $P12"

  # Cleanup intermediates
  rm -f "$CSR" "$EXT_FILE"
done

# ─── 3. Generate PKCS12 truststore (CA cert only) ───────────────────────
echo ""
echo ">>> Generating PKCS12 truststore..."
TRUSTSTORE="$BROKER_DIR/kafka.truststore.p12"

keytool -importcert -alias ca-root \
  -file "$CA_DIR/ca-cert.crt" \
  -keystore "$TRUSTSTORE" \
  -storetype PKCS12 \
  -storepass "$PASSWORD" \
  -noprompt

echo "    Truststore: $TRUSTSTORE"

# Copy truststore to client/ so it's ready to scp to client servers
cp "$TRUSTSTORE" "$CLIENT_DIR/kafka.truststore.p12"
echo "    Copied to:  $CLIENT_DIR/kafka.truststore.p12"

# ─── 4. Generate client cert + PKCS12 keystore ─────────────────────────
echo ""
echo ">>> Generating client cert..."
CLIENT_KEY="$CLIENT_DIR/client.key"
CLIENT_CSR="$CLIENT_DIR/client.csr"
CLIENT_CERT="$CLIENT_DIR/client.crt"
CLIENT_P12="$CLIENT_DIR/client.keystore.p12"
CLIENT_EXT="$CLIENT_DIR/client.ext"

cat > "$CLIENT_EXT" <<EOF
[v3_ext]
extendedKeyUsage = clientAuth
basicConstraints = CA:FALSE
EOF

openssl genrsa -out "$CLIENT_KEY" 2048

openssl req -new \
  -key "$CLIENT_KEY" \
  -out "$CLIENT_CSR" \
  -subj "/CN=kafka-client/O=KafkaSSLLab/L=PutraHeights/ST=Selangor/C=MY"

openssl x509 -req -in "$CLIENT_CSR" \
  -CA "$CA_DIR/ca-cert.crt" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
  -out "$CLIENT_CERT" \
  -days $VALIDITY \
  -extfile "$CLIENT_EXT" -extensions v3_ext

openssl pkcs12 -export \
  -in "$CLIENT_CERT" \
  -inkey "$CLIENT_KEY" \
  -chain -CAfile "$CA_DIR/ca-cert.crt" \
  -name "client" \
  -out "$CLIENT_P12" \
  -password "pass:$PASSWORD"

echo "    Client cert:     $CLIENT_CERT"
echo "    Client key:      $CLIENT_KEY"
echo "    Client keystore: $CLIENT_P12"

rm -f "$CLIENT_CSR" "$CLIENT_EXT"

# ─── 5. Generate service certs (Schema Registry, ksqlDB, Kafka Connect) ─
SERVICES=(
  "schema-registry:schema:$SR_DIR"
  "ksqldb:ksqldb:$KSQL_DIR"
  "connect:connect:$CONNECT_DIR"
)

for ENTRY in "${SERVICES[@]}"; do
  SVC_NAME="${ENTRY%%:*}"
  REST="${ENTRY#*:}"
  SVC_FQDN="${REST%%:*}"
  SVC_DIR="${REST#*:}"

  echo ""
  echo ">>> Generating cert for $SVC_NAME ($SVC_FQDN)..."

  SVC_KEY="$SVC_DIR/${SVC_NAME}.key"
  SVC_CSR="$SVC_DIR/${SVC_NAME}.csr"
  SVC_CERT="$SVC_DIR/${SVC_NAME}.crt"
  SVC_P12="$SVC_DIR/${SVC_NAME}.keystore.p12"
  SVC_EXT="$SVC_DIR/${SVC_NAME}.ext"

  cat > "$SVC_EXT" <<EOF
[v3_ext]
subjectAltName = DNS:$SVC_FQDN,DNS:localhost,IP:$HOST_IP,IP:127.0.0.1
extendedKeyUsage = serverAuth,clientAuth
basicConstraints = CA:FALSE
EOF

  openssl genrsa -out "$SVC_KEY" 2048

  openssl req -new \
    -key "$SVC_KEY" \
    -out "$SVC_CSR" \
    -subj "/CN=$SVC_FQDN/O=KafkaSSLLab/L=PutraHeights/ST=Selangor/C=MY"

  openssl x509 -req -in "$SVC_CSR" \
    -CA "$CA_DIR/ca-cert.crt" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
    -out "$SVC_CERT" \
    -days $VALIDITY \
    -extfile "$SVC_EXT" -extensions v3_ext

  openssl pkcs12 -export \
    -in "$SVC_CERT" \
    -inkey "$SVC_KEY" \
    -chain -CAfile "$CA_DIR/ca-cert.crt" \
    -name "$SVC_NAME" \
    -out "$SVC_P12" \
    -password "pass:$PASSWORD"

  # Copy shared truststore into service cert dir
  cp "$TRUSTSTORE" "$SVC_DIR/kafka.truststore.p12"

  echo "    Keystore:   $SVC_P12"
  echo "    Truststore: $SVC_DIR/kafka.truststore.p12"

  rm -f "$SVC_CSR" "$SVC_EXT"
done

# ─── 6. Generate SSL-only client cert ───────────────────────────────────
# For clients that cannot do SASL — connects via SSL listener (9197/9198/9199).
# Provide certs/tmon/ to the client team.
echo ""
echo ">>> Generating SSL-only client cert..."

TMON_KEY="$TMON_DIR/tmon-client.key"
TMON_CSR="$TMON_DIR/tmon-client.csr"
TMON_CERT="$TMON_DIR/tmon-client.crt"
TMON_P12="$TMON_DIR/tmon-client.keystore.p12"
TMON_EXT="$TMON_DIR/tmon-client.ext"

cat > "$TMON_EXT" <<EOF
[v3_ext]
extendedKeyUsage = clientAuth
basicConstraints = CA:FALSE
EOF

openssl genrsa -out "$TMON_KEY" 2048

openssl req -new \
  -key "$TMON_KEY" \
  -out "$TMON_CSR" \
  -subj "/CN=tmon-client/O=KafkaSSLLab/L=PutraHeights/ST=Selangor/C=MY"

openssl x509 -req -in "$TMON_CSR" \
  -CA "$CA_DIR/ca-cert.crt" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
  -out "$TMON_CERT" \
  -days $VALIDITY \
  -extfile "$TMON_EXT" -extensions v3_ext

openssl pkcs12 -export \
  -in "$TMON_CERT" \
  -inkey "$TMON_KEY" \
  -chain -CAfile "$CA_DIR/ca-cert.crt" \
  -name "tmon-client" \
  -out "$TMON_P12" \
  -password "pass:$PASSWORD"

cp "$TRUSTSTORE" "$TMON_DIR/kafka.truststore.p12"

echo "    TMON client cert:     $TMON_CERT"
echo "    TMON client key:      $TMON_KEY"
echo "    TMON client keystore: $TMON_P12"
echo "    TMON truststore:      $TMON_DIR/kafka.truststore.p12"

rm -f "$TMON_CSR" "$TMON_EXT"

# ─── 8. Create stub kafka_jaas.conf ─────────────────────────────────────
# Required by Confluent Docker's startup check (KAFKA_OPTS references this file).
# Per-listener JAAS is set via env vars in docker-compose.yml and takes precedence.
echo ""
echo ">>> Creating kafka_jaas.conf stub..."
cat > "$BROKER_DIR/kafka_jaas.conf" <<'EOF'
// Global JAAS stub — per-listener JAAS configs in docker-compose take precedence.
// Required so Confluent Docker's startup check passes when GSSAPI is enabled.
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="admin"
    password="<ADMIN_SCRAM_PASSWORD>";
};

KafkaClient {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="admin"
    password="<ADMIN_SCRAM_PASSWORD>";
};
EOF
echo "    Created: kafka_jaas.conf"

# ─── 9. Create credential files (Confluent Docker requirement) ──────────
echo ""
echo ">>> Creating credential files..."
echo "$PASSWORD" > "$BROKER_DIR/keystore_creds"
echo "$PASSWORD" > "$BROKER_DIR/truststore_creds"
echo "$PASSWORD" > "$BROKER_DIR/key_creds"

echo "    Created: keystore_creds, truststore_creds, key_creds"

# ─── 10. Cleanup serial file ────────────────────────────────────────────
rm -f "$CA_DIR/ca-cert.srl"

# ─── 11. Fix permissions so appuser inside containers can read files ─────
# CA private key stays 600 (root-only — it can sign any cert)
chmod 644 "$BROKER_DIR"/*.p12 "$BROKER_DIR"/*.key "$BROKER_DIR"/*.crt 2>/dev/null || true
chmod 644 "$CA_DIR"/*.crt 2>/dev/null || true
chmod 644 "$CLIENT_DIR"/*.p12 "$CLIENT_DIR"/*.key "$CLIENT_DIR"/*.crt 2>/dev/null || true
chmod 644 "$SR_DIR"/*.p12 "$SR_DIR"/*.key "$SR_DIR"/*.crt 2>/dev/null || true
chmod 644 "$KSQL_DIR"/*.p12 "$KSQL_DIR"/*.key "$KSQL_DIR"/*.crt 2>/dev/null || true
chmod 644 "$CONNECT_DIR"/*.p12 "$CONNECT_DIR"/*.key "$CONNECT_DIR"/*.crt 2>/dev/null || true
chmod 644 "$TMON_DIR"/*.p12 "$TMON_DIR"/*.key "$TMON_DIR"/*.crt 2>/dev/null || true

echo ""
echo "=== Certificate generation complete ==="
echo ""
echo "Cert layout:"
echo "  certs/ca/              — CA cert + private key (ca-key.pem is 600 root-only)"
echo "  certs/broker/          — kafka1/2/3 keystores, truststore, jaas stub, cred files"
echo "  certs/client/          — general client cert (external CDC client, lab testing)"
echo "  certs/schema-registry/ — Schema Registry keystore + truststore"
echo "  certs/ksqldb/          — ksqlDB keystore + truststore"
echo "  certs/connect/         — Kafka Connect keystore + truststore"
echo "  certs/tmon/            — TMON client cert + keystore + truststore (give to mainframe team)"
echo "  certs/keytabs/         — Kerberos keytabs (not touched by this script)"
echo ""
echo "SSL-only client cert handoff — give to client team:"
echo "  certs/tmon/tmon-client.crt        (PEM cert — for Schema Registry / ksqlDB)"
echo "  certs/tmon/tmon-client.key        (PEM key  — for Schema Registry / ksqlDB)"
echo "  certs/tmon/tmon-client.keystore.p12  (PKCS12 — for Kafka broker / Connect)"
echo "  certs/tmon/kafka.truststore.p12   (truststore — to verify all our services)"
echo "  Password for all: <SSL_KEYSTORE_PASSWORD>"
echo ""
echo "Next: docker compose up -d"
