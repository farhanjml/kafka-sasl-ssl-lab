#!/bin/bash
###############################################################################
# quick-start.sh — Full deployment for kafka-sasl-ssl-lab
#
# Run from this repo's root directory.
# Safe to re-run on an existing deployment — each step is idempotent.
#
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if grep -q '<KAFKA_HOST_IP>' "$SCRIPT_DIR/docker-compose.yml" 2>/dev/null; then
  echo "❌ docker-compose.yml still has placeholder values."
  echo "   Run ./scripts/configure.sh first, then run this script again."
  exit 1
fi

echo "============================================"
echo "  kafka-sasl-ssl-v8 — Quick Start"
echo "  Confluent Platform 8.2.0 / Kafka 4.2"
echo "  Host: <KAFKA_HOST_IP>"
echo "  Brokers SASL_SSL : 9193 / 9194 / 9195"
echo "  Brokers SSL      : 9197 / 9198 / 9199"
echo "  Schema Registry  : 8081"
echo "  ksqlDB           : 8088"
echo "  Kafka Connect    : 8083"
echo "  KDC              : 18088"
echo "============================================"
echo ""

# ─── Step 1: Generate SSL certificates ──────────────────────────────────────
echo "=== STEP 1/6: Generate SSL certificates ==="
chmod +x "$SCRIPT_DIR/scripts/"*.sh
bash "$SCRIPT_DIR/scripts/generate-certs.sh"
echo ""

# ─── Step 2: Start containers ────────────────────────────────────────────────
echo "=== STEP 2/6: Start Docker Compose ==="
docker compose up -d
echo ""

# ─── Step 3: Setup Kerberos ──────────────────────────────────────────────────
# setup-kerberos.sh handles:
#   - Fixing the kdc.conf (EXAMPLE.COM → KAFKA.LOCAL)
#   - Rebuilding the KDC database with a known master key
#   - Restarting the KDC
#   - Creating principals and exporting fresh keytabs
#   - Copying kafkaclient.keytab to certs/ for distribution to CDC client host
echo "=== STEP 3/6: Setup Kerberos ==="
bash "$SCRIPT_DIR/scripts/setup-kerberos.sh"
echo ""

# ─── Step 4: Restart brokers to pick up new keytabs ─────────────────────────
echo "=== STEP 4/6: Restart brokers (keytab pickup) ==="
docker compose restart kafka1 kafka2 kafka3
echo "Waiting 20s for brokers to stabilise..."
sleep 20
echo ""

# ─── Step 5: Create SCRAM users ──────────────────────────────────────────────
echo "=== STEP 5/6: Create SCRAM users ==="
bash "$SCRIPT_DIR/scripts/create-scram-users.sh"
echo ""

# ─── Step 6: Create topics (including __transaction_state) ───────────────────
echo "=== STEP 6/6: Create topics ==="
bash "$SCRIPT_DIR/scripts/create-topics.sh"
echo ""

# ─── Done ────────────────────────────────────────────────────────────────────
echo "============================================"
echo "  kafka-sasl-ssl-v8 is READY"
echo ""
echo "  Confluent Platform: 8.2.0"
echo "  Apache Kafka:       4.2"
echo "  Cluster ID:         v9QkkNkaQfCyup16RviRyQ"
echo ""
echo "  Kafka SASL_SSL (existing clients):"
echo "    kafka1: <KAFKA_HOST_IP>:9193  (kafka1:9093 internal)"
echo "    kafka2: <KAFKA_HOST_IP>:9194"
echo "    kafka3: <KAFKA_HOST_IP>:9195"
echo ""
echo "  Kafka SSL (pure SSL, no SASL):"
echo "    kafka1: <KAFKA_HOST_IP>:9197"
echo "    kafka2: <KAFKA_HOST_IP>:9198"
echo "    kafka3: <KAFKA_HOST_IP>:9199"
echo ""
echo "  Confluent services:"
echo "    Schema Registry: https://<KAFKA_HOST_IP>:8081"
echo "    ksqlDB:          https://<KAFKA_HOST_IP>:8088"
echo "    Kafka Connect:   https://<KAFKA_HOST_IP>:8083"
echo ""
echo "  SCRAM users:"
echo "    admin       / <ADMIN_SCRAM_PASSWORD>"
echo "    app-client / <APP_SCRAM_PASSWORD>"
echo "    demo-user   / <DEMO_SCRAM_PASSWORD>"
echo ""
echo "  Kerberos:"
echo "    KDC:       kdc (<KAFKA_HOST_IP>:18088)"
echo "    Realm:     KAFKA.LOCAL"
echo "    Principal: kafkaclient@KAFKA.LOCAL"
echo "    Keytab:    certs/keytabs/kafkaclient.keytab"
echo ""
echo "  SSL-only client cert package (certs/tmon/ — for SSL-only consumers):"
echo "    certs/tmon/tmon-client.crt"
echo "    certs/tmon/tmon-client.key"
echo "    certs/tmon/tmon-client.keystore.p12"
echo "    certs/tmon/kafka.truststore.p12"
echo "    Password: <SSL_KEYSTORE_PASSWORD>"
echo ""
echo "  Quick cluster test:"
echo "    docker exec kafka1 kafka-topics \\"
echo "      --bootstrap-server kafka1:19093 \\"
echo "      --command-config /etc/kafka/client-internal.properties \\"
echo "      --list"
echo "============================================"

