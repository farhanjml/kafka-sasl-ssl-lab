#!/bin/bash
###############################################################################
# create-scram-users.sh — Create SCRAM-SHA-256 users in Kafka metadata log
#
# Run AFTER brokers are up and healthy.
# SCRAM credentials are stored in the Kafka metadata log (KRaft), not in a
# file — re-run this script if the kafka1-data volume is ever wiped.
###############################################################################
set -euo pipefail

KAFKA_CONTAINER="kafka1"
BOOTSTRAP="kafka1:19093"
CONFIG_FILE="/etc/kafka/client-internal.properties"

echo "=== Creating SCRAM-SHA-256 Users ==="
echo ""

# Wait for kafka1 to be ready
echo ">>> Waiting for kafka1 to be ready..."
for i in $(seq 1 30); do
  if docker exec $KAFKA_CONTAINER kafka-broker-api-versions \
    --bootstrap-server $BOOTSTRAP \
    --command-config $CONFIG_FILE &>/dev/null; then
    echo "    Broker ready."
    break
  fi
  echo "    Attempt $i/30 — waiting 5s..."
  sleep 5
done

create_user() {
  local USER="$1"
  local PASS="$2"
  echo ""
  echo ">>> Creating user: $USER"
  docker exec $KAFKA_CONTAINER kafka-configs \
    --bootstrap-server $BOOTSTRAP \
    --command-config $CONFIG_FILE \
    --alter \
    --add-config "SCRAM-SHA-256=[iterations=8192,password=$PASS]" \
    --entity-type users \
    --entity-name "$USER"
  docker exec $KAFKA_CONTAINER kafka-configs \
    --bootstrap-server $BOOTSTRAP \
    --command-config $CONFIG_FILE \
    --alter \
    --add-config "SCRAM-SHA-512=[iterations=8192,password=$PASS]" \
    --entity-type users \
    --entity-name "$USER"
  echo "    Done: $USER"
}

create_user "admin"       "<ADMIN_SCRAM_PASSWORD>"
create_user "app-client" "<APP_SCRAM_PASSWORD>"
create_user "demo-user"   "<DEMO_SCRAM_PASSWORD>"

echo ""
echo "=== SCRAM user creation complete ==="
echo ""
echo "Users created:"
echo "  admin       / <ADMIN_SCRAM_PASSWORD>   (cluster admin)"
echo "  app-client / <APP_SCRAM_PASSWORD>    (External CDC client)"
echo "  demo-user   / <DEMO_SCRAM_PASSWORD>    (general testing)"
echo ""
echo "Verify with:"
echo "  docker exec $KAFKA_CONTAINER kafka-configs \\"
echo "    --bootstrap-server $BOOTSTRAP \\"
echo "    --command-config $CONFIG_FILE \\"
echo "    --describe --entity-type users"
echo ""
echo "Next: ./create-topics.sh"
