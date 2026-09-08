#!/bin/bash
###############################################################################
# create-topics.sh — Create Kafka topics for CDC pipeline + BankLab
# Run AFTER SCRAM users are created
#
# Also pre-creates __transaction_state so transactional producers
# can initialize without timing out while the coordinator auto-creates it.
###############################################################################
set -euo pipefail

KAFKA_CONTAINER="kafka1"
BOOTSTRAP="kafka1:19093"
CONFIG_FILE="/etc/kafka/client-internal.properties"

echo "=== Creating Kafka Topics ==="
echo ""

create_topic() {
  local NAME="$1"
  local PARTITIONS="$2"
  local REPLICATION="$3"

  echo ">>> Creating topic: $NAME (partitions=$PARTITIONS, replication=$REPLICATION)"
  docker exec $KAFKA_CONTAINER kafka-topics \
    --bootstrap-server $BOOTSTRAP \
    --command-config $CONFIG_FILE \
    --create \
    --topic "$NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --if-not-exists
  echo "    Done."
}

# CDC topics
create_topic "cdc-transactions" 6 2
create_topic "cdc-accounts"     3 2
create_topic "cdc-customers"    3 2

# BankLab event topic
create_topic "banklab-events"        3 2

# General demo/test topic
create_topic "demo-general"          1 1

# ─── Pre-create __transaction_state ──────────────────────────────────────────
# auto.create.topics.enable=false means this internal topic is never auto-
# created. Without it, the Transaction Coordinator must create it on-the-fly
# during the first initTransactions() call, which can exceed a strict client
# timeout and produce RC=129 "Timed out waiting for operation to finish".
echo ""
echo ">>> Pre-creating __transaction_state (required for transactional CDC producer)..."
docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server $BOOTSTRAP \
  --command-config $CONFIG_FILE \
  --create \
  --topic __transaction_state \
  --partitions 50 \
  --replication-factor 2 \
  --config cleanup.policy=compact \
  --config compression.type=producer \
  --config segment.bytes=104857600 \
  --config min.cleanable.dirty.ratio=0.005 \
  --if-not-exists
echo "    Done."

echo ""
echo "=== Topic creation complete ==="
echo ""
echo "Verifying..."
docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server $BOOTSTRAP \
  --command-config $CONFIG_FILE \
  --list

echo ""
echo "=== Done ==="
