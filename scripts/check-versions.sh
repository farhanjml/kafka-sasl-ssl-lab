#!/bin/bash
###############################################################################
# check-versions.sh — Report Confluent Platform + Apache Kafka versions
# CP 8.x versioning: Apache Kafka version = CP major - 4 (e.g. CP 8.2 = Kafka 4.2)
###############################################################################
set -euo pipefail

BROKERS=("kafka1" "kafka2" "kafka3")

# Derive Apache Kafka version from Confluent Platform version string
# CP 8.2.0 -> 4.2.0
cp_to_kafka_version() {
  local cp_ver="$1"
  local major minor patch
  major=$(echo "$cp_ver" | cut -d. -f1)
  minor=$(echo "$cp_ver" | cut -d. -f2)
  patch=$(echo "$cp_ver" | cut -d. -f3 | sed 's/-ccs//')
  echo "$((major - 4)).$minor.$patch"
}

echo "=== Kafka SSL Lab v8 — Version Check ==="
echo ""

for BROKER in "${BROKERS[@]}"; do
  IMAGE=$(docker inspect "$BROKER" --format '{{.Config.Image}}' 2>/dev/null)
  CP_VER=$(echo "$IMAGE" | cut -d: -f2)
  KAFKA_VER=$(cp_to_kafka_version "$CP_VER")
  echo "  $BROKER"
  echo "    Confluent Platform : $CP_VER"
  echo "    Apache Kafka       : $KAFKA_VER"
done

echo ""
echo "=== Done ==="
