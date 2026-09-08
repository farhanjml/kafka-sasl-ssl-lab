#!/bin/bash
###############################################################################
# manage-topic.sh — Interactive Kafka topic management
#
# Operations: list, describe, create, add partitions, alter config, delete
###############################################################################
set -uo pipefail

KAFKA_CONTAINER="kafka1"
BOOTSTRAP="kafka1:19093"
CONFIG_FILE="/etc/kafka/client-internal.properties"

# ─── Helpers ──────────────────────────────────────────────────────────────────

run_kafka() { docker exec "$KAFKA_CONTAINER" "$@"; }

check_broker() {
  if ! docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CONFIG_FILE" &>/dev/null; then
    echo "ERROR: Cannot reach broker. Is the cluster running?"
    echo "  docker compose up -d"
    exit 1
  fi
}

prompt() {
  local var_name="$1" label="$2" default="${3:-}" value=""
  if [[ -n "$default" ]]; then
    read -rp "$label [$default]: " value
    value="${value:-$default}"
  else
    while [[ -z "$value" ]]; do read -rp "$label: " value; done
  fi
  printf -v "$var_name" '%s' "$value"
}

pause() { echo ""; read -rp "  Press Enter to return to menu..."; }

hr() { printf '%s' "$(printf '%*s' "$1" | tr ' ' '-')"; }

# ─── Operations ───────────────────────────────────────────────────────────────

op_list() {
  echo ""
  echo "  Fetching topics..."
  echo ""

  local raw
  raw=$(run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --exclude-internal 2>/dev/null \
    | grep -v "^[[:space:]]" | grep "^Topic:") || true

  if [[ -z "$raw" ]]; then
    echo "  No topics found."
    return
  fi

  local w1=38 w2=10 w3=12
  printf "  %-${w1}s  %-${w2}s  %-${w3}s  %s\n" "TOPIC" "PARTITIONS" "REPLICATION" "CUSTOM CONFIGS"
  printf "  %-${w1}s  %-${w2}s  %-${w3}s  %s\n" "$(hr $w1)" "$(hr $w2)" "$(hr $w3)" "$(hr 30)"

  echo "$raw" | awk -F'\t' -v w1="$w1" -v w2="$w2" -v w3="$w3" '{
    name=""; parts=""; rep=""; cfgs="-"
    for (i=1; i<=NF; i++) {
      if ($i ~ /^Topic: /)             name = substr($i, 8)
      if ($i ~ /^PartitionCount: /)    parts = substr($i, 16)
      if ($i ~ /^ReplicationFactor: /) rep   = substr($i, 19)
      if ($i ~ /^Configs: /)           { cfgs = substr($i, 9); if (cfgs == "") cfgs = "-" }
    }
    if (name != "") printf "  %-*s  %-*s  %-*s  %s\n", w1, name, w2, parts, w3, rep, cfgs
  }'
  echo ""
}

op_describe() {
  echo ""
  prompt TOPIC "Topic name"
  echo ""
  run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --topic "$TOPIC"
  echo ""
  echo "  Custom configs:"
  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --entity-type topics --entity-name "$TOPIC" 2>/dev/null \
    | grep -v "^$" | sed 's/^/  /' || echo "  (none)"
  echo ""
}

op_create() {
  echo ""
  echo "  --- Create Topic ---"
  echo ""
  echo "  Type:"
  echo "    1) Standard  — distributed across all brokers (SASL_SSL)"
  echo "    2) PLAINTEXT — pinned to broker 1 only (port 9092, no auth)"
  read -rp "  Choice [1/2, default 1]: " TYPE_CHOICE
  TYPE_CHOICE="${TYPE_CHOICE:-1}"

  echo ""
  prompt TOPIC "  Topic name"

  if [[ "$TYPE_CHOICE" == "2" ]]; then
    echo ""
    echo "  Creating PLAINTEXT topic '$TOPIC' pinned to broker 1..."
    run_kafka kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CONFIG_FILE" \
      --create --topic "$TOPIC" \
      --replica-assignment 1 \
      --if-not-exists
  else
    prompt PARTITIONS  "  Partitions" "3"
    prompt REPLICATION "  Replication factor" "2"
    echo ""
    echo "  Creating: $TOPIC  (partitions=$PARTITIONS, replication=$REPLICATION)"
    run_kafka kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$CONFIG_FILE" \
      --create --topic "$TOPIC" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" \
      --if-not-exists
  fi

  echo "  Done."
  echo ""
  run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --topic "$TOPIC"
  echo ""
}

op_add_partitions() {
  echo ""
  echo "  --- Add Partitions ---"
  echo "  WARNING: Increasing partition count is IRREVERSIBLE."
  echo ""
  prompt TOPIC "  Topic name"

  echo ""
  echo "  Current state:"
  run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --topic "$TOPIC" 2>&1 \
    | grep -E "PartitionCount|Topic:" | sed 's/^/    /'

  echo ""
  prompt NEW_PARTITIONS "  New total partition count (must be greater than current)"

  run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --alter --topic "$TOPIC" --partitions "$NEW_PARTITIONS"
  echo "  Done."
  echo ""
}

op_alter_config() {
  echo ""
  echo "  --- Alter Topic Config ---"
  echo "  Common config keys:"
  echo "    retention.ms        e.g. 604800000   (7 days)"
  echo "    retention.bytes     e.g. 1073741824  (1 GB per partition)"
  echo "    cleanup.policy      delete | compact | delete,compact"
  echo "    max.message.bytes   e.g. 1048576     (1 MB)"
  echo "    min.insync.replicas e.g. 2"
  echo ""
  prompt TOPIC      "  Topic name"
  prompt CONFIG_KEY "  Config key"
  prompt CONFIG_VAL "  Config value"

  echo ""
  echo "  Setting $CONFIG_KEY=$CONFIG_VAL on $TOPIC..."
  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --alter --entity-type topics --entity-name "$TOPIC" \
    --add-config "${CONFIG_KEY}=${CONFIG_VAL}"
  echo "  Done."

  echo ""
  echo "  Current custom configs for $TOPIC:"
  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --entity-type topics --entity-name "$TOPIC" 2>/dev/null \
    | grep -v "^$" | sed 's/^/    /' || echo "    (none)"
  echo ""
}

op_consume() {
  echo ""
  echo "  --- Consume Messages ---"
  echo ""

  op_list

  prompt TOPIC "  Topic name"

  echo ""
  echo "  Listener:"
  echo "    1) Internal / PLAINTEXT  — no auth, admin use"
  echo "    2) External / SASL_SSL   — SCRAM-SHA-256 + mTLS (same as app clients)"
  read -rp "  Choice [1/2, default 1]: " LISTENER_CHOICE
  LISTENER_CHOICE="${LISTENER_CHOICE:-1}"

  echo ""
  echo "  Start offset:"
  echo "    1) Beginning  — read all existing messages then wait"
  echo "    2) Latest     — show only new messages from now on"
  read -rp "  Choice [1/2, default 1]: " OFF_CHOICE
  OFF_CHOICE="${OFF_CHOICE:-1}"

  echo ""
  read -rp "  Max messages [20, or 0 to stream until Ctrl+C]: " MAX_MSG
  MAX_MSG="${MAX_MSG:-20}"

  echo ""
  local PRETTY="n"
  if command -v jq &>/dev/null; then
    read -rp "  Pretty-print JSON with jq? [Y/n]: " PRETTY
    PRETTY="${PRETTY:-y}"
  fi

  echo ""
  [[ "$LISTENER_CHOICE" == "2" ]] && echo "  Listener: SASL_SSL (external)" \
                                  || echo "  Listener: PLAINTEXT (internal)"
  echo "  Topic   : $TOPIC"
  [[ "$OFF_CHOICE" == "2" ]] && echo "  Offset  : latest" || echo "  Offset  : beginning"
  [[ "$MAX_MSG"    == "0" ]] && echo "  Mode    : streaming — press Ctrl+C to stop" \
                             || echo "  Limit   : $MAX_MSG messages"
  echo ""
  printf "  %s\n" "$(hr 60)"

  if [[ "$LISTENER_CHOICE" == "2" ]]; then
    # SASL_SSL — write a temp config file inside the container (avoids --command-property quoting issues)
    local TRUSTSTORE="/tmp/kafka.truststore.p12"
    local KEYSTORE="/tmp/client.keystore.p12"
    local CFGTMP="/tmp/consume-sasl.properties"

    if ! docker exec "$KAFKA_CONTAINER" test -f "$KEYSTORE" 2>/dev/null; then
      echo "  Copying client certs into container..."
      docker cp "$(pwd)/certs/client/client.keystore.p12" "$KAFKA_CONTAINER:$KEYSTORE"
      docker cp "$(pwd)/certs/client/kafka.truststore.p12" "$KAFKA_CONTAINER:$TRUSTSTORE"
      echo "  Done."
    fi

    docker exec "$KAFKA_CONTAINER" bash -c "cat > $CFGTMP << 'EOPROPS'
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"demo-user\" password=\"<DEMO_SCRAM_PASSWORD>\";
ssl.truststore.location=$TRUSTSTORE
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=$KEYSTORE
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
EOPROPS"

    local BOOTSTRAP_CONSUME="kafka1:9093"
    local CONFIG_CONSUME="$CFGTMP"
  else
    local BOOTSTRAP_CONSUME="$BOOTSTRAP"
    local CONFIG_CONSUME="$CONFIG_FILE"
  fi

  local ARGS=(
    docker exec -i "$KAFKA_CONTAINER"
    kafka-console-consumer
    --bootstrap-server "$BOOTSTRAP_CONSUME"
    --command-config "$CONFIG_CONSUME"
    --topic "$TOPIC"
    --formatter-property print.timestamp=true
    --formatter-property print.partition=true
    --formatter-property print.offset=true
    --formatter-property print.key=true
    --formatter-property key.separator=" | "
  )

  [[ "$OFF_CHOICE" != "2" ]] && ARGS+=(--from-beginning)
  [[ "$MAX_MSG"    != "0" ]] && ARGS+=(--max-messages "$MAX_MSG")

  if [[ "${PRETTY,,}" == "y" ]]; then
    "${ARGS[@]}" 2>/dev/null \
      | grep --line-buffered -v "^Option \|^\[" \
      | while IFS= read -r line; do
          if [[ "$line" =~ ^CreateTime ]]; then
            # Format: CreateTime:<ts> | Partition:<N> | Offset:<N> | <key> | <json>
            meta="${line% | *}"   # everything before the last ' | '
            value="${line##* | }" # everything after the last ' | '
            echo ""
            echo "  --- $meta ---"
            if [[ "$value" == "{"* || "$value" == "["* ]]; then
              echo "$value" | jq .
            else
              echo "  $value"
            fi
          elif [[ "$line" == "{"* || "$line" == "["* ]]; then
            echo "$line" | jq .
          elif [[ -n "$line" ]]; then
            echo "  $line"
          fi
        done || true
  else
    echo "  Format: CreateTime:<ts> | Partition:<N> | Offset:<N> | <key> | <value>"
    echo ""
    "${ARGS[@]}" 2>/dev/null | grep --line-buffered -v "^Option \|^\[" || true
  fi

  echo ""
  printf "  %s\n" "$(hr 60)"
  echo "  Consumer stopped."
  echo ""
}

op_delete() {
  echo ""
  echo "  --- Delete Topic ---"
  echo "  WARNING: Permanent and cannot be undone."
  echo ""
  prompt TOPIC "  Topic name"

  echo ""
  read -rp "  Type the topic name again to confirm: " CONFIRM
  if [[ "$CONFIRM" != "$TOPIC" ]]; then
    echo "  Names do not match — aborting."
    return
  fi

  run_kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --delete --topic "$TOPIC"
  echo "  Topic '$TOPIC' deleted."
  echo ""
}

show_menu() {
  echo ""
  echo "  ╔══════════════════════════════════════╗"
  echo "  ║        Kafka Topic Manager           ║"
  echo "  ║  Broker: $BOOTSTRAP  ║"
  echo "  ╚══════════════════════════════════════╝"
  echo ""
  echo "  1) List topics"
  echo "  2) Describe topic"
  echo "  3) Create topic"
  echo "  4) Add partitions"
  echo "  5) Alter topic config"
  echo "  6) Delete topic"
  echo "  7) Consume messages"
  echo "  0) Exit"
  echo ""
}

# ─── Main loop ────────────────────────────────────────────────────────────────

check_broker

while true; do
  show_menu
  read -rp "  Choice: " CHOICE
  echo ""
  case "$CHOICE" in
    1) op_list;            pause ;;
    2) op_describe;        pause ;;
    3) op_create;          pause ;;
    4) op_add_partitions;  pause ;;
    5) op_alter_config;    pause ;;
    6) op_delete;          pause ;;
    7) op_consume;         pause ;;
    0|q|Q) echo "  Goodbye."; exit 0 ;;
    *) echo "  Invalid choice '$CHOICE'."; pause ;;
  esac
done
