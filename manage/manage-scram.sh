#!/bin/bash
###############################################################################
# manage-scram.sh — Interactive SCRAM-SHA-256 user management
#
# Operations: list, create, update password, delete
#
# SCRAM credentials are stored in the Kafka metadata log (KRaft).
# Re-run create-scram-users.sh if the kafka1-data volume is ever wiped.
###############################################################################
set -uo pipefail

KAFKA_CONTAINER="kafka1"
BOOTSTRAP="kafka1:19093"
CONFIG_FILE="/etc/kafka/client-internal.properties"
ITERATIONS=8192

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
  local var_name="$1" label="$2" value=""
  while [[ -z "$value" ]]; do read -rp "$label: " value; done
  printf -v "$var_name" '%s' "$value"
}

prompt_password() {
  local var_name="$1" label="${2:-Password}" p1="" p2=""
  while true; do
    read -rsp "$label: " p1; echo ""
    read -rsp "  Confirm password: " p2; echo ""
    [[ "$p1" == "$p2" ]] && break
    echo "  Passwords do not match. Try again."
  done
  printf -v "$var_name" '%s' "$p1"
}

pause() { echo ""; read -rp "  Press Enter to return to menu..."; }

hr() { printf '%s' "$(printf '%*s' "$1" | tr ' ' '-')"; }

# ─── Operations ───────────────────────────────────────────────────────────────

op_list() {
  echo ""
  echo "  Fetching users..."
  echo ""

  local raw
  raw=$(run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --describe --entity-type users 2>/dev/null \
    | grep "^SCRAM") || true

  if [[ -z "$raw" ]]; then
    echo "  No SCRAM users found."
    return
  fi

  local w1=25 w2=14
  printf "  %-${w1}s  %-${w2}s  %s\n" "USERNAME" "MECHANISM" "ITERATIONS"
  printf "  %-${w1}s  %-${w2}s  %s\n" "$(hr $w1)" "$(hr $w2)" "$(hr 10)"

  # Input:  SCRAM credential configs for user-principal 'admin' are SCRAM-SHA-256=iterations=8192
  # After sed: admin|SCRAM-SHA-256|8192
  echo "$raw" \
    | sed "s/SCRAM credential configs for user-principal '//;  s/' are /|/; s/=iterations=/|/" \
    | awk -F'|' -v w1="$w1" -v w2="$w2" \
        '{ printf "  %-*s  %-*s  %s\n", w1, $1, w2, $2, $3 }'
  echo ""
}

op_create() {
  echo ""
  echo "  --- Create SCRAM User ---"
  prompt        USERNAME "  Username"
  prompt_password PASSWORD "  Password"

  echo ""
  echo "  Creating user: $USERNAME"
  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --alter \
    --add-config "SCRAM-SHA-256=[iterations=${ITERATIONS},password=${PASSWORD}]" \
    --entity-type users --entity-name "$USERNAME"
  echo "  Done."
  echo ""
  echo "  Client JAAS config:"
  echo "    sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\"
  echo "      username=\"${USERNAME}\" \\"
  echo "      password=\"${PASSWORD}\";"
  echo ""
}

op_update_password() {
  echo ""
  echo "  --- Update Password ---"
  prompt        USERNAME "  Username"
  prompt_password PASSWORD "  New password"

  echo ""
  echo "  Updating password for: $USERNAME"
  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --alter \
    --add-config "SCRAM-SHA-256=[iterations=${ITERATIONS},password=${PASSWORD}]" \
    --entity-type users --entity-name "$USERNAME"
  echo "  Done."
  echo ""
}

op_delete() {
  echo ""
  echo "  --- Delete SCRAM User ---"
  echo "  WARNING: Removes credentials permanently."
  echo ""
  prompt USERNAME "  Username to delete"

  echo ""
  read -rp "  Type the username again to confirm: " CONFIRM
  if [[ "$CONFIRM" != "$USERNAME" ]]; then
    echo "  Names do not match — aborting."
    return
  fi

  run_kafka kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CONFIG_FILE" \
    --alter \
    --delete-config "SCRAM-SHA-256" \
    --entity-type users --entity-name "$USERNAME"
  echo "  Done. User '$USERNAME' deleted."
  echo ""
}

show_menu() {
  echo ""
  echo "  ╔══════════════════════════════════════╗"
  echo "  ║       Kafka SCRAM User Manager       ║"
  echo "  ║  Broker: $BOOTSTRAP  ║"
  echo "  ╚══════════════════════════════════════╝"
  echo ""
  echo "  1) List users"
  echo "  2) Create user"
  echo "  3) Update password"
  echo "  4) Delete user"
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
    2) op_create;          pause ;;
    3) op_update_password; pause ;;
    4) op_delete;          pause ;;
    0|q|Q) echo "  Goodbye."; exit 0 ;;
    *) echo "  Invalid choice '$CHOICE'."; pause ;;
  esac
done
