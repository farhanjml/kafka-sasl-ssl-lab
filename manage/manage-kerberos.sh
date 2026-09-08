#!/bin/bash
###############################################################################
# manage-kerberos.sh — Interactive Kerberos principal and keytab management
#
# Operations: list, create principal + keytab, re-key, delete
#
# NOTE: Always rm -f keytab before ktadd — ktadd appends, stale kvno entries
# cause "Password incorrect" errors on kinit.
###############################################################################
set -uo pipefail

KDC_CONTAINER="kafka-kdc"
REALM="KAFKA.LOCAL"
KEYTAB_DIR="/etc/keytabs"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../certs/keytabs"

# ─── Helpers ──────────────────────────────────────────────────────────────────

run_kdc() { docker exec "$KDC_CONTAINER" "$@"; }

check_kdc() {
  if ! docker exec "$KDC_CONTAINER" kadmin.local -q "listprincs" &>/dev/null; then
    echo "ERROR: Cannot reach KDC container '$KDC_CONTAINER'."
    echo "  docker compose up -d"
    echo "  # If KDC was recreated, run: bash scripts/setup-kerberos.sh"
    exit 1
  fi
}

prompt() {
  local var_name="$1" label="$2" value=""
  while [[ -z "$value" ]]; do read -rp "$label: " value; done
  printf -v "$var_name" '%s' "$value"
}

pause() { echo ""; read -rp "  Press Enter to return to menu..."; }

hr() { printf '%s' "$(printf '%*s' "$1" | tr ' ' '-')"; }

# ─── Operations ───────────────────────────────────────────────────────────────

op_list() {
  echo ""
  echo "  Fetching principals..."
  echo ""

  local raw
  raw=$(run_kdc kadmin.local -q "listprincs" 2>/dev/null \
    | grep -v "^Authenticating" | grep -v "^$" | sort) || true

  if [[ -z "$raw" ]]; then
    echo "  No principals found."
    return
  fi

  local w1=42
  printf "  %-${w1}s  %s\n" "PRINCIPAL" "TYPE"
  printf "  %-${w1}s  %s\n" "$(hr $w1)" "$(hr 10)"

  echo "$raw" | awk -v w1="$w1" '{
    p = $0
    type = "client"
    if (p ~ /^kafka\//)                             type = "broker"
    if (p ~ /^K\/M@/ || p ~ /^krbtgt\// || \
        p ~ /^kadmin\// || p ~ /^kiprop\//)         type = "(internal)"
    printf "  %-*s  %s\n", w1, p, type
  }'
  echo ""
}

op_create() {
  echo ""
  echo "  --- Create New Client Principal + Keytab ---"
  echo "  Use this for a new CDC source, CDC client host, or customer application."
  echo ""
  echo "  Examples:"
  echo "    kafkaclient    →  kafkaclient@$REALM"
  echo "    mydb-client    →  mydb-client@$REALM"
  echo "    bankTXN        →  bankTXN@$REALM"
  echo ""
  prompt PRINCIPAL_SHORT "  Principal name (no @realm)"

  local PRINCIPAL="${PRINCIPAL_SHORT}@${REALM}"
  local KEYTAB_FILE="${PRINCIPAL_SHORT}.keytab"
  local KEYTAB_PATH="${KEYTAB_DIR}/${KEYTAB_FILE}"

  echo ""
  echo "  Creating principal: $PRINCIPAL"
  run_kdc kadmin.local -q "addprinc -randkey $PRINCIPAL" 2>/dev/null || true

  echo "  Exporting keytab: $KEYTAB_PATH"
  run_kdc sh -c "rm -f $KEYTAB_PATH"
  run_kdc kadmin.local -q "ktadd -k $KEYTAB_PATH $PRINCIPAL" 2>/dev/null
  run_kdc chmod 644 "$KEYTAB_PATH"

  echo "  Copying to host: $CERT_DIR/$KEYTAB_FILE"
  docker cp "${KDC_CONTAINER}:${KEYTAB_PATH}" "${CERT_DIR}/${KEYTAB_FILE}"
  chmod 644 "${CERT_DIR}/${KEYTAB_FILE}"

  echo ""
  echo "  Done."
  echo ""
  echo "  Principal : $PRINCIPAL"
  echo "  Keytab    : $CERT_DIR/$KEYTAB_FILE"
  echo ""
  echo "  Next steps:"
  echo "    scp $CERT_DIR/$KEYTAB_FILE root@<server>:/path/to/$KEYTAB_FILE"
  echo "    scp certs/client/kafka.truststore.p12 root@<server>:/path/to/"
  echo "    scp certs/client/client.keystore.p12  root@<server>:/path/to/"
  echo ""
  echo "  GSSAPI client config:"
  echo "    security.protocol=SASL_SSL"
  echo "    sasl.mechanism=GSSAPI"
  echo "    sasl.kerberos.service.name=kafka"
  echo "    sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \\"
  echo "      useKeyTab=true storeKey=true \\"
  echo "      keyTab=\"/path/to/$KEYTAB_FILE\" \\"
  echo "      principal=\"$PRINCIPAL\";"
  echo ""
}

op_rekey() {
  echo ""
  echo "  --- Re-key Existing Principal ---"
  echo "  Rotates the key and exports a fresh keytab. Old keytabs stop working immediately."
  echo ""
  op_list

  prompt PRINCIPAL_SHORT "  Principal name to re-key (no @realm)"
  local PRINCIPAL="${PRINCIPAL_SHORT}@${REALM}"
  local KEYTAB_FILE="${PRINCIPAL_SHORT}.keytab"
  local KEYTAB_PATH="${KEYTAB_DIR}/${KEYTAB_FILE}"

  echo ""
  echo "  Re-keying: $PRINCIPAL"
  run_kdc kadmin.local -q "cpw -randkey $PRINCIPAL" 2>/dev/null

  echo "  Exporting fresh keytab..."
  run_kdc sh -c "rm -f $KEYTAB_PATH"
  run_kdc kadmin.local -q "ktadd -k $KEYTAB_PATH $PRINCIPAL" 2>/dev/null
  run_kdc chmod 644 "$KEYTAB_PATH"

  echo "  Copying to host: $CERT_DIR/$KEYTAB_FILE"
  docker cp "${KDC_CONTAINER}:${KEYTAB_PATH}" "${CERT_DIR}/${KEYTAB_FILE}"
  chmod 644 "${CERT_DIR}/${KEYTAB_FILE}"

  echo ""
  echo "  Done. Fresh keytab: $CERT_DIR/$KEYTAB_FILE"
  echo "  Distribute to all servers using this principal — old keytabs are now invalid."
  echo ""
}

op_delete() {
  echo ""
  echo "  --- Delete Principal ---"
  echo "  WARNING: Permanent. All keytabs for this principal stop working immediately."
  echo ""
  op_list

  prompt PRINCIPAL_SHORT "  Principal name to delete (no @realm)"
  local PRINCIPAL="${PRINCIPAL_SHORT}@${REALM}"

  echo ""
  read -rp "  Type the full principal ($PRINCIPAL) to confirm: " CONFIRM
  if [[ "$CONFIRM" != "$PRINCIPAL" ]]; then
    echo "  Does not match — aborting."
    return
  fi

  run_kdc kadmin.local -q "delprinc -force $PRINCIPAL" 2>/dev/null
  echo "  Principal '$PRINCIPAL' deleted."
  echo ""
  echo "  Remove the keytab file from the host if it exists:"
  echo "    rm -f $CERT_DIR/${PRINCIPAL_SHORT}.keytab"
  echo ""
}

show_menu() {
  echo ""
  echo "  ╔══════════════════════════════════════╗"
  echo "  ║      Kafka Kerberos Manager          ║"
  echo "  ║  KDC: $KDC_CONTAINER   Realm: $REALM  ║"
  echo "  ╚══════════════════════════════════════╝"
  echo ""
  echo "  1) List principals"
  echo "  2) Create new principal + keytab"
  echo "  3) Re-key principal (rotate + export fresh keytab)"
  echo "  4) Delete principal"
  echo "  0) Exit"
  echo ""
}

# ─── Main loop ────────────────────────────────────────────────────────────────

check_kdc

while true; do
  show_menu
  read -rp "  Choice: " CHOICE
  echo ""
  case "$CHOICE" in
    1) op_list;   pause ;;
    2) op_create; pause ;;
    3) op_rekey;  pause ;;
    4) op_delete; pause ;;
    0|q|Q) echo "  Goodbye."; exit 0 ;;
    *) echo "  Invalid choice '$CHOICE'."; pause ;;
  esac
done
