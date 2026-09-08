#!/bin/bash
###############################################################################
# setup-kerberos.sh — Fix KDC config, rebuild database, create principals
#                     and export fresh keytabs for brokers + external CDC client
#
# Run AFTER docker compose up (KDC must be running).
# Safe to re-run: deletes stale keytabs before exporting to avoid
# "Password incorrect" errors caused by accumulated duplicate kvno entries.
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../certs/keytabs"

KDC_CONTAINER="kafka-kdc"
REALM="KAFKA.LOCAL"
KEYTAB_DIR="/etc/keytabs"
KDC_MASTER_PASSWORD="<KDC_MASTER_PASSWORD>"

BROKERS=("kafka1" "kafka2" "kafka3")
BROKER_FQDNS=("kafka1" "kafka2" "kafka3")

echo "=== Kerberos Setup for Kafka SSL Lab v8 ==="
echo "Realm: $REALM"
echo ""

# ─── 1. Wait for KDC ────────────────────────────────────────────────────────
echo ">>> Waiting for KDC..."
for i in $(seq 1 15); do
  if docker exec $KDC_CONTAINER kadmin.local -q "listprincs" &>/dev/null; then
    echo "    KDC ready."
    break
  fi
  echo "    Attempt $i/15 — waiting 3s..."
  sleep 3
done

# ─── 2. Fix kdc.conf ────────────────────────────────────────────────────────
# The container image ships /var/kerberos/krb5kdc/kdc.conf configured for
# EXAMPLE.COM (RHEL default). Overwrite it with the correct KAFKA.LOCAL config
# so krb5kdc can decrypt the KAFKA.LOCAL database on startup.
echo ""
echo ">>> Fixing kdc.conf for KAFKA.LOCAL..."
docker exec $KDC_CONTAINER sh -c "cat > /var/kerberos/krb5kdc/kdc.conf << 'EOF'
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    KAFKA.LOCAL = {
        acl_file = /var/kerberos/krb5kdc/kadm5.acl
        dict_file = /usr/share/dict/words
        supported_enctypes = aes256-cts:normal aes128-cts:normal
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
    }
EOF"
docker exec $KDC_CONTAINER sh -c \
  "cp /etc/krb5kdc/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl 2>/dev/null || \
   echo '*/admin@KAFKA.LOCAL *' > /var/kerberos/krb5kdc/kadm5.acl"
echo "    kdc.conf updated."

# ─── 3. Rebuild database with known master key ───────────────────────────────
# The image database was created with the RHEL default master_key_type
# (aes256-cts-hmac-sha384-192) which conflicts with our supported_enctypes.
# Destroy and recreate with a consistent config so keytab auth works.
echo ""
echo ">>> Rebuilding KDC database for KAFKA.LOCAL..."
docker exec $KDC_CONTAINER sh -c \
  "kdb5_util destroy -f 2>/dev/null; \
   kdb5_util create -r $REALM -s -P $KDC_MASTER_PASSWORD"
echo "    Database rebuilt."

# ─── 4. Restart KDC to reload new database + kdc.conf ───────────────────────
echo ""
echo ">>> Restarting KDC to load new database..."
docker compose restart kdc
sleep 5
# Verify KDC is back up
for i in $(seq 1 10); do
  if docker exec $KDC_CONTAINER kadmin.local -q "listprincs" &>/dev/null; then
    echo "    KDC ready."
    break
  fi
  echo "    Waiting for KDC ($i/10)..."
  sleep 3
done

# ─── 5. Create broker principals and keytabs ─────────────────────────────────
for i in "${!BROKERS[@]}"; do
  BROKER="${BROKERS[$i]}"
  FQDN="${BROKER_FQDNS[$i]}"
  PRINCIPAL="kafka/${FQDN}@${REALM}"
  KEYTAB="${KEYTAB_DIR}/${BROKER}.keytab"

  echo ""
  echo ">>> Creating principal: $PRINCIPAL"
  docker exec $KDC_CONTAINER kadmin.local -q \
    "addprinc -randkey $PRINCIPAL" 2>/dev/null || true

  echo ">>> Exporting keytab: $KEYTAB"
  # Delete before exporting — ktadd appends rather than replaces.
  # Stale entries with old key material cause "Password incorrect" on kinit.
  docker exec $KDC_CONTAINER sh -c "rm -f $KEYTAB"
  docker exec $KDC_CONTAINER kadmin.local -q \
    "ktadd -k $KEYTAB $PRINCIPAL" 2>/dev/null
  docker exec $KDC_CONTAINER chmod 644 "$KEYTAB"
done

# ─── 6. Create CDC client principal and keytab ──────────────────────────────
CLIENT_PRINCIPAL="kafkaclient@${REALM}"
CLIENT_KEYTAB="${KEYTAB_DIR}/kafkaclient.keytab"

echo ""
echo ">>> Creating client principal: $CLIENT_PRINCIPAL"
docker exec $KDC_CONTAINER kadmin.local -q \
  "addprinc -randkey $CLIENT_PRINCIPAL" 2>/dev/null || true

echo ">>> Exporting keytab: $CLIENT_KEYTAB"
docker exec $KDC_CONTAINER sh -c "rm -f $CLIENT_KEYTAB"
docker exec $KDC_CONTAINER kadmin.local -q \
  "ktadd -k $CLIENT_KEYTAB $CLIENT_PRINCIPAL" 2>/dev/null
docker exec $KDC_CONTAINER chmod 644 "$CLIENT_KEYTAB"

# ─── 7. Copy client keytab to certs directory (for scp to CDC client host) ───────
echo ""
echo ">>> Copying kafkaclient.keytab to $CERT_DIR/..."
docker cp $KDC_CONTAINER:$CLIENT_KEYTAB "$CERT_DIR/kafkaclient.keytab"
chmod 644 "$CERT_DIR/kafkaclient.keytab"
echo "    Copied: $CERT_DIR/kafkaclient.keytab"

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Kerberos setup complete ==="
echo ""
echo "Principals created:"
docker exec $KDC_CONTAINER kadmin.local -q "listprincs" 2>/dev/null \
  | grep -E "kafka|kafkaclient"
echo ""
echo "Keytabs in $KEYTAB_DIR (inside KDC container):"
docker exec $KDC_CONTAINER ls -la $KEYTAB_DIR/
echo ""
echo "Client keytab for CDC client: $CERT_DIR/kafkaclient.keytab"
echo ""
echo "NOTE: Restart brokers to pick up new keytabs:"
echo "  docker compose restart kafka1 kafka2 kafka3"
echo ""
echo "Then copy keytab to CDC client host:"
echo "  scp $CERT_DIR/kafkaclient.keytab cdc-client-host:/opt/cdc-client/certs/kafkaclient.keytab"
