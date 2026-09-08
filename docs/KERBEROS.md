# Kerberos Maintenance Guide — v8

## Tickets vs Keytabs — Key Distinction

| Item | What it is | Expires? | Action needed |
|------|-----------|----------|---------------|
| **Keytab file** | Cryptographic key file on disk | Never (unless re-keyed) | Copy once to CDC client host, done |
| **TGT (ticket)** | In-memory session credential | 24 hours | The client renews automatically using the keytab |
| **KDC database** | Principals + master key on broker host | Never (until container recreated) | See KDC persistence section |

**You do not need to touch the keytab daily.** The CDC client (via librdkafka) presents the keytab to the KDC automatically on each connection and whenever its ticket expires.

---

## Routine Checks

### Verify the KDC is running

```bash
docker exec kafka-kdc kadmin.local -q "listprincs" 2>&1 | grep kafka
```

Expected output — all four principals:

```
kafka/kafka1@KAFKA.LOCAL
kafka/kafka2@KAFKA.LOCAL
kafka/kafka3@KAFKA.LOCAL
kafkaclient@KAFKA.LOCAL
```

### Verify a keytab is still valid (from CDC client host)

```bash
kinit -kt /opt/cdc-client/certs/kafkaclient.keytab \
  kafkaclient@KAFKA.LOCAL
klist
```

### Check broker Kerberos authentication

```bash
docker logs kafka1 2>&1 | grep -i "gssapi\|kerberos\|auth" | tail -10
```

---

## KDC Persistence Warning

The KDC database lives in the **container writable layer**, not in a persistent volume. The `kafka-sasl-ssl-lab_kdc-data` Docker volume mounts at `/var/lib/krb5kdc` but the real database is at `/var/kerberos/krb5kdc/` inside the container.

| Event | KDC survives? |
|-------|--------------|
| `docker compose restart kdc` | Yes ✓ |
| `docker compose up -d kdc` (after config change — recreates container) | **No** ✗ |
| Host reboot | Yes ✓ |
| `docker compose down -v` | No ✗ |

**After any container recreation, run the full recovery procedure below.**

---

## Full Recovery — After KDC Container Recreation

The quickest path is to re-run `setup-kerberos.sh` which handles everything:

```bash
cd kafka-sasl-ssl-lab
bash scripts/setup-kerberos.sh
docker compose restart kafka1 kafka2 kafka3
sleep 20
```

Then push the fresh keytab to the CDC client host:

```bash
scp certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/kafkaclient.keytab
```

If you need to run steps manually:

### Step 1 — Fix kdc.conf

```bash
docker exec kafka-kdc sh -c "cat > /var/kerberos/krb5kdc/kdc.conf << 'EOF'
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
docker exec kafka-kdc sh -c \
  "cp /etc/krb5kdc/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl 2>/dev/null || \
   echo '*/admin@KAFKA.LOCAL *' > /var/kerberos/krb5kdc/kadm5.acl"
```

### Step 2 — Rebuild database

```bash
docker exec kafka-kdc sh -c \
  "kdb5_util destroy -f 2>/dev/null; \
   kdb5_util create -r KAFKA.LOCAL -s -P <KDC_MASTER_PASSWORD>"
```

### Step 3 — Restart KDC

```bash
# Use service name 'kdc', not container name 'kafka-kdc'
docker compose restart kdc
sleep 5
```

### Step 4 — Recreate principals and export keytabs

```bash
bash scripts/setup-kerberos.sh
```

### Step 5 — Restart brokers

```bash
docker compose restart kafka1 kafka2 kafka3
sleep 20
```

### Step 6 — Copy fresh keytab to CDC client host

```bash
scp certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/kafkaclient.keytab
```

### Step 7 — Verify on CDC client host

```bash
kinit -kt /opt/cdc-client/certs/kafkaclient.keytab \
  kafkaclient@KAFKA.LOCAL && klist
```

---

## Re-keying a Principal (rotate without full rebuild)

```bash
# Rotate key and export fresh keytab
docker exec kafka-kdc sh -c "rm -f /etc/keytabs/kafkaclient.keytab"
docker exec kafka-kdc kadmin.local -q \
  "ktadd -k /etc/keytabs/kafkaclient.keytab kafkaclient@KAFKA.LOCAL"
docker exec kafka-kdc chmod 644 /etc/keytabs/kafkaclient.keytab

# Copy to host
docker cp kafka-kdc:/etc/keytabs/kafkaclient.keytab \
  certs/keytabs/kafkaclient.keytab

# Copy to CDC client host
scp certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/kafkaclient.keytab
```

No broker restart needed — `kafkaclient` is only used by the external CDC client, not by the brokers.

---

## Troubleshooting Reference

| Error | Cause | Fix |
|-------|-------|-----|
| `Password incorrect` | Stale keytab entries from previous KDC database | `rm keytab && ktadd` |
| `Cannot contact any KDC` | Port 88 not reachable or `/etc/hosts` missing | Check port 88 published; add `<KAFKA_HOST_IP> kdc` to `/etc/hosts` |
| `Client not found in Kerberos database` | KDC recreated, principals lost | Run full recovery procedure |
| `Clock skew too great` | Time diff > 5 min between CDC client host and KDC | `chronyc makestep` or `ntpdate` |
| `no such service: kafka-kdc` | `docker compose restart` called with container name | Use service name `kdc`, not container name `kafka-kdc` |
| `No credentials were supplied` | CDC client not configured with keytab path | Set `sasl.kerberos.keytab` and `sasl.kerberos.principal` in the CDC client config |
| `No provider for SASL mechanism GSSAPI` | librdkafka compiled without SASL | Install `cyrus-sasl-gssapi` on the CDC client host OS |

---

## Current Configuration Summary

| Item | Value |
|------|-------|
| Realm | `KAFKA.LOCAL` |
| KDC host | `kdc` → `<KAFKA_HOST_IP>` (port 88) |
| KDC master password | `<KDC_MASTER_PASSWORD>` |
| Client principal | `kafkaclient@KAFKA.LOCAL` |
| Client keytab (broker host) | `certs/keytabs/kafkaclient.keytab` |
| Client keytab (CDC client host) | `/opt/cdc-client/certs/kafkaclient.keytab` |
| Ticket lifetime | 24 hours (renewable 7 days) |
| Broker principals | `kafka/kafkaN@KAFKA.LOCAL` (N = 1, 2, 3) |
