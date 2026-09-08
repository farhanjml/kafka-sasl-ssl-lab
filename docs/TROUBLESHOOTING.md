# kafka-sasl-ssl-v8 — Troubleshooting Guide

Confluent Platform 8.2.0 / Apache Kafka 4.2 | KRaft | SCRAM-SHA-256/512 + GSSAPI + Schema Registry + ksqlDB + Kafka Connect

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  Docker Network: kafka-net  Subnet: 172.31.0.0/24              │
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                   │
│  │  kafka1  │   │  kafka2  │   │  kafka3  │                   │
│  │ .0.11    │   │ .0.12    │   │ .0.13    │                   │
│  │ :9093    │   │ :9094    │   │ :9095    │  ← container port  │
│  │ (SASL_SSL│   │ (SASL_SSL│   │ (SASL_SSL│                   │
│  │ :9097    │   │ :9098    │   │ :9099    │  ← SSL-only        │
│  └──────────┘   └──────────┘   └──────────┘                   │
│  ┌───────────┐  ┌──────────────────┐  ┌──────────┐            │
│  │ kafka-kdc │  │ schema-registry  │  │  ksqldb  │            │
│  │ .0.10     │  │ .0.20  :8081 HTTP│  │ .0.21    │            │
│  └───────────┘  └──────────────────┘  │ :8088 HTTP│           │
│                 ┌──────────────────┐   └──────────┘            │
│                 │  kafka-connect   │                            │
│                 │ .0.22  :8083 HTTP│                            │
│                 └──────────────────┘                            │
└────────────────────────────────────────────────────────────────┘
Host: <KAFKA_HOST_IP> | Cluster ID: v9QkkNkaQfCyup16RviRyQ
External ports: SASL_SSL 9193/9194/9195 | SSL 9197/9198/9199 | KDC 18088
```

### Listener Map

| Listener | Protocol | Container port | Host port | Used by |
|----------|----------|---------------|-----------|---------|
| SASL_SSL | SASL_SSL | 9093/9094/9095 | 9193/9194/9195 | external CDC clients |
| SSL | SSL (mTLS) | 9097/9098/9099 | 9197/9198/9199 | SSL-only clients (no SASL) |
| INTERNAL | PLAINTEXT | 19093/19094/19095 | — | SR, ksqlDB, Connect, CLI admin |
| CONTROLLER | PLAINTEXT | 9096 | — | KRaft controller (kafka1 only) |
| PLAINTEXT | PLAINTEXT | 9092 | 9192 | Lab testing — kafka1 only |

**Critical:** `KAFKA_ADVERTISED_LISTENERS` uses HOST ports (9193/9194/9195) for SASL_SSL so external clients get correct addresses from metadata. Internal services use INTERNAL listener (19093/19094/19095) inside Docker.

---

## Quick Verification

Before running the tests below, copy the client keystore into the container (it is not mounted by default — only broker keystores are in `certs/broker/`):

```bash
docker cp certs/client/client.keystore.p12 kafka1:/tmp/client.keystore.p12
```

### SCRAM-SHA-256

```bash
docker exec kafka1 bash -c '
cat > /tmp/scram.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="demo-user" password="<DEMO_SCRAM_PASSWORD>";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=/tmp/client.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
EOF
kafka-topics --bootstrap-server kafka1:9093 \
  --command-config /tmp/scram.properties --list'
```

### GSSAPI / Kerberos

```bash
docker exec kafka1 bash -c '
cat > /tmp/gssapi.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="/etc/keytabs/kafkaclient.keytab" principal="kafkaclient@KAFKA.LOCAL";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=/tmp/client.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
EOF
kafka-topics --bootstrap-server kafka1:9093 \
  --command-config /tmp/gssapi.properties --list'
```

> The `TGT renewal thread has been interrupted` warning at the end is harmless — it is the background ticket-renewal thread exiting when the CLI process ends.

### Produce and Consume

```bash
# Produce
docker exec kafka1 bash -c \
  'echo "test-message" | kafka-console-producer \
    --bootstrap-server kafka1:9093 \
    --topic demo-general \
    --producer.config /tmp/scram.properties'

# Consume
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:9093 \
  --topic demo-general \
  --from-beginning --max-messages 1 --timeout-ms 15000 \
  --consumer.config /tmp/scram.properties
```

---

## Issue #1 — `KAFKA_OPTS is required` / Startup Check Fails

### Symptom

```
KAFKA_OPTS is required.
Command [...] FAILED !
```

Broker container restarts in a loop immediately at startup.

### Root Cause

Confluent's Docker entrypoint requires `KAFKA_OPTS` to be set whenever SASL + GSSAPI is enabled. It must point to a JAAS file on the filesystem.

### Fix

`certs/broker/kafka_jaas.conf` is a stub generated by `generate-certs.sh`. It satisfies the check. `docker-compose.yml` sets:

```yaml
KAFKA_OPTS: "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_jaas.conf"
```

If the file is missing, re-run `generate-certs.sh` and `docker compose up -d` (not restart).

---

## Issue #2 — `JAAS config entry not terminated by semi-colon`

### Symptom

```
java.lang.IllegalArgumentException: JAAS config entry not terminated by semi-colon
```

### Root Cause

YAML `|` block scalars embed literal newlines into environment variable values. Kafka's JAAS parser treats newlines as token boundaries.

### Fix

All `*_SASL_JAAS_CONFIG` environment variables in `docker-compose.yml` must be single-line quoted strings. Never use `|` block scalars for JAAS values.

---

## Issue #3 — Keytab Permission Denied

### Symptom

```
klist: Permission denied while starting keytab scan
javax.security.auth.login.LoginException: Could not login: the client is being asked for a password
```

### Root Cause

`kadmin.local ktadd` creates keytab files owned by `root` with mode `600`. Kafka brokers run as `appuser` (uid=1000).

### Fix

`setup-kerberos.sh` runs `chmod 644` after every `ktadd`. To fix manually:

```bash
docker exec kafka-kdc sh -c \
  'chmod 644 /etc/keytabs/kafka1.keytab \
             /etc/keytabs/kafka2.keytab \
             /etc/keytabs/kafka3.keytab \
             /etc/keytabs/kafkaclient.keytab'
```

---

## Issue #4 — Kerberos `Checksum failed` / `CLIENT_NOT_FOUND`

### Symptom

```
LoginException: Client not found in Kerberos database (6) - CLIENT_NOT_FOUND
LoginException: Checksum failed
```

### Root Cause

**CLIENT_NOT_FOUND** — KDC container was recreated (principals lost) but keytab files still exist in the `keytabs` volume.

**Checksum failed** — `ktadd` appends rather than replaces keytab entries. After a KDC database rebuild, stale entries from the previous database remain in the file with invalid key material.

### Fix

Re-run `setup-kerberos.sh` (it deletes keytabs before re-exporting):

```bash
bash scripts/setup-kerberos.sh
docker compose restart kafka1 kafka2 kafka3
```

Or delete manually before re-running:

```bash
docker exec kafka-kdc sh -c \
  'rm -f /etc/keytabs/kafka1.keytab /etc/keytabs/kafka2.keytab \
         /etc/keytabs/kafka3.keytab /etc/keytabs/kafkaclient.keytab'
bash scripts/setup-kerberos.sh
```

---

## Issue #5 — SSL Keystore Permission Denied

### Symptom

```
KafkaException: Failed to load SSL keystore /etc/kafka/secrets/kafka1.keystore.p12
```

### Root Cause

`openssl pkcs12 -export` defaults to mode `600`. `appuser` inside the container cannot read root-owned `600` files.

### Fix

`generate-certs.sh` runs `chmod 644` on all generated files. To fix manually:

```bash
chmod 644 certs/broker/*.p12
chmod 644 certs/broker/*.key
chmod 644 certs/broker/*.crt
```

---

## Issue #6 — Controller Registration Timeout (SCRAM Chicken-and-Egg)

### Symptom

```
ERROR [BrokerLifecycleManager id=2] Shutting down because we were unable to register
      with the controller quorum.
```

### Root Cause

SCRAM credentials are stored in Kafka's metadata log. At first boot the log is empty — SCRAM authentication cannot succeed. If CONTROLLER or INTERNAL listeners use SASL, the cluster deadlocks and never starts.

### Fix

CONTROLLER and INTERNAL use `PLAINTEXT` in `docker-compose.yml`:

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
```

These listeners are only reachable inside the Docker private network (`172.30.0.0/24`).

---

## Issue #7 — GSSAPI `Server not found in Kerberos database`

### Symptom

```
SaslAuthenticationException: GSS initiate failed
    Caused by: KrbException: Server not found in Kerberos database (7) - LOOKING_UP_SERVER
```

### Root Cause

Kerberos builds the service principal from the hostname of the server the client connects to. If the advertised listener uses an IP address instead of a FQDN, Kerberos looks for `kafka/<KAFKA_HOST_IP>@KAFKA.LOCAL` which does not exist.

### Fix

Advertised listeners use FQDNs in `docker-compose.yml`:

```yaml
KAFKA_ADVERTISED_LISTENERS: SASL_SSL://kafka1:9093,INTERNAL://kafka1:19093
```

Clients outside Docker need `/etc/hosts` entries:

```
<KAFKA_HOST_IP>  kafka1
<KAFKA_HOST_IP>  kafka2
<KAFKA_HOST_IP>  kafka3
```

> Do NOT add `extra_hosts` mapping broker FQDNs to the host IP inside containers — this overrides Docker's internal DNS and breaks the CONTROLLER listener (port 9096 is not published to the host).

---

## Issue #8 — `no such service: kafka-kdc` in setup-kerberos.sh

### Symptom

```
no such service: kafka-kdc
```

Script exits partway through `setup-kerberos.sh`.

### Root Cause

`docker compose restart` takes a **service name**, not a container name. The service is named `kdc` in `docker-compose.yml`; the container is named `kafka-kdc`. Using the container name fails.

### Fix

This is fixed in `scripts/setup-kerberos.sh` — the restart line uses `docker compose restart kdc`. If the error reappears, verify the script hasn't been overwritten with an older version.

---

## Issue #9 — CDC client RC=129 transaction init timeout

### Symptom

```
TCS0129E: Kafka failed to initialize transactions: Timed out waiting for operation to finish
```

All brokers stuck in `TRY_CONNECT` for 120 seconds then give up.

### Root Causes (check in order)

**A. Advertised listener port mismatch** — bootstrap succeeds but metadata returns wrong port.  
Check: broker `KAFKA_ADVERTISED_LISTENERS` uses container port (e.g. `9093`) but external port mapping is different (e.g. `9193:9093`). External clients try `9093` (closed) after getting metadata.  
Fix: advertised listeners must use HOST port (`9193`/`9194`/`9195`).

**B. SASL mechanism not enabled** — SCRAM-SHA-512 missing from `KAFKA_SASL_ENABLED_MECHANISMS`.  
Check: librdkafka log shows `TRY_CONNECT → DOWN` in ~3s (not 120s). SASL negotiation rejects at handshake.  
Fix: add `SCRAM-SHA-512` to mechanisms + add `KAFKA_LISTENER_NAME_SASL__SSL_SCRAM_SHA_512_SASL_JAAS_CONFIG`.

**C. Wrong password** — SCRAM credential in Kafka doesn't match the CDC client config.  
Check: librdkafka log shows `SASL authentication error: Authentication failed during authentication due to invalid credentials`.  
Fix: `kafka-configs --alter --add-config "SCRAM-SHA-256=[password=<actual>]" --entity-name app-client`

**D. `__transaction_state` topic missing** — RC=129 even when connection succeeds.  
Fix: pre-create with 50 partitions (done by `create-topics.sh`).

---

## Issue #10 — Schema Registry / ksqlDB can't connect to brokers

### Symptom

```
Connection to node X (kafkaX/172.31.0.1X:919X) could not be established
```

SR or ksqlDB logs this in a loop. Container stays up but can't function.

### Root Cause

Internal Docker services bootstrap via SASL_SSL (container port 9093). Broker returns metadata with advertised address `kafkaX:9193` (host port). From inside Docker, port `9193` doesn't exist — broker only listens on `9093`. Connection refused.

### Fix

Configure SR/ksqlDB/Connect to use INTERNAL listener (PLAINTEXT, port 1909X):

```yaml
# Schema Registry
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka1:19093,...
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: PLAINTEXT

# ksqlDB
KSQL_BOOTSTRAP_SERVERS: kafka1:19093,...
KSQL_SECURITY_PROTOCOL: PLAINTEXT

# Connect
CONNECT_BOOTSTRAP_SERVERS: PLAINTEXT://kafka1:19093,...
CONNECT_SECURITY_PROTOCOL: PLAINTEXT
```

---

## Issue #11 — kafka-connect crash loop, exit 0, no error in logs

### Symptom

Container restarts every ~35 seconds. Port 8083 never opens. Logs show plugin scan then stop. No ERROR line.

### Root Cause

`CONNECT_HEAP_OPTS` writes to properties file only — NOT to actual JVM startup args. Confluent Docker uses `KAFKA_HEAP_OPTS` env var for JVM args. Without it, JVM defaults to `-Xmx2G`, OOM-killed immediately against small `mem_limit`. Exit code is 0 (JVM exits before logging anything).

### Fix

```yaml
CONNECT_HEAP_OPTS: "-Xms256m -Xmx512m"
KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"   # ← this one actually sets JVM args
mem_limit: 768m                          # must exceed heap + JVM overhead (~200m)
```

Same pattern applies to ksqlDB (`KSQL_HEAP_OPTS` + `KAFKA_HEAP_OPTS`).

---

## Full Reset Procedure

```bash
sudo docker compose down -v
./quick-start.sh
# After quick-start, re-copy certs to the CDC client (new CA generated each run):
scp certs/ca/ca-cert.crt certs/client/client.crt certs/client/client.key \
  certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/
```

> `docker compose down -v` wipes ALL named volumes. Full `quick-start.sh` required after — SCRAM users, topics, Kerberos principals all recreated. Re-copy certs to the CDC client after every cert regeneration.

---

## File Reference

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Full cluster definition |
| `certs/broker/kafka_jaas.conf` | JAAS stub (generated by `generate-certs.sh`) |
| `config/client-internal.properties` | CLI config for INTERNAL (PLAINTEXT) listener |
| `kerberos/krb5.conf` | Kerberos client config |
| `scripts/generate-certs.sh` | CA, broker certs, PKCS12 keystores |
| `scripts/setup-kerberos.sh` | Kerberos principals and keytabs |
| `scripts/create-scram-users.sh` | SCRAM-SHA-256 users |
| `scripts/create-topics.sh` | Lab topics + `__transaction_state` |
| `quick-start.sh` | Full setup in one command |
