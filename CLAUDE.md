# CLAUDE.md — kafka-sasl-ssl-v8

This file provides guidance to Claude Code when working with this repository.

## What This Is

A 3-broker Apache Kafka cluster (Confluent Platform 8.2.0 / Apache Kafka 4.2, KRaft mode) running in Docker with a full security stack, plus the Confluent service stack:

- **SSL-only listener** on each broker (ports 9197/9198/9199) — pure mTLS, no SASL — for clients that cannot do SASL
- **Schema Registry** (port 8081, HTTPS)
- **ksqlDB** (port 8088, HTTPS)
- **Kafka Connect** (port 8083, HTTPS)
- **SSL-only client cert package** (`certs/tmon/`) — pre-generated, ready to hand off to any SSL-only consumer (e.g. mainframe TMON integration)

Host: `<KAFKA_HOST_IP>` (AlmaLinux). Network: `172.31.0.0/24`.

---

## Common Commands

### Full deployment (first time or after full teardown)

```bash
chmod +x quick-start.sh scripts/*.sh
./quick-start.sh
```

### Manual step-by-step

```bash
bash scripts/generate-certs.sh              # CA + broker + service + SSL-only client certs
docker compose up -d                        # Start all containers (KDC, brokers, SR, ksqlDB, Connect)
sleep 10
bash scripts/setup-kerberos.sh              # Create Kerberos principals + keytabs
docker compose restart kafka1 kafka2 kafka3
sleep 15
bash scripts/create-scram-users.sh          # Create SCRAM-SHA-256 users
bash scripts/create-topics.sh               # Create application topics
```

### Day-to-day operations

```bash
# Start / stop
docker compose up -d
docker compose down

# Check broker logs
docker logs kafka1 --tail 50
docker logs kafka2 --tail 50
docker logs kafka3 --tail 50

# Check new service logs
docker logs schema-registry --tail 50
docker logs ksqldb --tail 50
docker logs kafka-connect --tail 50

# List topics (INTERNAL PLAINTEXT listener — no auth needed)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --list

# Verify all brokers are up
docker exec kafka1 kafka-broker-api-versions \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  2>&1 | grep "^kafka"

# Check versions across all brokers
bash scripts/check-versions.sh

# Re-export CDC client keytab from KDC
docker exec kafka-kdc kadmin.local -q \
  "ktadd -k /etc/keytabs/kafkaclient.keytab kafkaclient@KAFKA.LOCAL"
docker cp kafka-kdc:/etc/keytabs/kafkaclient.keytab certs/keytabs/kafkaclient.keytab

# Produce (SCRAM-SHA-256 over SASL_SSL)
docker cp certs/client/client.keystore.p12 kafka1:/tmp/client.keystore.p12
docker cp certs/client/kafka.truststore.p12 kafka1:/tmp/kafka.truststore.p12

docker exec kafka1 bash -c 'echo "test-message" | kafka-console-producer \
  --bootstrap-server kafka1:9093 \
  --topic cdc-transactions \
  --producer-property security.protocol=SASL_SSL \
  --producer-property sasl.mechanism=SCRAM-SHA-256 \
  --producer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"demo-user\" password=\"<DEMO_SCRAM_PASSWORD>\";" \
  --producer-property ssl.truststore.location=/tmp/kafka.truststore.p12 \
  --producer-property ssl.truststore.type=PKCS12 \
  --producer-property ssl.truststore.password=<SSL_KEYSTORE_PASSWORD> \
  --producer-property ssl.keystore.location=/tmp/client.keystore.p12 \
  --producer-property ssl.keystore.type=PKCS12 \
  --producer-property ssl.keystore.password=<SSL_KEYSTORE_PASSWORD> \
  --producer-property ssl.endpoint.identification.algorithm='

# Consume — PLAINTEXT/INTERNAL (quickest, no auth)
docker exec -i kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --topic cdc-transactions \
  --from-beginning \
  --max-messages 20

# Consume — SASL_SSL (SCRAM-SHA-256 + mTLS)
docker cp certs/client/client.keystore.p12 kafka1:/tmp/client.keystore.p12
docker cp certs/client/kafka.truststore.p12 kafka1:/tmp/kafka.truststore.p12

docker exec kafka1 bash -c "cat > /tmp/consume-sasl.properties << 'EOF'
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"demo-user\" password=\"<DEMO_SCRAM_PASSWORD>\";
ssl.truststore.location=/tmp/kafka.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=/tmp/client.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
EOF"

docker exec -i kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:9093 \
  --command-config /tmp/consume-sasl.properties \
  --topic cdc-transactions \
  --from-beginning --max-messages 20

# PLAINTEXT topic creation (pinned to broker 1 — only kafka1 has port 9192)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --create --topic <topic-name> \
  --replica-assignment 1
```

---

## Architecture

### Containers and network

| Container | IP | Host Ports | Role |
|-----------|-----|-----------|------|
| `kafka-kdc` | 172.31.0.10 | 18088, 18749 | MIT Kerberos KDC, realm `KAFKA.LOCAL` |
| `kafka1` | 172.31.0.11 | 9193, 9192, 9197 | Broker + KRaft controller |
| `kafka2` | 172.31.0.12 | 9194, 9198 | Broker only |
| `kafka3` | 172.31.0.13 | 9195, 9199 | Broker only |
| `schema-registry` | 172.31.0.20 | 8081 | Schema Registry (HTTPS) |
| `ksqldb` | 172.31.0.21 | 8088 | ksqlDB (HTTPS) |
| `kafka-connect` | 172.31.0.22 | 8083 | Kafka Connect (HTTPS) |

Network: `kafka-net`, `172.31.0.0/24`. RAM budget: ~3.7 GB total — upgraded host RAM required.

### Listener layout

| Listener | Protocol | Internal port | Host port | Used by |
|----------|----------|-------------|-----------|---------|
| `SASL_SSL` | SASL_SSL | 9093/9094/9095 | 9193/9194/9195 | External CDC clients, internal services (SR, ksqlDB, Connect) |
| `SSL` | SSL (mTLS) | 9097/9098/9099 | 9197/9198/9199 | SSL-only clients (no SASL support) |
| `PLAINTEXT` | PLAINTEXT | 9092 | 9192 | Lab testing — kafka1 only |
| `CONTROLLER` | PLAINTEXT | 9096 | — | KRaft controller (internal only) |
| `INTERNAL` | PLAINTEXT | 19093/19094/19095 | — | Inter-broker + CLI admin |

The `SSL` listener is pure mTLS — no SASL. Use it for clients that cannot do SASL/SCRAM/GSSAPI.  
Schema Registry, ksqlDB, and Kafka Connect connect to Kafka internally via `SASL_SSL`.

### SCRAM Users

| User | Password | Purpose |
|------|----------|---------|
| `admin` | `<ADMIN_SCRAM_PASSWORD>` | Cluster admin + internal service auth (SR, ksqlDB, Connect) |
| `app-client` | `<APP_SCRAM_PASSWORD>` | External CDC client |
| `demo-user` | `<DEMO_SCRAM_PASSWORD>` | General testing |

### Topics

| Topic | Partitions | Replication | Purpose |
|-------|-----------|-------------|---------|
| `cdc-transactions` | 6 | 2 | CDC — transactions |
| `cdc-accounts` | 3 | 2 | CDC — accounts |
| `cdc-customers` | 3 | 2 | CDC — customers |
| `banklab-events` | 3 | 2 | BankLab events |
| `demo-general` | 1 | 1 | General testing |
| `__transaction_state` | 50 | 2 | Transactional producer state |

External integrators (e.g. installers that auto-create topics via Admin API) should manage their own topics — do not pre-create.

### Named Volumes

Docker Compose prefixes each volume with the project name (the directory name by default). For this repo:

| Volume | Purpose |
|--------|---------|
| `<project>_kdc-data` | Kerberos volume mount (see KDC persistence warning below) |
| `<project>_keytabs` | Shared keytabs (KDC + all brokers) |
| `<project>_kafka1-data` | Kafka log + KRaft metadata (SCRAM users, topics) |
| `<project>_kafka2-data` | Kafka log data |
| `<project>_kafka3-data` | Kafka log data |

### Cert directories

| Path | Contents | Used by |
|------|----------|---------|
| `certs/ca/` | `ca-cert.crt`, `ca-key.pem` (600) | Signs all other certs |
| `certs/broker/` | `kafka1/2/3.keystore.p12`, `kafka.truststore.p12`, cred files, JAAS stub | Broker containers |
| `certs/client/` | `client.keystore.p12`, `client.crt`, `client.key` | external CDC client, lab testing |
| `certs/schema-registry/` | `schema-registry.keystore.p12`, `kafka.truststore.p12` | Schema Registry container |
| `certs/ksqldb/` | `ksqldb.keystore.p12`, `kafka.truststore.p12` | ksqlDB container |
| `certs/connect/` | `connect.keystore.p12`, `kafka.truststore.p12` | Kafka Connect container |
| `certs/tmon/` | `tmon-client.keystore.p12`, `tmon-client.crt`, `tmon-client.key`, `kafka.truststore.p12` | SSL-only client handoff package (pre-generated, ready for handoff) |
| `certs/keytabs/` | `kafkaclient.keytab` | CDC client host (Kerberos) |

---

## SSL / PKI

- **Format**: PKCS12 for all keystores (Confluent Docker native format)
- **Password**: `<SSL_KEYSTORE_PASSWORD>` for all keystores, truststores, and key passwords
- **CA**: `certs/ca/ca-cert.crt` — signs all service and client certs
- **mTLS**: `KAFKA_SSL_CLIENT_AUTH: required` on all SSL-using listeners — every client must present a cert trusted by our CA
- **SAN**: All server certs include `DNS:<hostname>`, `IP:<KAFKA_HOST_IP>`, `IP:127.0.0.1`
- **SSL-only client cert CN**: `tmon-client` — `extendedKeyUsage=clientAuth` only

### How internal services authenticate to Kafka

Schema Registry, ksqlDB, and Kafka Connect connect to Kafka via `SASL_SSL` (not the SSL-only listener). They use:
- **SASL**: SCRAM-SHA-256 with `admin` / `<ADMIN_SCRAM_PASSWORD>`
- **SSL**: their own service keystore (e.g. `schema-registry.keystore.p12`) as client cert + shared truststore

### How SSL-only clients connect

SSL-only clients connect to the `SSL` listener (pure mTLS, no SASL). They present a client keystore (e.g. `certs/tmon/tmon-client.keystore.p12`) and our brokers verify against `kafka.truststore.p12` (which contains the CA cert).

---

## Kerberos

| Item | Value |
|------|-------|
| Realm | `KAFKA.LOCAL` |
| KDC host (external) | `<KAFKA_HOST_IP>:18088` |
| KDC host (internal Docker) | `kdc:88` |
| KDC master password | `<KDC_MASTER_PASSWORD>` |
| Client principal | `kafkaclient@KAFKA.LOCAL` |

For external clients doing `kinit`, update `krb5.conf` to use port `18088`:
```ini
[realms]
 KAFKA.LOCAL = {
  kdc = <KAFKA_HOST_IP>:18088
  admin_server = <KAFKA_HOST_IP>:18088
 }
```

KDC persistence warning: the actual KDC database lives at `/var/kerberos/krb5kdc/` (writable layer), not in the `kdc-data` volume. Recreating the `kafka-kdc` container destroys all principals. After any KDC recreation, run `setup-kerberos.sh` + restart all brokers.

---

## External CDC Client Integration

CDC client host: `<CDC_CLIENT_HOST_IP>` | Cert path: `/opt/cdc-client/certs/`

Push certs after cert regeneration:
```bash
scp certs/ca/ca-cert.crt certs/client/client.crt certs/client/client.key \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/
scp certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/opt/cdc-client/certs/
```

The CDC client connects via `SASL_SSL` on port **9193**. Update the CDC client's connection config when switching to this lab.

---

## Key Constraints and Non-Obvious Behaviours

- **`auto.create.topics.enable=false`** — all topics must be created explicitly. External installers that create their own topics via Admin API still work.
- **PLAINTEXT topics must use `--replica-assignment 1`** — only kafka1 has the PLAINTEXT listener (host port 9192). Use `manage-topic.sh` option 3 → type 2.
- **SSL listener shares broker keystores** — the `SSL://` listener on each broker uses the same `kafkaX.keystore.p12` and `kafka.truststore.p12` as the `SASL_SSL` listener. No separate keystore needed.
- **SSL-only listener has no SASL** — do not send SASL credentials to ports 9197/9198/9199. Use SASL_SSL on 9193/9194/9195 instead.
- **YAML `|` blocks break JAAS configs** — `*_SASL_JAAS_CONFIG` env vars must be single-line quoted strings.
- **Config file edits require `docker compose up -d`** not `docker compose restart` — Docker bind-mount inodes are cached.
- **SCRAM users stored in Kafka metadata log** — re-run `create-scram-users.sh` if `kafka1-data` volume is deleted.
- **Keytab permissions**: `setup-kerberos.sh` runs `chmod 644` after each `ktadd` so `appuser` inside broker containers can read them.
- **`docker compose restart` uses service names** — service is `kdc`, container is `kafka-kdc`. `docker compose restart kafka-kdc` fails; use `docker compose restart kdc`.
- **client.keystore.p12 not mounted in broker containers** — `docker cp` it to `/tmp/` before using SASL_SSL for produce/consume. Lost on container restart.
- **`-it` vs `-i` with pipes** — always use `docker exec -i` (no `-t`) when piping output to `jq`, `grep`, etc.
- **Deprecated CLI flags (CP 8.x)**: `--formatter-property` not `--property`, `--command-config` not `--consumer.config`.
- **Advertised listeners must use HOST ports** — `KAFKA_ADVERTISED_LISTENERS` uses `9193/9194/9195` (host ports), not `9093/9094/9095` (container ports). External clients use metadata to connect after bootstrap — wrong port = RC=129 transaction timeout.
- **Internal services use INTERNAL listener** — SR, ksqlDB, Connect connect via `PLAINTEXT://kafkaX:1909X`. They cannot reach host-mapped ports from inside Docker.
- **`KAFKA_HEAP_OPTS` sets actual JVM args for Connect** — `CONNECT_HEAP_OPTS` only writes to properties file. Without `KAFKA_HEAP_OPTS`, JVM defaults to `-Xmx2G` and OOM-kills the container silently (exit 0, no log).
- **app-client password must match exactly what the CDC client is configured with** — double-check the client's output target config before creating SCRAM credentials.
- **Re-copy certs to the CDC client after every cert regen** — `generate-certs.sh` creates a new CA each run. New CA = CDC client connection fails until new `ca-cert.crt`, `client.crt`, `client.key`, `kafkaclient.keytab` pushed to `<CDC_CLIENT_HOST_IP>`.

---

## File Layout

| Path | Purpose |
|------|---------|
| `docker-compose.yml` | All containers, networks, volumes |
| `quick-start.sh` | Full deployment sequence |
| `kerberos/Dockerfile` | KDC container build (AlmaLinux 9 + krb5-server) |
| `kerberos/krb5.conf` | Kerberos client config |
| `scripts/generate-certs.sh` | CA + broker + service + SSL-only client certs |
| `scripts/setup-kerberos.sh` | Kerberos principals + keytabs |
| `scripts/create-scram-users.sh` | SCRAM-SHA-256 users |
| `scripts/create-topics.sh` | Application topics |
| `scripts/check-versions.sh` | Confluent Platform + Kafka versions per broker |
| `manage/manage-scram.sh` | Interactive SCRAM user management |
| `manage/manage-kerberos.sh` | Interactive Kerberos principal management |
| `manage/manage-topic.sh` | Interactive topic management |
| `config/client-internal.properties` | CLI tool config for INTERNAL listener |
| `certs/broker/` | Broker keystores, truststore, cred files, JAAS stub |
| `certs/ca/` | CA cert + private key |
| `certs/client/` | General client cert (external CDC client, lab) |
| `certs/schema-registry/` | Schema Registry keystore + truststore |
| `certs/ksqldb/` | ksqlDB keystore + truststore |
| `certs/connect/` | Kafka Connect keystore + truststore |
| `certs/tmon/` | SSL-only client cert package (pre-generated, ready for handoff) |
| `certs/keytabs/` | Kerberos keytabs |
| `docs/` | Operations, Kerberos, CDC client integration, troubleshooting, patches |
