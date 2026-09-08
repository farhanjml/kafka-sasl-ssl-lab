# Kafka SSL Lab v8 — Operations Guide

3-broker Confluent Platform 8.2.0 (Apache Kafka 4.2) cluster with KRaft, mutual TLS, SCRAM-SHA-256, and Kerberos GSSAPI.
Running on `<KAFKA_HOST_IP>` (AlmaLinux), co-hosted with BankLab.

---

## Quick Reference

| Item | Value |
|------|-------|
| Host | `<KAFKA_HOST_IP>` |
| External ports | `9093` (kafka1), `9094` (kafka2), `9095` (kafka3) |
| Cluster ID | `ErAddTzdTLSPKOogQn1N3w` |
| SSL password | `<SSL_KEYSTORE_PASSWORD>` |
| Kerberos realm | `KAFKA.LOCAL` |
| Project root | your deployment directory |
| Confluent Platform | `8.2.0` |
| Apache Kafka | `4.2` |

### SCRAM Users

| User | Password | Purpose |
|------|----------|---------|
| `admin` | `<ADMIN_SCRAM_PASSWORD>` | Cluster admin |
| `app-client` | `<APP_SCRAM_PASSWORD>` | External CDC client |
| `demo-user` | `<DEMO_SCRAM_PASSWORD>` | General testing |

### Topics

| Topic | Partitions | Replication | Purpose |
|-------|-----------|-------------|---------|
| `cdc-transactions` | 6 | 2 | CDC — transactions |
| `cdc-accounts` | 3 | 2 | CDC — accounts |
| `cdc-customers` | 3 | 2 | CDC — customers |
| `banklab-events` | 3 | 2 | BankLab events |
| `demo-general` | 1 | 1 | Testing |

### Kerberos Principals

| Principal | Keytab | Used by |
|-----------|--------|---------|
| `kafka/kafka1@KAFKA.LOCAL` | `/etc/keytabs/kafka1.keytab` | Broker 1 |
| `kafka/kafka2@KAFKA.LOCAL` | `/etc/keytabs/kafka2.keytab` | Broker 2 |
| `kafka/kafka3@KAFKA.LOCAL` | `/etc/keytabs/kafka3.keytab` | Broker 3 |
| `kafkaclient@KAFKA.LOCAL` | `/etc/keytabs/kafkaclient.keytab` | External CDC client |

---

## Day-to-Day Operations

```bash
# Start cluster
docker compose up -d

# Stop cluster (data preserved)
docker compose down

# Check broker health
docker exec kafka1 kafka-broker-api-versions \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  2>&1 | grep "^kafka"

# Tail broker logs
docker logs kafka1 --tail 50 -f
docker logs kafka2 --tail 50 -f
docker logs kafka3 --tail 50 -f
```

---

## Topic Management

> **Note:** `auto.create.topics.enable=false` — all topics must be created explicitly.

### Interactive script (recommended)

```bash
bash manage/manage-topic.sh
```

### Manual commands

```bash
# List topics
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --list

# Describe a topic
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --describe --topic <TOPIC_NAME>

# Create a topic
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --create --topic <TOPIC_NAME> \
  --partitions 3 \
  --replication-factor 2

# Change topic config (e.g. retention to 7 days)
docker exec kafka1 kafka-configs \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --alter --entity-type topics --entity-name <TOPIC_NAME> \
  --add-config retention.ms=604800000

# Delete a topic (permanent)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --delete --topic <TOPIC_NAME>
```

| Operation | Supported | Notes |
|-----------|-----------|-------|
| Increase partitions | Yes | Irreversible |
| Decrease partitions | No | Kafka does not support this |
| Change retention, compaction | Yes | `kafka-configs --alter` |
| Change replication factor | Indirect | Requires `kafka-reassign-partitions` |

---

## SCRAM User Management

SCRAM credentials are stored in the Kafka metadata log (KRaft), not in a file.
If the `kafka-sasl-ssl-lab_kafka1-data` volume is wiped, re-run `scripts/create-scram-users.sh`.

### Interactive script (recommended)

```bash
bash manage/manage-scram.sh
```

### Manual commands

```bash
# List all SCRAM users
docker exec kafka1 kafka-configs \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --describe --entity-type users

# Create a new user
docker exec kafka1 kafka-configs \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=<PASSWORD>]' \
  --entity-type users --entity-name <USERNAME>

# Delete a user
docker exec kafka1 kafka-configs \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --alter \
  --delete-config 'SCRAM-SHA-256' \
  --entity-type users --entity-name <USERNAME>
```

---

## Kerberos / Keytab Management

See [`KERBEROS.md`](KERBEROS.md) for the full KDC recovery procedure.

### Interactive script (recommended)

```bash
bash manage/manage-kerberos.sh
```

### Manual commands

```bash
# List all principals
docker exec kafka-kdc kadmin.local -q "listprincs" 2>/dev/null | grep -v "^Authenticating"

# Export keytab (always delete first to avoid stale kvno entries)
docker exec kafka-kdc sh -c "rm -f /etc/keytabs/<NAME>.keytab"
docker exec kafka-kdc kadmin.local -q \
  "ktadd -k /etc/keytabs/<NAME>.keytab <PRINCIPAL_NAME>@KAFKA.LOCAL" 2>/dev/null
docker exec kafka-kdc chmod 644 /etc/keytabs/<NAME>.keytab
docker cp kafka-kdc:/etc/keytabs/<NAME>.keytab certs/keytabs/<NAME>.keytab
```

---

## Adding a New CDC Source

### 1. Create a topic

```bash
bash manage/manage-topic.sh
```

### 2. Create a SCRAM user

```bash
bash manage/manage-scram.sh
```

### 3. Create a Kerberos principal (if using GSSAPI)

```bash
bash manage/manage-kerberos.sh
scp certs/keytabs/<NAME>.keytab root@<source-server>:/path/to/
```

### 4. Client config — SCRAM-SHA-256

```properties
bootstrap.servers=<KAFKA_HOST_IP>:9093,<KAFKA_HOST_IP>:9094,<KAFKA_HOST_IP>:9095
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<USERNAME>" password="<PASSWORD>";
ssl.truststore.location=/path/to/kafka.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=/path/to/client.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
```

### 5. Client config — GSSAPI / Kerberos

```properties
bootstrap.servers=<KAFKA_HOST_IP>:9093,<KAFKA_HOST_IP>:9094,<KAFKA_HOST_IP>:9095
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
  useKeyTab=true storeKey=true \
  keyTab="/path/to/<NAME>.keytab" \
  principal="<NAME>@KAFKA.LOCAL";
ssl.truststore.location=/path/to/kafka.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=<SSL_KEYSTORE_PASSWORD>
ssl.keystore.location=/path/to/client.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
```

Copy SSL files to source server:

```bash
scp certs/client/kafka.truststore.p12 root@<source-server>:/path/to/
scp certs/client/client.keystore.p12  root@<source-server>:/path/to/
```

---

## Full Initial Setup

```bash
chmod +x quick-start.sh scripts/*.sh
./quick-start.sh
```

Manual step-by-step:

```bash
bash scripts/generate-certs.sh
docker compose up -d
sleep 10
bash scripts/setup-kerberos.sh
docker compose restart kafka1 kafka2 kafka3
sleep 20
bash scripts/create-scram-users.sh
bash scripts/create-topics.sh
```
