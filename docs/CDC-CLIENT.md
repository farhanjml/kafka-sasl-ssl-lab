# External CDC Client — Kafka Client Configuration & Troubleshooting

Generic reference for configuring an external CDC / streaming client (e.g. a CDC replication tool) against this Kafka lab, and troubleshooting the most common connection failure (transactional producer timeout).

---

## Broker Connection

| Field | Value |
|-------|-------|
| Bootstrap Servers | `kafka1:9193,kafka2:9194,kafka3:9195` |
| Security Protocol | `SASL_SSL` |
| Confluent Platform | `8.2.0` (Apache Kafka 4.2) |

---

## Topics

| Topic | Partitions | Replication | Purpose |
|-------|-----------|-------------|---------|
| `cdc-transactions` | 6 | 2 | Transaction CDC |
| `cdc-accounts` | 3 | 2 | Account CDC |
| `cdc-customers` | 3 | 2 | Customer CDC |

Rename these in `scripts/create-topics.sh` to match your actual source tables/topics.

---

## Option A — SCRAM-SHA-256

### SASL Settings

| Field | Value |
|-------|-------|
| SASL Mechanism | `SCRAM-SHA-256` |
| Username | `app-client` |
| Password | `<APP_SCRAM_PASSWORD>` |

### Properties File

```properties
bootstrap.servers=kafka1:9193,kafka2:9194,kafka3:9195
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="app-client" \
  password="<APP_SCRAM_PASSWORD>";

ssl.ca.location=/path/on/client/host/ca-cert.crt
ssl.certificate.location=/path/on/client/host/client.crt
ssl.key.location=/path/on/client/host/client.key
ssl.key.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
```

---

## Option B — GSSAPI / Kerberos

### SASL Settings

| Field | Value |
|-------|-------|
| SASL Mechanism | `GSSAPI` |
| Kerberos Service Name | `kafka` |
| Principal | `kafkaclient@KAFKA.LOCAL` |
| Keytab File | `/path/on/client/host/kafkaclient.keytab` |
| Realm | `KAFKA.LOCAL` |
| KDC | `kdc` |

### Properties File

```properties
bootstrap.servers=kafka1:9193,kafka2:9194,kafka3:9195
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
  useKeyTab=true \
  storeKey=true \
  keyTab="/path/on/client/host/kafkaclient.keytab" \
  principal="kafkaclient@KAFKA.LOCAL";

ssl.ca.location=/path/on/client/host/ca-cert.crt
ssl.certificate.location=/path/on/client/host/client.crt
ssl.key.location=/path/on/client/host/client.key
ssl.key.password=<SSL_KEYSTORE_PASSWORD>
ssl.endpoint.identification.algorithm=
```

---

## Files to Copy to the Client Host

Run from the Kafka host after cert regeneration — copy to wherever your client expects its cert bundle:

```bash
scp certs/ca/ca-cert.crt \
    certs/client/client.crt \
    certs/client/client.key \
    root@<CDC_CLIENT_HOST_IP>:/path/on/client/host/

# Kerberos keytab (also auto-copied locally by setup-kerberos.sh)
scp certs/keytabs/kafkaclient.keytab \
  root@<CDC_CLIENT_HOST_IP>:/path/on/client/host/
```

---

## DNS — Required on the Client Host

```
<KAFKA_HOST_IP>  kafka1
<KAFKA_HOST_IP>  kafka2
<KAFKA_HOST_IP>  kafka3
<KAFKA_HOST_IP>  kdc
```

Copy `krb5.conf` for Kerberos (Option B):

```bash
scp kerberos/krb5.conf root@<CDC_CLIENT_HOST_IP>:/etc/krb5.conf
```

---

## SCRAM Users

| Username | Password | Purpose |
|----------|----------|---------|
| `admin` | `<ADMIN_SCRAM_PASSWORD>` | Cluster administration |
| `app-client` | `<APP_SCRAM_PASSWORD>` | External CDC pipeline |
| `demo-user` | `<DEMO_SCRAM_PASSWORD>` | General testing |

---

## Transaction Initialization Timeout (RC=129-style errors)

### Symptom

A transactional producer client reports something like:

```
Kafka failed to initialize transactions:
Timed out waiting for operation to finish, retry call to resume.
```

### Root Cause

Transactional producer clients call `initTransactions()` before the first record, which contacts the Transaction Coordinator. The coordinator stores state in `__transaction_state`. If that topic doesn't exist, auto-creation involves electing leaders across 50 partitions — this takes 10–30 seconds and commonly exceeds a client's connect deadline.

> `auto.create.topics.enable=false` in this lab means `__transaction_state` is never auto-created. `scripts/create-topics.sh` pre-creates it. Only needed again if `__transaction_state` was deleted.

### Fix — Pre-create `__transaction_state`

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --create \
  --topic __transaction_state \
  --partitions 50 \
  --replication-factor 2 \
  --config cleanup.policy=compact \
  --config compression.type=producer \
  --config segment.bytes=104857600 \
  --config min.cleanable.dirty.ratio=0.005 \
  --if-not-exists
```

### Verify the topic is healthy

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --describe --topic __transaction_state 2>&1 | head -5
```

All partitions should show `Isr` matching `Replicas`.

### Other Causes

| Symptom | Check | Fix |
|---------|-------|-----|
| `__transaction_state` missing | `kafka-topics --list` | Pre-create with command above |
| Topic exists but still times out | Client producer properties | Add `transaction.timeout.ms=120000` |
| SSL handshake errors | `openssl x509 -dates` on certs | Regenerate with `generate-certs.sh`, push new certs to the client |
| Kerberos `CLIENT_NOT_FOUND` | `klist` on client host | Re-export keytab — see [`KERBEROS.md`](KERBEROS.md) |
| Broker unreachable | `ping` + `/etc/hosts` | Add DNS entries to the client's `/etc/hosts` |

```properties
# Add to the client's Kafka producer config if timeout persists
transaction.timeout.ms=120000
request.timeout.ms=120000
```

### Diagnose

```bash
# 1. Check if topic exists
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  --list | grep __transaction_state

# 2. Check broker transaction logs
docker logs kafka1 2>&1 | grep -i "transaction" | tail -30

# 3. Verify all brokers are healthy
docker exec kafka1 kafka-broker-api-versions \
  --bootstrap-server kafka1:19093 \
  --command-config /etc/kafka/client-internal.properties \
  2>&1 | grep "^kafka"
```
