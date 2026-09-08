# Kafka SASL/SSL Lab

![Confluent Platform](https://img.shields.io/badge/Confluent%20Platform-8.2.0-0073ec)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2-black)
![KRaft](https://img.shields.io/badge/mode-KRaft%20(no%20ZooKeeper)-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A fully containerized 3-broker **Confluent Platform 8.2.0 / Apache Kafka 4.2** cluster in **KRaft mode** (no ZooKeeper), pre-wired with every major Kafka security mechanism side by side: **mutual TLS**, **SASL/SCRAM-SHA-256/512**, and **Kerberos (GSSAPI)** — plus Schema Registry, ksqlDB, and Kafka Connect.

Built as a reference lab for testing CDC / streaming clients against every supported Kafka auth mode without standing up separate clusters.

---

## Why this lab

Most Kafka security tutorials pick *one* auth mechanism. Real integrations (mainframe CDC tools, legacy clients, modern apps) often need to be tested against several at once. This lab exposes **four listeners side by side** on one cluster so you can validate a client against SASL_SSL, plain SSL, Kerberos, or no auth — without rebuilding anything.

## Architecture

| Component | Role |
|---|---|
| `kafka1`, `kafka2`, `kafka3` | Brokers + KRaft controller quorum (kafka1 is controller) |
| `schema-registry` | Avro/JSON schema management |
| `ksqldb` | Stream processing (persistent queries/streams) |
| `kafka-connect` | Connector framework (REST on `8083`) |
| `kdc` | MIT Kerberos KDC (realm `KAFKA.LOCAL`) for GSSAPI auth |

### Listener matrix

| Listener | Ports (kafka1/2/3) | Auth | Encryption | Use case |
|---|---|---|---|---|
| `SASL_SSL` | `9193 / 9194 / 9195` | SCRAM-SHA-256/512 or GSSAPI | TLS + mTLS | External clients (CDC pipelines, apps) |
| `SSL` | `9197 / 9198 / 9199` | none (cert-only) | TLS + mTLS | Clients without SASL support |
| `PLAINTEXT` | `9192 / 9292 / 9392` | none | none | Local lab testing only |
| `INTERNAL` | docker-only | none | none | Inter-broker traffic |

### Service endpoints

| Service | Port | Notes |
|---|---|---|
| Schema Registry | `8081` | REST at `/subjects`, `/schemas` |
| Kafka Connect | `8083` | REST; `/health`, `/connectors` |
| ksqlDB | `8088` | REST at `/ksql` |
| KDC | `18088` / `18749` | Kerberos realm `KAFKA.LOCAL` |

Full details: [`docs/ENDPOINTS.md`](docs/ENDPOINTS.md).

---

## Prerequisites

- Docker + Docker Compose v2
- Bash, OpenSSL (cert generation)
- **6 GB+ free RAM.** Container memory limits alone total ~4 GB (3 brokers × 768 MB + Kafka Connect 768 MB + ksqlDB 512 MB + Schema Registry 256 MB + KDC 256 MB) — leave headroom for the host OS and Docker itself. Check free RAM first: `free -h`.

## Before you run anything — replace placeholders

This repo ships with placeholder values, not working defaults. `quick-start.sh` will run, but SCRAM auth, TLS, and Kerberos will all use these placeholders unless you replace them first:

| File | Placeholder(s) | Replace with |
|---|---|---|
| `docker-compose.yml` | `<KAFKA_HOST_IP>` (all occurrences) | Your host's real IP — used in cert SANs and advertised listeners |
| `docker-compose.yml` | `<ADMIN_SCRAM_PASSWORD>` (in each broker's `SASL_JAAS_CONFIG`) | A strong password for the `admin` SCRAM user |
| `scripts/create-scram-users.sh` | `<ADMIN_SCRAM_PASSWORD>`, `<APP_SCRAM_PASSWORD>`, `<DEMO_SCRAM_PASSWORD>` | Match whatever you set above for `admin`; pick your own for the other two users |
| `scripts/generate-certs.sh` | `<SSL_KEYSTORE_PASSWORD>` | A password for the generated keystores/truststores |
| `scripts/setup-kerberos.sh` | `<KDC_MASTER_PASSWORD>` | A password for the Kerberos KDC database |

Quickest way to check you got them all:

```bash
grep -rn '<[A-Z_]*>' docker-compose.yml scripts/*.sh
```

(should return nothing once every placeholder is replaced)

## Quick start

```bash
git clone https://github.com/farhanjml/kafka-sasl-ssl-lab.git
cd kafka-sasl-ssl-lab

# 1. Replace placeholders — see table above
# 2. Make scripts executable
chmod +x quick-start.sh scripts/*.sh

# 3. Deploy
./quick-start.sh
```

`quick-start.sh` runs 6 steps automatically, in order, printing progress for each:

1. Generate SSL certificates (`scripts/generate-certs.sh`)
2. Start all containers (`docker compose up -d`)
3. Set up the Kerberos KDC (`scripts/setup-kerberos.sh`)
4. Restart brokers so they pick up fresh keytabs
5. Create SCRAM users (`scripts/create-scram-users.sh`)
6. Create topics (`scripts/create-topics.sh`)

Takes a few minutes on first run (pulls the `cp-kafka:8.2.0` image, ~1.3 GB). Safe to re-run — every step is idempotent. When it finishes, it prints all broker addresses, ports, and credentials to the terminal.

## Managing the cluster

| Script | Purpose |
|---|---|
| `manage/manage-scram.sh` | Create/update/delete SASL SCRAM users interactively |
| `manage/manage-kerberos.sh` | Kerberos keytab and principal management |
| `manage/manage-topic.sh` | Topic creation with client config examples for every listener |
| `scripts/check-versions.sh` | Verify broker/component versions match expectations |

## Documentation

| Doc | Contents |
|---|---|
| [`docs/SETUP.md`](docs/SETUP.md) | Full architecture, security stack, component deep-dives |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Day-to-day ops: SCRAM users, topics, Kerberos routine, CDC onboarding |
| [`docs/KERBEROS.md`](docs/KERBEROS.md) | KDC recovery, keytab rotation, troubleshooting |
| [`docs/CDC-CLIENT.md`](docs/CDC-CLIENT.md) | Example CDC client config (SCRAM + Kerberos) and a transaction-timeout fix |
| [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) | Known issues, root causes, fixes |
| [`docs/PATCHES.md`](docs/PATCHES.md) | Patch history |

## Repo structure

```
.
├── docker-compose.yml       # Full stack: brokers, SR, ksqlDB, Connect, KDC
├── quick-start.sh           # One-shot deploy script
├── config/                  # Client properties for internal (no-auth) access
├── kerberos/                # KDC Dockerfile + krb5.conf
├── manage/                  # Interactive admin scripts (SCRAM, Kerberos, topics)
├── scripts/                 # Cert generation, user/topic bootstrap, version checks
└── docs/             # Detailed setup, ops, and troubleshooting docs
```

`certs/` (generated keys/keystores) and `.env` are **not** committed — see below.

## Security & secrets

1. **Generate your own certs** — `scripts/generate-certs.sh` creates a fresh CA and all broker/client keystores; never reuse certs from another environment.
2. **Never commit**: `certs/`, `*.key`, `*.p12`, `*.jks`, `*.keytab`, `.env` — all excluded via `.gitignore`.
3. Rotate all SCRAM passwords via `manage/manage-scram.sh` before exposing any port beyond localhost.

## License

MIT — see [LICENSE](LICENSE).


