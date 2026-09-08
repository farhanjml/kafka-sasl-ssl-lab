# Kafka SASL/SSL Lab

![Confluent Platform](https://img.shields.io/badge/Confluent%20Platform-8.2.0-0073ec)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2-black)
![KRaft](https://img.shields.io/badge/mode-KRaft%20(no%20ZooKeeper)-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A fully containerized 3-broker **Confluent Platform 8.2.0 / Apache Kafka 4.2** cluster in **KRaft mode** (no ZooKeeper), pre-wired with every major Kafka security mechanism side by side: **mutual TLS**, **SASL/SCRAM-SHA-256/512**, and **Kerberos (GSSAPI)** — plus Schema Registry, ksqlDB, and Kafka Connect.

Built as a reference lab for testing CDC / streaming clients against every supported Kafka auth mode without standing up separate clusters.

> ⚠️ **This repo is a sanitized template.** All real hostnames, IPs, and passwords from the original deployment have been replaced with placeholders (`<KAFKA_HOST_IP>`, `<ADMIN_SCRAM_PASSWORD>`, etc.) — see [Security](#security--secrets) before using it.

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
- ~2 GB free RAM for the full stack (brokers ~600 MB each, ksqlDB ~500 MB, others ~250–350 MB)

## Quick start

```bash
git clone https://github.com/farhanjml/kafka-sasl-ssl-lab.git
cd kafka-sasl-ssl-lab
chmod +x quick-start.sh scripts/*.sh
./quick-start.sh
```

`quick-start.sh` runs, in order: certificate generation → `docker compose up -d` → Kerberos KDC setup → SCRAM user creation → topic creation → version checks. Safe to re-run — every step is idempotent.

**Before running:** edit `docker-compose.yml` and replace `<KAFKA_HOST_IP>` with your host's real IP (used in cert SANs and advertised listeners).

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

This is a sanitized copy of a working internal deployment. Before use:

1. **Generate your own certs** — `scripts/generate-certs.sh` creates a fresh CA and all broker/client keystores; never reuse certs from another environment.
2. **Set your own passwords** — replace every `<PLACEHOLDER>` in `docker-compose.yml` and `docs/*.md` (SCRAM user passwords, keystore/truststore password, KDC master password).
3. **Never commit**: `certs/`, `*.key`, `*.p12`, `*.jks`, `*.keytab`, `.env` — all excluded via `.gitignore`.
4. Default SCRAM users ship with placeholder passwords only — rotate immediately via `manage/manage-scram.sh` before exposing any port beyond localhost.

## License

MIT — see [LICENSE](LICENSE).
