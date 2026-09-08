# Kafka SASL/SSL Lab

![Confluent Platform](https://img.shields.io/badge/Confluent%20Platform-8.2.0-0073ec)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2-black)
![KRaft](https://img.shields.io/badge/mode-KRaft%20(no%20ZooKeeper)-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A fully containerized 3-broker **Confluent Platform 8.2.0 / Apache Kafka 4.2** cluster in **KRaft mode** (no ZooKeeper), pre-wired with every major Kafka security mechanism side by side: **mutual TLS**, **SASL/SCRAM-SHA-256/512**, and **Kerberos (GSSAPI)** — plus Schema Registry, ksqlDB, and Kafka Connect.

Built as a reference lab for testing CDC / streaming clients against every supported Kafka auth mode without standing up separate clusters.

> **This lab is for development and testing only.** Do not use it in production.

---

## Why this lab

Most Kafka security tutorials pick *one* auth mechanism. Real integrations (mainframe CDC tools, legacy clients, modern apps) often need to be tested against several at once. This lab exposes **four listeners side by side** on one cluster so you can validate a client against SASL_SSL, plain SSL, Kerberos, or no auth — without rebuilding anything.

## Architecture

This lab runs all three brokers as Docker containers on one host. Each broker listens on its own set of ports on that same host. The brokers are not three separate servers — one machine runs all three, plus every other service below.

| Component | Role |
|---|---|
| `kafka1`, `kafka2`, `kafka3` | Brokers + KRaft controller quorum (kafka1 is controller) |
| `schema-registry` | Avro/JSON schema management |
| `ksqldb` | Stream processing (persistent queries/streams) |
| `kafka-connect` | Connector framework (REST on `8083`) |
| `kdc` | MIT Kerberos KDC (realm `KAFKA.LOCAL`) for GSSAPI auth |

### Listener matrix

All three brokers run on one host. Each column below is one broker's port on that same host, not three separate machines.

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

- Docker and Docker Compose v2
- Bash and OpenSSL, for certificate generation
- 6 GB or more of free RAM

The container memory limits alone total about 4 GB: three brokers at 768 MB each, Kafka Connect at 768 MB, ksqlDB at 512 MB, Schema Registry at 256 MB, and the KDC at 256 MB. Leave extra room for the host OS and Docker itself. Check free RAM before you start:

```bash
free -h
```

## Quick start

```bash
git clone https://github.com/farhanjml/kafka-sasl-ssl-lab.git
cd kafka-sasl-ssl-lab
chmod +x quick-start.sh scripts/*.sh

# 1. Set your host IP and passwords
./scripts/configure.sh

# 2. Deploy the cluster
./quick-start.sh
```

### Step 1 — `scripts/configure.sh`

This script prepares the repository for your host. Run it once.

It does four things:

1. Detects your host's IP address. It asks you to confirm or enter a different one.
2. Generates a random password for each SCRAM user, the SSL keystores, and the Kerberos KDC.
3. Writes the IP address and passwords into `docker-compose.yml` and `scripts/*.sh`.
4. Saves every value to `.env` so you can look them up later. Git ignores this file — it never leaves your machine.

The script refuses to run twice. A second run would create passwords that do not match certificates from the first run. To start over, delete the generated files first:

```bash
rm -rf certs .env
docker compose down -v
./scripts/configure.sh --force
```

### Step 2 — `quick-start.sh`

This script builds the cluster. It runs six steps, in order, and prints progress for each:

1. Generates SSL certificates.
2. Starts all containers.
3. Sets up the Kerberos KDC.
4. Restarts the brokers so they load the new keytabs.
5. Creates the SCRAM users.
6. Creates the topics.

The first run pulls the `cp-kafka:8.2.0` image (about 1.3 GB) and takes a few minutes. You can run it again safely — each step checks its own state first. When it finishes, it prints every broker address, port, and credential to the terminal.

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
└── docs/                     # Detailed setup, ops, and troubleshooting docs
```

`configure.sh` creates `certs/` and `.env`. Git ignores both — see below.

## Security and secrets

1. `scripts/generate-certs.sh` creates a new certificate authority and new keystores every time you run it. Never reuse certificates from another environment.
2. Never commit `certs/`, `*.key`, `*.p12`, `*.jks`, `*.keytab`, or `.env`. `.gitignore` already excludes them.
3. Rotate every SCRAM password with `manage/manage-scram.sh` before you expose any port beyond localhost.

## License

MIT — see [LICENSE](LICENSE).





