# Endpoints Reference

## Kafka Bootstrap Servers

| Listener | Bootstrap (IP) | Bootstrap (Hostname) |
|----------|----------------|----------------------|
| SASL_SSL | `<KAFKA_HOST_IP>:9193,<KAFKA_HOST_IP>:9194,<KAFKA_HOST_IP>:9195` | `kafka1:9193,kafka2:9194,kafka3:9195` |
| PLAINTEXT | `<KAFKA_HOST_IP>:9192,<KAFKA_HOST_IP>:9292,<KAFKA_HOST_IP>:9392` | `kafka1:9192,kafka2:9292,kafka3:9392` |
| SSL | `<KAFKA_HOST_IP>:9197,<KAFKA_HOST_IP>:9198,<KAFKA_HOST_IP>:9199` | `kafka1:9197,kafka2:9198,kafka3:9199` |

## Service REST APIs

| Service | Protocol | URL (IP) | URL (Hostname) |
|---------|----------|----------|----------------|
| Schema Registry | HTTP | `http://<KAFKA_HOST_IP>:8081` | `http://schema-registry:8081` |
| ksqlDB | HTTP | `http://<KAFKA_HOST_IP>:8088` | `http://ksqldb:8088` |
| Kafka Connect | HTTP | `http://<KAFKA_HOST_IP>:8083` | `http://connect:8083` |

## /etc/hosts (client machines)

```
<KAFKA_HOST_IP>  kafka1 kafka2 kafka3
<KAFKA_HOST_IP>  schema-registry ksqldb connect
```

## Listener Notes

| Listener | Auth | Encryption | Use |
|----------|------|------------|-----|
| SASL_SSL | SCRAM-SHA-256 / GSSAPI | TLS + mTLS | External CDC clients |
| PLAINTEXT | none | none | Lab testing only |
| SSL | none (mTLS only) | TLS + mTLS | SSL-only clients (no SASL support) |
| INTERNAL | none | none | Docker-internal only — not for external clients |
