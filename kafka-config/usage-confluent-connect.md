# Usage Guide: MySQL → Kafka Connect → ksqlDB

Stack: Confluent Platform 7.6.0 | JDBC Source Connector | ksqlDB | Kerberos (SASL_PLAINTEXT)

---

## Prasyarat

Pastikan semua container running:

```bash
docker compose -f confluent-ksql.yml up -d
docker compose -f confluent-ksql.yml ps
```

Semua service harus berstatus `Up` atau `healthy`:
- `kdc` (Kerberos KDC)
- `zookeeper`, `kafka`
- `schema-registry`
- `mysql`
- `kafka-connect`
- `ksqldb-server`, `ksqldb-cli`

---

## Troubleshooting: KDC Gagal Start

Jika container `kdc` exit dengan error `cannot initialize realm` atau `No such file or directory`,
artinya volume KDC corrupt dari run sebelumnya. Solusi:

```bash
docker compose -f confluent-ksql.yml down
docker volume rm confluent_kdc-data confluent_keytabs
docker compose -f confluent-ksql.yml up -d
```

> Volume `confluent_mysql-data` tidak perlu dihapus — data MySQL tetap aman.

---

## Step 1 — Buat Tabel & Data Awal di MySQL

```bash
docker exec -it mysql mysql -u kafka_user -pkafka_pass confluent_db
```

Di dalam MySQL shell:

```sql
CREATE TABLE IF NOT EXISTS orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  product VARCHAR(100),
  qty INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (product, qty) VALUES ('Laptop', 2), ('Mouse', 5);
SELECT * FROM orders;
```

---

## Step 2 — Register MySQL Source Connector

Cek connector yang sudah ada:

```bash
curl -s http://localhost:8083/connectors | python -m json.tool
```

Register connector baru:

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-source",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "connection.url": "jdbc:mysql://mysql:3306/confluent_db",
      "connection.user": "kafka_user",
      "connection.password": "kafka_pass",
      "mode": "incrementing",
      "incrementing.column.name": "id",
      "table.whitelist": "orders",
      "topic.prefix": "mysql.",
      "poll.interval.ms": "3000",
      "numeric.mapping": "best_fit"
    }
  }' | python -m json.tool
```

Cek status connector (tunggu ~5 detik):

```bash
curl -s http://localhost:8083/connectors/mysql-source/status | python -m json.tool
```

Output yang diharapkan:
```json
{
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }]
}
```

> Jika task FAILED karena tabel tidak ditemukan, pastikan tabel sudah dibuat di Step 1
> lalu jalankan: `curl -s -X POST http://localhost:8083/connectors/mysql-source/restart`

Verifikasi topic Kafka sudah terbuat:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list | grep mysql
# Output: mysql.orders
```

---

## Step 3 — Buat Stream di ksqlDB

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Di dalam ksqlDB CLI:

```sql
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM orders_stream (
  id INT,
  product VARCHAR,
  qty INT,
  created_at BIGINT
) WITH (
  KAFKA_TOPIC='mysql.orders',
  VALUE_FORMAT='AVRO'
);
```

---

## Step 4 — Query & Monitor Perubahan

### Lihat data yang sudah ada

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT id, product, qty FROM orders_stream LIMIT 10;
```

### Listen real-time (EMIT CHANGES)

```sql
SELECT id, product, qty FROM orders_stream EMIT CHANGES;
-- Ctrl+C untuk stop
```

Terminal ini akan menampilkan setiap row baru yang masuk dari MySQL secara real-time.

### Test insert baru dari MySQL (di terminal lain)

```bash
docker exec -it mysql mysql -u kafka_user -pkafka_pass confluent_db
```

```sql
INSERT INTO orders (product, qty) VALUES ('Keyboard', 3);
INSERT INTO orders (product, qty) VALUES ('Monitor', 1);
```

Row baru akan muncul di ksqlDB dalam ~3 detik (sesuai `poll.interval.ms`).

---

## Consume Raw dari Kafka

Format binary Avro (mentah — untuk debugging):

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic mysql.orders \
  --from-beginning
```

---

## Referensi Endpoint

| Service         | URL                                  |
|-----------------|--------------------------------------|
| Kafka Connect   | http://localhost:8083                |
| Schema Registry | http://localhost:8081                |
| ksqlDB REST API | http://localhost:8088                |
| MySQL           | localhost:3306 (db: confluent_db)    |

### Perintah cepat Kafka Connect

```bash
# List connectors
curl -s http://localhost:8083/connectors

# Status connector
curl -s http://localhost:8083/connectors/mysql-source/status | python -m json.tool

# Restart connector
curl -s -X POST http://localhost:8083/connectors/mysql-source/restart

# Hapus connector
curl -s -X DELETE http://localhost:8083/connectors/mysql-source

# List plugin yang tersedia
curl -s http://localhost:8083/connector-plugins | python -m json.tool
```

### Perintah cepat ksqlDB CLI

```bash
# Masuk ke CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# Di dalam CLI:
SHOW TOPICS;
SHOW STREAMS;
DESCRIBE orders_stream;
PRINT 'mysql.orders' FROM BEGINNING;
```
