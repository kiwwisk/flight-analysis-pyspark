# flight-analysis-pyspark

## Preparing test environment

In the command line start testing environment (pyspark master, worker, postgresql and pgadmin) with this command:

```bash
docker compose up -d
```

Opened ports on `localhost`:

- `8080` - spark master web interface
- `7077` - spark port for workers
- `8081` - spark worker 1
- `15432` - pgadmin web interface

To connect to pgadmin web interface:

user: `admin@pgadmin.com`
password: `password`

In the pgadmin you can connect to `postgres:5432` PostgreSQL server.

** Make sure you create database `zadanie` in PostgreSQL instance! **

## Building image with pyspark tasks

```bash
docker build -t pyspark-demo/ake:1.0 .
```

This build the image based on `python:3.8-slim` with `pyspark`, `OpenJDK` and `postgresql` JDBC driver.

To run the image type from the project dir:

```bash
docker run -it --rm --network="valllue-zadanie_default" pyspark-demo/ake:1.0
```

---

Upon running you should see:

```none

+----------------+--------------+
|departure delays|arrival delays|
+----------------+--------------+
|7               |6             |
+----------------+--------------+

+----------------------------------------------------------------+---------+-----------------------+-----------------+------------+
|statuskey                                                       |state    |updatedat              |aircraftregnumber|flightnumber|
+----------------------------------------------------------------+---------+-----------------------+-----------------+------------+
|2dabdd879800b7f30b3f3eef8c2f56dbae24eb542399b9d15d22088b58d4ae14|Landed   |2023-10-03T13:41:22.078|NULL             |476         |
|af89e25495692a0bcd5e742c5239c71b6e74985bd3ef936ea2827bb0e9e1a4ef|InGate   |2023-10-03T02:01:27.224|JA314J           |476         |
|9e34394abe83142c5a8000197516822dc32bad97ea81ae508195940af0765961|Landed   |2023-10-03T10:13:34.644|N342UP           |476         |
|3d7496b198a37b5401841876ac9eabdacfa065f316417d68480f832c008f82d9|InGate   |2023-10-03T14:35:46.538|N826NN           |476         |
|fa3e07f019fe3dd03dc7ba02aac85bc874d023179f58fddc659accd8d0a9a9c2|InGate   |2023-10-03T09:17:16.239|GEUYE            |476         |
|55a23dce3c3aa9976b6c70ce1554a88e075d64380c6891ca933a585a0593e187|InGate   |2023-10-03T01:12:15.986|A7LAF            |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|Invented |2023-10-02T20:26:29.057|HL7534           |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|Invented |2023-10-02T20:26:29.057|HL7534           |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|Scheduled|2023-10-02T21:26:29.057|HL7534           |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|Scheduled|2023-10-02T21:26:29.057|HL7534           |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|InGate   |2023-10-02T22:26:29.057|HL7534           |476         |
|ad7e703e763f73a7c5f807140755e8626ebba7f76046415b4789d497215bea39|InGate   |2023-10-02T22:26:29.057|HL7534           |476         |
|224b0ebdf6f19879160010c86435ebf0ae6d42a05f62b6c9c8fbe36bfb202352|InGate   |2023-10-03T16:00:54.026|9YJAM            |476         |
|c28e5c5bffc8a86d3a1aae702915ca39899e91d2ee131d644e65146364cb30e7|Landed   |2023-10-03T13:04:49.056|N271FE           |476         |
|c1bbb6b086a6d38a0a97c39ae177ddb512719bf1e6e4f304dbba3bb13a29736a|InGate   |2023-10-03T10:45:53.021|EIDVL            |476         |
+----------------------------------------------------------------+---------+-----------------------+-----------------+------------+
```

