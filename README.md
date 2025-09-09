# video-analytics-sim — infra/initial

This milestone spins up core infra: Redpanda (Kafka API), ClickHouse, MinIO and Superset.

To run:
1. `docker-compose up -d`
2. Wait ~10-30s for services to become ready.
3. Create ClickHouse table:
   - find the clickhouse container name: `docker ps`
   - run: `docker exec -i <clickhouse_container_name> clickhouse-client --queries-file analytics/clickhouse-ddl.sql`

Ports:
- Redpanda (Kafka): 9092
- ClickHouse HTTP: 8123
- Superset UI: 8088
- MinIO: 9001