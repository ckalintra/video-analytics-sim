# video-analytics-sim — infra/initial

This milestone spins up core infra: Redpanda (Kafka API), ClickHouse, MinIO and Superset.

To run:
1. `docker-compose up -d`
2. Wait ~10-30s for services to become ready.
3. Install dependencies

cd services/user-simulator
npm install

cd ../streaming-server
npm install

cd ../orchestrator
npm install

4. run services

cd services/user-simulator
npm start

cd services/streaming-server
npm start

cd services/orchestrator
npm start


Ports:
- Redpanda (Kafka): 9092
- ClickHouse HTTP: 8123
- Superset UI: 8088
- MinIO: 9001