const { Kafka } = require('kafkajs');
const axios = require('axios');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.EVENT_TOPIC || 'events';
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || 'http://localhost:8123/?query=INSERT%20INTO%20analytics.events_raw%20FORMAT%20JSONEachRow';

const kafka = new Kafka({ clientId: 'kch-ingest', brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'clickhouse-ingesters' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const v = message.value.toString();
        const obj = JSON.parse(v);
        // ensure meta is a string
        obj.meta = typeof obj.meta === 'string' ? obj.meta : JSON.stringify(obj.meta || {});
        // Post single JSON row (ClickHouse JSONEachRow accepts newline separated JSON rows)
        await axios.post(CLICKHOUSE_URL, JSON.stringify(obj), { headers: { 'Content-Type': 'application/json' } });
        console.log('inserted', obj.event_type, obj.event_id);
      } catch (err) {
        console.error('insert failed', err.message || err);
      }
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });