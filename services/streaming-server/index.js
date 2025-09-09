const { Kafka } = require('kafkajs');
const express = require('express');
const { randomUUID } = require('crypto');
const { createClient } = require('@clickhouse/client');

const clickhouse = createClient({
  host: 'http://localhost:8123',
});

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.EVENT_TOPIC || 'events';
const MAX_CAPACITY = process.env.MAX_CAPACITY ? Number(process.env.MAX_CAPACITY) : 100;

const kafka = new Kafka({ clientId: 'stream-srv', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

const activeStreams = new Map(); // session_id -> {user_id, video_id}

async function start() {
  await producer.connect();
  const app = express();
  app.use(express.json());

  app.post('/startStream', async (req, res) => {
    const { user_id, session_id, video_id } = req.body;
    if (activeStreams.size >= MAX_CAPACITY) return res.status(503).json({ error: 'over capacity' });
    activeStreams.set(session_id, { user_id, video_id, started_at: Date.now() });
    await produce({ source: 'server', source_id: `stream-srv-${process.pid}`, event_type: 'stream_start', session_id, video_id, user_id });
    res.json({ ok: true });
  });

  app.post('/stopStream', async (req, res) => {
    const { session_id } = req.body;
    if (activeStreams.has(session_id)) {
      activeStreams.delete(session_id);
      await produce({ source: 'server', source_id: `stream-srv-${process.pid}`, event_type: 'stream_stop', session_id });
    }
    res.json({ ok: true });
  });

  app.get('/health', (req, res) => res.send('ok'));
  app.listen(process.env.PORT || 4000, () => console.log('streaming server listening'));
}

async function produce(payload) {
  const envelope = { event_id: randomUUID(), timestamp: new Date().toISOString(), ...payload, err_score: 0.0 };
  try {
    await producer.send({ topic: TOPIC, messages: [{ value: JSON.stringify(envelope) }] });
    await logEventToClickhouse(envelope);
    console.log('server-event', envelope.event_type, envelope.session_id || '');
  } catch (err) {
    console.error('server produce failed', err);
  }
}

async function logEventToClickhouse(event) {
  try {
    await clickhouse.insert({
      table: 'events',
      values: [{
        event_time: new Date().toISOString().replace('T', ' ').split('.')[0],
        user_id: event.user_id || 'unknown',
        event_type: event.event_type || 'unknown',
        video_id: event.video_id || 'none',
        server_id: event.source_id || 'stream-srv',
        payload: JSON.stringify(event)
      }],
      format: 'JSONEachRow',
    });
    console.log('[ClickHouse] inserted', event.event_type);
  } catch (err) {
    console.error('[ClickHouse] insert failed:', err.message);
  }
}

start().catch(err => { console.error(err); process.exit(1); });