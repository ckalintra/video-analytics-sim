const { Kafka } = require('kafkajs');
const express = require('express');
const { randomUUID } = require('crypto');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.EVENT_TOPIC || 'events';

const kafka = new Kafka({ clientId: 'user-sim', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

async function start() {
  await producer.connect();
  const app = express();
  app.use(express.json());

  app.post('/addOneUser', async (req, res) => {
    const userId = `user-${randomUUID()}`;
    runUserSession(userId).catch(console.error);
    res.json({ ok: true, userId });
  });

  app.get('/health', (req, res) => res.send('ok'));

  const port = process.env.PORT || 3001;
  app.listen(port, () => console.log(`user-sim listening ${port}`));
}

async function runUserSession(userId) {
  const sessionId = `sess-${randomUUID()}`;
  let pos = 0;
  const videoId = `video-${Math.floor(Math.random() * 1000)}`;

  await produceEvent({ source: 'client', source_id: userId, event_type: 'page_view', video_id: videoId, session_id: sessionId, meta: { ua: 'user-sim' } });

  await produceEvent({ source: 'client', source_id: userId, event_type: 'video_play', video_id: videoId, playback_pos: pos, session_id: sessionId });

  const durationMs = 1000 * (10 + Math.floor(Math.random() * 30)); // 10..40s

  const hb = setInterval(async () => {
    pos += 10;
    await produceEvent({ source: 'client', source_id: userId, event_type: 'heartbeat', video_id: videoId, playback_pos: pos, session_id: sessionId });
  }, 10000);

  setTimeout(async () => {
    clearInterval(hb);
    await produceEvent({ source: 'client', source_id: userId, event_type: 'video_pause', video_id: videoId, playback_pos: pos, session_id: sessionId });
    await produceEvent({ source: 'client', source_id: userId, event_type: 'page_unload', video_id: videoId, playback_pos: pos, session_id: sessionId });
  }, durationMs);
}

async function produceEvent(payload) {
  const envelope = { event_id: randomUUID(), timestamp: new Date().toISOString(), ...payload, err_score: 0.0 };
  try {
    await producer.send({ topic: TOPIC, messages: [{ value: JSON.stringify(envelope) }] });
    console.log('produced', envelope.event_type, envelope.source_id);
  } catch (err) {
    console.error('produce failed', err);
  }
}

start().catch(err => { console.error(err); process.exit(1); });