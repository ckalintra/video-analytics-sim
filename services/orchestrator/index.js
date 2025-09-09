const express = require('express');
const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.EVENT_TOPIC || 'events';

const kafka = new Kafka({ clientId: 'orchestrator', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

//default vals
let config = {
  waveHeight: 5,
  waveWidth: 60,
  errorFactor: 0
};

let tick = 0;
let servers = [{ id: 1, load: 0 }];
const MAX_SERVER_CAPACITY = 100;

const USER_SIM_URL = 'http://localhost:3001/addOneUser';
const STREAM_SERVER_BASE_PORT = 4000;

const axios = require('axios');

async function start() {
  await producer.connect();

  const app = express();
  app.use(express.json());

  app.get('/config', (req, res) => res.json(config));

  app.post('/config', (req, res) => {
    const { waveHeight, waveWidth, errorFactor } = req.body;
    if (waveHeight !== undefined) config.waveHeight = Number(waveHeight);
    if (waveWidth !== undefined) config.waveWidth = Number(waveWidth);
    if (errorFactor !== undefined) config.errorFactor = Number(errorFactor);
    res.json({ ok: true, config });
  });

  app.get('/servers', (req, res) => res.json(servers));

  app.listen(5000, () => console.log('orchestrator running on :5000'));

  setInterval(mainLoop, 1000);
}

async function mainLoop() {
  tick++;
  const sine = Math.sin((2 * Math.PI * tick) / config.waveWidth);
  const usersToSpawn = Math.floor(((sine + 1) / 2) * config.waveHeight);

  for (let i = 0; i < usersToSpawn; i++) {
    spawnUser();
  }

  autoScaleCheck();
}

async function spawnUser() {
  try {
    const resp = await axios.post(USER_SIM_URL);
    const userId = resp.data.userId;

    const srv = servers.reduce((a, b) => (a.load <= b.load ? a : b));
    srv.load++;
    await produceEvent({
      source: 'orchestrator',
      source_id: 'orch-1',
      event_type: 'user_spawn',
      user_id: userId,
      server_id: srv.id
    });
  } catch (err) {
    console.error('spawn user failed', err.message);
  }
}

function autoScaleCheck() {
  for (const srv of servers) {
    if (srv.load >= MAX_SERVER_CAPACITY * 0.8) {
      const newId = servers.length + 1;
      servers.push({ id: newId, load: 0 });
      produceEvent({
        source: 'orchestrator',
        source_id: 'orch-1',
        event_type: 'scale_up',
        server_id: newId
      });
      console.log('scaled up: new server', newId);
      break;
    }
  }
}

async function produceEvent(payload) {
  const envelope = {
    event_id: randomUUID(),
    timestamp: new Date().toISOString(),
    ...payload,
    err_score: config.errorFactor
  };
  try {
    await producer.send({ topic: TOPIC, messages: [{ value: JSON.stringify(envelope) }] });
    console.log('orch-event', envelope.event_type);
  } catch (err) {
    console.error('produce event failed', err.message);
  }
}

start().catch(err => {
  console.error(err);
  process.exit(1);
});
