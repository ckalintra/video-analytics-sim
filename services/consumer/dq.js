const { exec } = require('child_process');

function runQuery(q) {
  return new Promise((resolve, reject) => {
    exec(
      `curl -s 'http://localhost:8123/?query=${encodeURIComponent(q)}'`,
      (err, stdout) => {
        if (err) reject(err);
        else resolve(stdout.trim());
      }
    );
  });
}

async function runDQ() {
  console.log('Running Data Quality Checks...');
  const total = await runQuery('SELECT count() FROM analytics.events_raw');
  const missingUser = await runQuery(
    'SELECT count() FROM analytics.events_raw WHERE user_id = \'\' OR user_id IS NULL'
  );
  const errorEvents = await runQuery(
    'SELECT count() FROM analytics.events_raw WHERE err_score > 0'
  );

  console.log(`Total events: ${total}`);
  console.log(`Missing user_id: ${missingUser}`);
  console.log(`Events with errorFactor: ${errorEvents}`);
}

runDQ().catch(console.error);