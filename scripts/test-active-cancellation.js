const http = require('http');

const BASE_URL = 'http://localhost:3000';

function makeRequest(method, path, data = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BASE_URL);
    const payload = data ? JSON.stringify(data) : null;

    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: method,
      headers: {
        'Accept': 'application/json',
      },
    };

    if (payload) {
      options.headers['Content-Type'] = 'application/json';
      options.headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = http.request(options, (res) => {
      let bodyText = '';
      res.on('data', (chunk) => {
        bodyText += chunk;
      });
      res.on('end', () => {
        let parsed = null;
        try {
          if (bodyText) {
            parsed = JSON.parse(bodyText);
          }
        } catch (e) {
          // Keep parsed as null if JSON parsing fails
        }
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          rawBody: bodyText,
          body: parsed,
        });
      });
    });

    req.on('error', (err) => {
      reject(err);
    });

    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runTest() {
  console.log('====================================================');
  console.log('Orchestrix Phase 2 Active Cancellation Test Script');
  console.log('====================================================\n');

  // Step 1: Submit Job
  console.log('[1/5] Submitting test job with task "simulate-long-job"...');
  let submitRes;
  try {
    submitRes = await makeRequest('POST', '/jobs', { task: 'simulate-long-job' });
  } catch (err) {
    console.error('❌ FAILED: Could not connect to API server at http://localhost:3000:', err.message);
    process.exit(1);
  }

  console.log(`      HTTP Status: ${submitRes.statusCode}`);
  console.log(`      Response: ${submitRes.rawBody}`);

  if (submitRes.statusCode !== 201 || !submitRes.body || !submitRes.body.jobId) {
    console.error('❌ FAILED: Job creation did not return 201 Created or is missing jobId.');
    process.exit(1);
  }

  const jobId = submitRes.body.jobId;
  console.log(`      ✅ Job created successfully. Job ID: ${jobId}\n`);

  // Step 2: Poll until job is active/running
  console.log(`[2/5] Polling GET /jobs/${jobId} until status is active/running (max 10s)...`);
  const pollStart = Date.now();
  let isActive = false;
  let pollRes = null;

  while (Date.now() - pollStart < 10000) {
    try {
      pollRes = await makeRequest('GET', `/jobs/${jobId}`);
      if (pollRes.statusCode === 200 && pollRes.body) {
        const state = pollRes.body.state;
        const dbStatus = pollRes.body.dbStatus;

        console.log(`      Poll status -> BullMQ State: "${state}", DB Status: "${dbStatus}"`);

        if (state === 'active' || dbStatus === 'running') {
          isActive = true;
          break;
        }

        if (state === 'completed' || dbStatus === 'completed' || state === 'failed' || dbStatus === 'failed') {
          console.error(`❌ FAILED: Job reached terminal state '${state || dbStatus}' before cancellation request could be sent.`);
          process.exit(1);
        }
      }
    } catch (err) {
      console.warn(`      Warning polling status: ${err.message}`);
    }
    await delay(200);
  }

  if (!isActive) {
    console.error(`❌ FAILED: Job ${jobId} did not become active within 10 seconds.`);
    process.exit(1);
  }

  console.log(`      ✅ Job ${jobId} is actively running!\n`);

  // Step 3: Trigger Active Job Cancellation
  console.log(`[3/5] Immediately sending POST /jobs/${jobId}/cancel...`);
  let cancelRes;
  try {
    cancelRes = await makeRequest('POST', `/jobs/${jobId}/cancel`, {
      reason: 'Automated active cancellation test',
      cancelledBy: 'test_admin',
    });
  } catch (err) {
    console.error(`❌ FAILED: Request to cancel job ${jobId} failed:`, err.message);
    process.exit(1);
  }

  console.log(`      HTTP Status: ${cancelRes.statusCode}`);
  console.log(`      Response: ${cancelRes.rawBody}\n`);

  // Step 4: Poll to verify DB status is cancelled
  console.log(`[4/5] Polling GET /jobs/${jobId} to verify final PostgreSQL status is cancelled...`);
  const verifyStart = Date.now();
  let isCancelled = false;
  let finalStatusRes = null;

  while (Date.now() - verifyStart < 5000) {
    try {
      finalStatusRes = await makeRequest('GET', `/jobs/${jobId}`);
      if (finalStatusRes.statusCode === 200 && finalStatusRes.body) {
        const dbStatus = finalStatusRes.body.dbStatus;
        const state = finalStatusRes.body.state;

        if (dbStatus === 'cancelled' || state === 'cancelled') {
          isCancelled = true;
          break;
        }
      }
    } catch (err) {
      console.warn(`      Warning polling final status: ${err.message}`);
    }
    await delay(200);
  }

  // Step 5: Supplementary Failed/DLQ Check
  console.log('[5/5] Fetching supplementary DLQ information from GET /jobs-status/failed...');
  try {
    const failedRes = await makeRequest('GET', '/jobs-status/failed');
    if (failedRes.statusCode === 200 && failedRes.body) {
      console.log(`      Failed Jobs Count in Queue: ${failedRes.body.count || 0}`);
    } else {
      console.log(`      Supplementary DLQ info status: ${failedRes.statusCode}`);
    }
  } catch (err) {
    console.log(`      Supplementary DLQ info unavailable: ${err.message}`);
  }

  console.log('\n====================================================');
  if (cancelRes.statusCode === 200 && isCancelled) {
    console.log(`🎉 TEST RESULT: PASS`);
    console.log(`   Job ${jobId} was actively halted mid-execution.`);
    console.log(`   Final DB Status: "cancelled"`);
    console.log('====================================================\n');
    process.exit(0);
  } else {
    console.error(`❌ TEST RESULT: FAIL`);
    console.error(`   Cancel HTTP Code: ${cancelRes.statusCode} (Expected 200)`);
    console.error(`   Is DB Status Cancelled: ${isCancelled}`);
    console.log('====================================================\n');
    process.exit(1);
  }
}

runTest();
