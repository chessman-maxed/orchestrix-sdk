/**
 * Orchestrix SDK Integration Test
 * 
 * Tests the SDK against a live Orchestrix API server running at http://localhost:3000.
 * Imports directly from the compiled ./dist build output.
 */

const {
  OrchestrixClient,
  OrchestrixNotFoundError,
  OrchestrixValidationError,
  OrchestrixApiError,
  OrchestrixConnectionError,
} = require('./dist');

async function runIntegrationTest() {
  const baseUrl = process.env.ORCHESTRIX_URL || 'http://localhost:3000';
  console.log(`\n==================================================`);
  console.log(`🚀 Starting Orchestrix SDK Integration Test`);
  console.log(`   Target Server: ${baseUrl}`);
  console.log(`   Importing SDK from: ./dist`);
  console.log(`==================================================\n`);

  const client = new OrchestrixClient({ baseUrl, timeoutMs: 10000 });

  let createdJobId = null;

  // Test Step 1: Create a Job
  try {
    console.log('🔹 Step 1: Testing client.jobs.create()...');
    const createPayload = {
      message: 'Integration test job payload',
      timestamp: Date.now(),
      environment: 'test',
    };

    const createOptions = {
      priority: 3,
      delay: 0,
    };

    const createResult = await client.jobs.create(createPayload, createOptions);

    console.log('   ✅ Job Creation Response received:');
    console.log(`      - Message: ${createResult.message}`);
    console.log(`      - Job ID: ${createResult.jobId}`);
    console.log(`      - Priority: ${createResult.priority}`);
    console.log(`      - Delay: ${createResult.delay}`);
    console.log(`      - Is Duplicate: ${createResult.isDuplicate}`);

    if (!createResult.jobId) {
      throw new Error('❌ Verification failed: jobs.create() did not return a valid jobId');
    }

    createdJobId = createResult.jobId;
    console.log('   ✅ Verification Passed: jobs.create() returned a valid jobId.\n');
  } catch (err) {
    console.error('❌ Step 1 Failed (Job Creation Error):', err);
    throw err;
  }

  // Test Step 2: Get Job Details
  try {
    console.log(`🔹 Step 2: Testing client.jobs.get("${createdJobId}")...`);
    const jobDetails = await client.jobs.get(createdJobId);

    console.log('   ✅ Job Details retrieved:');
    console.log(`      - ID: ${jobDetails.id}`);
    console.log(`      - State: ${jobDetails.state}`);
    console.log(`      - DB Status: ${jobDetails.dbStatus}`);
    console.log(`      - Priority: ${jobDetails.priority}`);
    console.log(`      - Delay: ${jobDetails.delay}`);
    console.log(`      - Attempts Made: ${jobDetails.attemptsMade}`);
    console.log(`      - Max Attempts: ${jobDetails.maxAttempts}`);
    console.log(`      - Data:`, jobDetails.data);
    console.log(`      - Progress: ${jobDetails.progress}`);

    // Field & Match Verifications
    if (jobDetails.id !== createdJobId) {
      throw new Error(`❌ Verification failed: Retrieved job ID (${jobDetails.id}) does not match created job ID (${createdJobId})`);
    }

    const requiredFields = ['id', 'state', 'priority', 'delay', 'attemptsMade', 'maxAttempts', 'data', 'progress'];
    const missingFields = requiredFields.filter((field) => !(field in jobDetails));
    if (missingFields.length > 0) {
      throw new Error(`❌ Verification failed: Retrieved job is missing required fields: ${missingFields.join(', ')}`);
    }

    console.log('   ✅ Verification Passed: Retrieved job ID matches created ID & contains all expected fields.');
    console.log(`   ℹ️ Note: Retrieval succeeded regardless of job state ("${jobDetails.state}").\n`);
  } catch (err) {
    console.error('❌ Step 2 Failed (Job Retrieval Error):', err);
    throw err;
  }

  // Test Step 3: Error Handling Verification (404 Not Found)
  try {
    const nonExistentId = 'non-existent-job-id-99999999';
    console.log(`🔹 Step 3: Testing error handling with non-existent job ID ("${nonExistentId}")...`);
    await client.jobs.get(nonExistentId);
    console.error('❌ Step 3 Failed: Expected request to fail with OrchestrixNotFoundError, but it succeeded.');
  } catch (err) {
    if (err instanceof OrchestrixNotFoundError || err instanceof OrchestrixApiError) {
      console.log('   ✅ Clear Error Handling Verified:');
      console.log(`      - Error Name: ${err.name}`);
      console.log(`      - Status Code: ${err.status}`);
      console.log(`      - Status Text: ${err.message}`);
      console.log('   ✅ Verification Passed: Non-existent job request failed cleanly with clear error reporting.\n');
    } else {
      console.error('   ❌ Step 3 Unexpected Error Type:', err);
      throw err;
    }
  }

  // Test Step 4: Error Handling Verification (Client-side Validation)
  try {
    console.log('🔹 Step 4: Testing client-side validation error handling (invalid priority = 15)...');
    await client.jobs.create({ test: 1 }, { priority: 15 });
    console.error('❌ Step 4 Failed: Expected request to fail with OrchestrixValidationError, but it succeeded.');
  } catch (err) {
    if (err instanceof OrchestrixValidationError) {
      console.log('   ✅ Client Validation Error Verified:');
      console.log(`      - Error Name: ${err.name}`);
      console.log(`      - Message: ${err.message}`);
      console.log('   ✅ Verification Passed: Validation error caught prior to HTTP request.\n');
    } else {
      console.error('   ❌ Step 4 Unexpected Error Type:', err);
      throw err;
    }
  }

  console.log(`==================================================`);
  console.log(`🎉 ALL INTEGRATION TESTS PASSED SUCCESSFULLY!`);
  console.log(`==================================================\n`);
}

runIntegrationTest().catch((err) => {
  console.error('\n❌ INTEGRATION TEST SUITE FAILED:', err.message || err);
  process.exit(1);
});
