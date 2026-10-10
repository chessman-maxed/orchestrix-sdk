import { Pool } from 'pg';

const pool = new Pool({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  database: process.env.DATABASE_NAME || 'orchestrix',
  user: process.env.DATABASE_USER || 'orchestrix',
  password: process.env.DATABASE_PASSWORD || 'orchestrix_dev',
});

/**
  * Best-effort initialization of the database schema.
  * Logs errors if DB is unreachable, but does not throw.
  */
export async function initDb(): Promise<void> {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS job_batches (
        id                 BIGSERIAL    PRIMARY KEY,
        name               TEXT         NOT NULL,
        status             TEXT         NOT NULL DEFAULT 'pending',
        total_jobs         INTEGER      NOT NULL DEFAULT 0,
        completed_jobs     INTEGER      NOT NULL DEFAULT 0,
        failed_jobs        INTEGER      NOT NULL DEFAULT 0,
        dead_lettered_jobs INTEGER      NOT NULL DEFAULT 0,
        cancelled_jobs     INTEGER      NOT NULL DEFAULT 0,
        running_jobs       INTEGER      NOT NULL DEFAULT 0,
        created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        started_at         TIMESTAMPTZ,
        completed_at       TIMESTAMPTZ,
        updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id                   BIGSERIAL    PRIMARY KEY,
        bullmq_job_id        TEXT         NOT NULL UNIQUE,
        task_type            TEXT         NOT NULL,
        status               TEXT         NOT NULL DEFAULT 'pending',
        priority             INTEGER      NOT NULL DEFAULT 5,
        attempts             INTEGER      NOT NULL DEFAULT 0,
        max_attempts         INTEGER      NOT NULL DEFAULT 3,
        worker_id            TEXT,
        created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        started_at           TIMESTAMPTZ,
        completed_at         TIMESTAMPTZ,
        failed_at            TIMESTAMPTZ,
        cancelled_at         TIMESTAMPTZ,
        cancelled_by         TEXT,
        cancellation_reason  TEXT,
        execution_time_ms    INTEGER,
        failed_reason        TEXT,
        replayed_from_job_id TEXT         REFERENCES jobs(bullmq_job_id) ON DELETE SET NULL,
        batch_id             BIGINT       REFERENCES job_batches(id) ON DELETE SET NULL
      )
    `);
    await pool.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS batch_id BIGINT REFERENCES job_batches(id) ON DELETE SET NULL`);
    await pool.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ`);
    await pool.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancelled_by TEXT`);
    await pool.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancellation_reason TEXT`);
    await pool.query(`ALTER TABLE job_batches ADD COLUMN IF NOT EXISTS cancelled_jobs INTEGER NOT NULL DEFAULT 0`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs (status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jobs_bullmq_id  ON jobs (bullmq_job_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jobs_batch_id   ON jobs (batch_id)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS schedules (
        id             BIGSERIAL    PRIMARY KEY,
        name           TEXT         NOT NULL,
        task_type      TEXT         NOT NULL,
        payload        JSONB        NOT NULL DEFAULT '{}'::jsonb,
        interval_ms    BIGINT       NOT NULL,
        priority       INTEGER      NOT NULL DEFAULT 5,
        is_paused      BOOLEAN      NOT NULL DEFAULT FALSE,
        next_run_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        last_run_at    TIMESTAMPTZ,
        total_runs     INTEGER      NOT NULL DEFAULT 0,
        created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON schedules (next_run_at, is_paused)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS job_dependencies (
        id                 BIGSERIAL    PRIMARY KEY,
        job_id             TEXT         NOT NULL REFERENCES jobs(bullmq_job_id) ON DELETE CASCADE,
        depends_on_job_id  TEXT         NOT NULL REFERENCES jobs(bullmq_job_id) ON DELETE CASCADE,
        created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_job_dependency UNIQUE (job_id, depends_on_job_id)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_job_deps_job_id ON job_dependencies (job_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_job_deps_depends_on ON job_dependencies (depends_on_job_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs (worker_id)`);

    console.log('[DB] ✅ PostgreSQL: job_batches, jobs, schedules, and job_dependencies tables ready');
  } catch (error) {
    console.error('[DB] ⚠️ PostgreSQL initialization failed (server/worker will continue running without DB persistence):', error);
  }
}

export async function insertSchedule(params: {
  name: string;
  taskType: string;
  intervalMs: number;
  priority?: number;
  payload?: any;
}): Promise<any> {
  const result = await pool.query(
    `INSERT INTO schedules (name, task_type, interval_ms, priority, payload, next_run_at)
     VALUES ($1, $2, $3::BIGINT, $4, $5, NOW() + ($3::BIGINT || ' milliseconds')::INTERVAL)
     RETURNING *`,
    [
      params.name,
      params.taskType,
      params.intervalMs,
      params.priority ?? 5,
      JSON.stringify(params.payload ?? {}),
    ]
  );
  return result.rows[0];
}

export async function getSchedules(): Promise<any[]> {
  const result = await pool.query(`SELECT * FROM schedules ORDER BY id ASC`);
  return result.rows;
}

export async function getScheduleById(id: number | string): Promise<any | null> {
  const result = await pool.query(`SELECT * FROM schedules WHERE id = $1`, [id]);
  return result.rows[0] || null;
}

export async function updateScheduleState(id: number | string, isPaused: boolean): Promise<any | null> {
  // If resuming, reset next_run_at to NOW() so execution resumes immediately
  const query = isPaused
    ? `UPDATE schedules SET is_paused = TRUE, updated_at = NOW() WHERE id = $1 RETURNING *`
    : `UPDATE schedules SET is_paused = FALSE, next_run_at = NOW(), updated_at = NOW() WHERE id = $1 RETURNING *`;
  const result = await pool.query(query, [id]);
  return result.rows[0] || null;
}

export async function deleteSchedule(id: number | string): Promise<boolean> {
  const result = await pool.query(`DELETE FROM schedules WHERE id = $1 RETURNING id`, [id]);
  return (result.rowCount ?? 0) > 0;
}

export async function fetchDueSchedules(): Promise<any[]> {
  const result = await pool.query(
    `SELECT * FROM schedules
     WHERE next_run_at <= NOW() AND is_paused = FALSE
     ORDER BY next_run_at ASC`
  );
  return result.rows;
}

export async function claimAndAdvanceSchedule(id: number | string, intervalMs: number): Promise<any | null> {
  // Atomically claim the schedule and advance next_run_at from NOW() + interval_ms (preventing catch-up backlog)
  const result = await pool.query(
    `UPDATE schedules
     SET next_run_at = NOW() + ($2::BIGINT || ' milliseconds')::INTERVAL,
         last_run_at = NOW(),
         total_runs  = total_runs + 1,
         updated_at  = NOW()
     WHERE id = $1 AND next_run_at <= NOW() AND is_paused = FALSE
     RETURNING *`,
    [id, intervalMs]
  );
  return result.rows[0] || null;
}


export async function insertJob(params: {
  bullmqJobId: string;
  taskType: string;
  priority: number;
  maxAttempts: number;
  replayedFromJobId?: string;
  batchId?: number | string;
}): Promise<void> {
  await pool.query(
    `INSERT INTO jobs
       (bullmq_job_id, task_type, status, priority, max_attempts, replayed_from_job_id, batch_id)
     VALUES ($1, $2, 'pending', $3, $4, $5, $6)`,
    [
      params.bullmqJobId,
      params.taskType,
      params.priority,
      params.maxAttempts,
      params.replayedFromJobId ?? null,
      params.batchId ?? null,
    ]
  );
}

export async function updateJobStarted(
  bullmqJobId: string,
  workerId: string
): Promise<number | null> {
  const result = await pool.query(
    `UPDATE jobs
     SET status     = 'running',
         started_at = NOW(),
         worker_id  = $2
     WHERE bullmq_job_id = $1
       AND status IN ('pending', 'retrying')
     RETURNING batch_id`,
    [bullmqJobId, workerId]
  );
  return result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
}

export async function updateJobCompleted(params: {
  bullmqJobId: string;
  attemptsMade: number;
  executionTimeMs: number;
}): Promise<{ rowCount: number; batchId: number | null }> {
  const result = await pool.query(
    `UPDATE jobs
     SET status            = 'completed',
         completed_at      = NOW(),
         attempts          = $2,
         execution_time_ms = $3
     WHERE bullmq_job_id = $1
       AND status = 'running'
     RETURNING batch_id`,
    [params.bullmqJobId, params.attemptsMade, params.executionTimeMs]
  );
  const rowCount = result.rowCount ?? 0;
  const batchId = result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
  return { rowCount, batchId };
}

export async function updateJobFailed(params: {
  bullmqJobId: string;
  attemptsMade: number;
  maxAttempts: number;
  failedReason: string;
}): Promise<{ rowCount: number; batchId: number | null }> {
  const isPermanent = params.attemptsMade >= params.maxAttempts;

  let result;
  if (isPermanent) {
    result = await pool.query(
      `UPDATE jobs
       SET status        = 'failed',
           failed_at     = NOW(),
           attempts      = $2,
           failed_reason = $3
       WHERE bullmq_job_id = $1
         AND status = 'running'
       RETURNING batch_id`,
      [params.bullmqJobId, params.attemptsMade, params.failedReason]
    );
  } else {
    result = await pool.query(
      `UPDATE jobs
       SET status        = 'retrying',
           attempts      = $2,
           failed_reason = $3
       WHERE bullmq_job_id = $1
         AND status = 'running'
       RETURNING batch_id`,
      [params.bullmqJobId, params.attemptsMade, params.failedReason]
    );
  }
  const rowCount = result.rowCount ?? 0;
  const batchId = result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
  return { rowCount, batchId };
}

export async function updateJobStalled(bullmqJobId: string): Promise<number | null> {
  const result = await pool.query(
    `UPDATE jobs
     SET status        = 'stalled',
         failed_reason = 'Job stalled: worker lock expired or process crashed'
     WHERE bullmq_job_id = $1
       AND status = 'running'
     RETURNING batch_id`,
    [bullmqJobId]
  );
  return result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
}

export async function updateJobDeadLettered(params: {
  bullmqJobId: string;
  attemptsMade: number;
  failedReason: string;
}): Promise<number | null> {
  const result = await pool.query(
    `UPDATE jobs
     SET status        = 'dead_lettered',
         failed_at     = NOW(),
         attempts      = $2,
         failed_reason = $3
     WHERE bullmq_job_id = $1
       AND status = 'failed'
     RETURNING batch_id`,
    [params.bullmqJobId, params.attemptsMade, params.failedReason]
  );
  return result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
}

export async function cancelJobInDb(params: {
  bullmqJobId: string;
  cancelledBy?: string;
  reason?: string;
}): Promise<{ rowCount: number; batchId: number | null; status: string | null }> {
  const result = await pool.query(
    `UPDATE jobs
     SET status              = 'cancelled',
         cancelled_at        = NOW(),
         cancelled_by        = $2,
         cancellation_reason = $3
     WHERE bullmq_job_id = $1
       AND status IN ('pending', 'retrying', 'running')
     RETURNING batch_id, status`,
    [params.bullmqJobId, params.cancelledBy || 'user', params.reason || null]
  );
  const rowCount = result.rowCount ?? 0;
  const batchId = result.rows[0]?.batch_id ? Number(result.rows[0].batch_id) : null;
  const status = result.rows[0]?.status || null;
  return { rowCount, batchId, status };
}

export async function getJobByBullId(bullmqJobId: string): Promise<any | null> {
  const result = await pool.query(`SELECT * FROM jobs WHERE bullmq_job_id = $1`, [bullmqJobId]);
  return result.rows[0] || null;
}

/**
 * Checks for circular dependency using recursive CTE.
 * Returns true if adding (jobId -> dependsOnJobId) creates a cycle.
 * A cycle occurs if there is already a path from dependsOnJobId to jobId.
 */
export async function hasCircularDependency(jobId: string, dependsOnJobId: string): Promise<boolean> {
  if (jobId === dependsOnJobId) return true;
  const result = await pool.query(
    `WITH RECURSIVE dependency_chain AS (
       SELECT depends_on_job_id FROM job_dependencies WHERE job_id = $1
       UNION
       SELECT d.depends_on_job_id
       FROM job_dependencies d
       INNER JOIN dependency_chain dc ON d.job_id = dc.depends_on_job_id
     )
     SELECT 1 FROM dependency_chain WHERE depends_on_job_id = $2 LIMIT 1`,
    [dependsOnJobId, jobId]
  );
  return result.rows.length > 0;
}

export async function addJobDependency(jobId: string, dependsOnJobId: string): Promise<any> {
  const result = await pool.query(
    `INSERT INTO job_dependencies (job_id, depends_on_job_id)
     VALUES ($1, $2)
     ON CONFLICT (job_id, depends_on_job_id) DO NOTHING
     RETURNING *`,
    [jobId, dependsOnJobId]
  );
  return result.rows[0] || null;
}

export async function getJobDependencies(jobId: string): Promise<any[]> {
  const result = await pool.query(
    `SELECT d.id AS dependency_id, d.depends_on_job_id, j.status, j.task_type, j.failed_reason
     FROM job_dependencies d
     LEFT JOIN jobs j ON d.depends_on_job_id = j.bullmq_job_id
     WHERE d.job_id = $1
     ORDER BY d.id ASC`,
    [jobId]
  );
  return result.rows;
}

export async function removeJobDependency(jobId: string, dependencyId: string | number): Promise<boolean> {
  let result;
  // If dependencyId is a numeric DB ID or dependent job ID
  if (!isNaN(Number(dependencyId))) {
    result = await pool.query(
      `DELETE FROM job_dependencies WHERE job_id = $1 AND id = $2 RETURNING id`,
      [jobId, dependencyId]
    );
  } else {
    result = await pool.query(
      `DELETE FROM job_dependencies WHERE job_id = $1 AND depends_on_job_id = $2 RETURNING id`,
      [jobId, dependencyId]
    );
  }
  return (result.rowCount ?? 0) > 0;
}

export type DependencyEvaluationResult = {
  allCompleted: boolean;
  hasFailed: boolean;
  failedDependencyId?: string;
  pendingCount: number;
  totalDependencies: number;
};

export async function evaluateJobDependencies(jobId: string): Promise<DependencyEvaluationResult> {
  const deps = await getJobDependencies(jobId);
  if (deps.length === 0) {
    return { allCompleted: true, hasFailed: false, pendingCount: 0, totalDependencies: 0 };
  }

  let pendingCount = 0;
  let hasFailed = false;
  let failedDependencyId: string | undefined = undefined;

  for (const dep of deps) {
    const status = dep.status;
    if (status === 'completed') {
      continue;
    } else if (status === 'failed' || status === 'dead_lettered') {
      hasFailed = true;
      failedDependencyId = dep.depends_on_job_id;
      break;
    } else {
      pendingCount++;
    }
  }

  const allCompleted = !hasFailed && pendingCount === 0;
  return {
    allCompleted,
    hasFailed,
    failedDependencyId,
    pendingCount,
    totalDependencies: deps.length,
  };
}

export async function getJobMetricsFromDb(): Promise<{
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  deadLetteredJobs: number;
  cancelledJobs: number;
  runningJobs: number;
  pendingJobs: number;
  retryingJobs: number;
  stalledJobs: number;
  avgExecutionTimeMs: number;
  successRate: number;
  failureRate: number;
  totalAttempts: number;
  completedLastMinute: number;
}> {
  const result = await pool.query(`
    SELECT
      COUNT(*)::INT AS total_jobs,
      COUNT(*) FILTER (WHERE status = 'completed')::INT AS completed_jobs,
      COUNT(*) FILTER (WHERE status = 'failed')::INT AS failed_jobs,
      COUNT(*) FILTER (WHERE status = 'dead_lettered')::INT AS dead_lettered_jobs,
      COUNT(*) FILTER (WHERE status = 'cancelled')::INT AS cancelled_jobs,
      COUNT(*) FILTER (WHERE status = 'running')::INT AS running_jobs,
      COUNT(*) FILTER (WHERE status = 'pending')::INT AS pending_jobs,
      COUNT(*) FILTER (WHERE status = 'retrying')::INT AS retrying_jobs,
      COUNT(*) FILTER (WHERE status = 'stalled')::INT AS stalled_jobs,
      COALESCE(AVG(execution_time_ms) FILTER (WHERE status = 'completed'), 0)::FLOAT AS avg_execution_time_ms,
      COALESCE(SUM(attempts), 0)::INT AS total_attempts,
      COUNT(*) FILTER (WHERE status = 'completed' AND completed_at >= NOW() - INTERVAL '1 minute')::INT AS completed_last_minute
    FROM jobs
  `);

  const row = result.rows[0] || {};
  const totalJobs = Number(row.total_jobs) || 0;
  const completedJobs = Number(row.completed_jobs) || 0;
  const failedJobs = Number(row.failed_jobs) || 0;
  const deadLetteredJobs = Number(row.dead_lettered_jobs) || 0;
  const cancelledJobs = Number(row.cancelled_jobs) || 0;

  const totalFinished = completedJobs + failedJobs + deadLetteredJobs + cancelledJobs;
  const successRate = totalFinished > 0 ? (completedJobs / totalFinished) * 100 : 0;
  const failureRate = totalFinished > 0 ? ((failedJobs + deadLetteredJobs) / totalFinished) * 100 : 0;

  return {
    totalJobs,
    completedJobs,
    failedJobs,
    deadLetteredJobs,
    cancelledJobs,
    runningJobs: Number(row.running_jobs) || 0,
    pendingJobs: Number(row.pending_jobs) || 0,
    retryingJobs: Number(row.retrying_jobs) || 0,
    stalledJobs: Number(row.stalled_jobs) || 0,
    avgExecutionTimeMs: Math.round(Number(row.avg_execution_time_ms) || 0),
    successRate: parseFloat(successRate.toFixed(2)),
    failureRate: parseFloat(failureRate.toFixed(2)),
    totalAttempts: Number(row.total_attempts) || 0,
    completedLastMinute: Number(row.completed_last_minute) || 0,
  };
}

export async function getSchedulerMetricsFromDb(): Promise<{
  totalSchedules: number;
  activeSchedules: number;
  pausedSchedules: number;
  totalExecutions: number;
  lastScheduledExecution: string | null;
  nextScheduledExecution: string | null;
}> {
  const result = await pool.query(`
    SELECT
      COUNT(*)::INT AS total_schedules,
      COUNT(*) FILTER (WHERE is_paused = FALSE)::INT AS active_schedules,
      COUNT(*) FILTER (WHERE is_paused = TRUE)::INT AS paused_schedules,
      COALESCE(SUM(total_runs), 0)::INT AS total_executions,
      MAX(last_run_at) AS last_scheduled_execution,
      MIN(next_run_at) FILTER (WHERE is_paused = FALSE) AS next_scheduled_execution
    FROM schedules
  `);

  const row = result.rows[0] || {};
  return {
    totalSchedules: Number(row.total_schedules) || 0,
    activeSchedules: Number(row.active_schedules) || 0,
    pausedSchedules: Number(row.paused_schedules) || 0,
    totalExecutions: Number(row.total_executions) || 0,
    lastScheduledExecution: row.last_scheduled_execution ? new Date(row.last_scheduled_execution).toISOString() : null,
    nextScheduledExecution: row.next_scheduled_execution ? new Date(row.next_scheduled_execution).toISOString() : null,
  };
}

export async function getWorkflowMetricsFromDb(): Promise<{
  jobsWithDependenciesCount: number;
  totalDependenciesCount: number;
  completedDependentJobs: number;
  failedDependentJobs: number;
  blockedJobsCount: number;
}> {
  const depsCountRes = await pool.query(`SELECT COUNT(*)::INT AS total_deps, COUNT(DISTINCT job_id)::INT AS distinct_jobs FROM job_dependencies`);
  const depsCountRow = depsCountRes.rows[0] || {};

  const statusRes = await pool.query(`
    SELECT
      COUNT(DISTINCT j.bullmq_job_id) FILTER (WHERE j.status = 'completed')::INT AS completed_dependent,
      COUNT(DISTINCT j.bullmq_job_id) FILTER (WHERE j.status IN ('failed', 'dead_lettered'))::INT AS failed_dependent
    FROM jobs j
    INNER JOIN job_dependencies d ON j.bullmq_job_id = d.job_id
  `);
  const statusRow = statusRes.rows[0] || {};

  // A job is considered blocked if its status is 'pending' or 'retrying' and at least one of its dependencies is 'failed' or 'dead_lettered'
  const blockedRes = await pool.query(`
    SELECT COUNT(DISTINCT d.job_id)::INT AS blocked_count
    FROM job_dependencies d
    INNER JOIN jobs parent ON d.depends_on_job_id = parent.bullmq_job_id
    INNER JOIN jobs child ON d.job_id = child.bullmq_job_id
    WHERE child.status IN ('pending', 'retrying')
      AND parent.status IN ('failed', 'dead_lettered')
  `);
  const blockedRow = blockedRes.rows[0] || {};

  return {
    jobsWithDependenciesCount: Number(depsCountRow.distinct_jobs) || 0,
    totalDependenciesCount: Number(depsCountRow.total_deps) || 0,
    completedDependentJobs: Number(statusRow.completed_dependent) || 0,
    failedDependentJobs: Number(statusRow.failed_dependent) || 0,
    blockedJobsCount: Number(blockedRow.blocked_count) || 0,
  };
}

export async function getWorkerJobCountsFromDb(): Promise<Record<string, { completed: number; failed: number; running: number }>> {
  const result = await pool.query(`
    SELECT
      worker_id,
      COUNT(*) FILTER (WHERE status = 'completed')::INT AS completed_count,
      COUNT(*) FILTER (WHERE status IN ('failed', 'dead_lettered'))::INT AS failed_count,
      COUNT(*) FILTER (WHERE status = 'running')::INT AS running_count
    FROM jobs
    WHERE worker_id IS NOT NULL
    GROUP BY worker_id
  `);

  const stats: Record<string, { completed: number; failed: number; running: number }> = {};
  for (const row of result.rows) {
    stats[row.worker_id] = {
      completed: Number(row.completed_count) || 0,
      failed: Number(row.failed_count) || 0,
      running: Number(row.running_count) || 0,
    };
  }
  return stats;
}

export async function createJobBatch(name: string, totalJobs: number): Promise<any> {
  const result = await pool.query(
    `INSERT INTO job_batches (name, total_jobs, status)
     VALUES ($1, $2, 'pending')
     RETURNING *`,
    [name, totalJobs]
  );
  return result.rows[0];
}

export async function getJobBatches(): Promise<any[]> {
  const result = await pool.query(`SELECT * FROM job_batches ORDER BY id DESC`);
  return result.rows;
}

export async function getJobBatchById(id: number | string): Promise<any | null> {
  const result = await pool.query(`SELECT * FROM job_batches WHERE id = $1`, [id]);
  return result.rows[0] || null;
}

export async function getBatchJobs(batchId: number | string): Promise<any[]> {
  const result = await pool.query(`SELECT * FROM jobs WHERE batch_id = $1 ORDER BY id ASC`, [batchId]);
  return result.rows;
}

export async function updateBatchProgress(batchId: number | string): Promise<any | null> {
  const result = await pool.query(
    `UPDATE job_batches b
     SET
       completed_jobs     = s.completed_cnt,
       failed_jobs        = s.failed_cnt,
       dead_lettered_jobs = s.dead_lettered_cnt,
       cancelled_jobs     = s.cancelled_cnt,
       running_jobs       = s.running_cnt,
       total_jobs         = GREATEST(b.total_jobs, s.actual_total),
       status = CASE
         WHEN s.actual_total > 0 AND (s.completed_cnt + s.failed_cnt + s.dead_lettered_cnt + s.cancelled_cnt) = s.actual_total THEN
           CASE
             WHEN s.completed_cnt = s.actual_total THEN 'completed'
             WHEN s.cancelled_cnt = s.actual_total THEN 'cancelled'
             WHEN (s.failed_cnt + s.dead_lettered_cnt) = s.actual_total THEN 'failed'
             ELSE 'partial'
           END
         WHEN s.running_cnt > 0 OR (s.completed_cnt + s.failed_cnt + s.dead_lettered_cnt + s.cancelled_cnt) > 0 THEN 'running'
         ELSE 'pending'
       END,
       started_at = COALESCE(b.started_at, CASE
         WHEN (s.running_cnt > 0 OR (s.completed_cnt + s.failed_cnt + s.dead_lettered_cnt + s.cancelled_cnt) > 0) THEN NOW()
         ELSE NULL
       END),
       completed_at = CASE
         WHEN s.actual_total > 0 AND (s.completed_cnt + s.failed_cnt + s.dead_lettered_cnt + s.cancelled_cnt) = s.actual_total THEN COALESCE(b.completed_at, NOW())
         ELSE NULL
       END,
       updated_at = NOW()
     FROM (
       SELECT
         COUNT(*)::INT AS actual_total,
         COUNT(*) FILTER (WHERE status = 'completed')::INT AS completed_cnt,
         COUNT(*) FILTER (WHERE status = 'failed')::INT AS failed_cnt,
         COUNT(*) FILTER (WHERE status = 'dead_lettered')::INT AS dead_lettered_cnt,
         COUNT(*) FILTER (WHERE status = 'cancelled')::INT AS cancelled_cnt,
         COUNT(*) FILTER (WHERE status = 'running')::INT AS running_cnt
       FROM jobs
       WHERE batch_id = $1
     ) s
     WHERE b.id = $1
     RETURNING b.*`,
    [batchId]
  );
  return result.rows[0] || null;
}

export async function deleteJobBatch(batchId: number | string): Promise<boolean> {
  await pool.query(`UPDATE jobs SET batch_id = NULL WHERE batch_id = $1`, [batchId]);
  const result = await pool.query(`DELETE FROM job_batches WHERE id = $1 RETURNING id`, [batchId]);
  return (result.rowCount ?? 0) > 0;
}

export async function getBatchMetricsFromDb(): Promise<{
  totalBatches: number;
  pendingBatches: number;
  runningBatches: number;
  completedBatches: number;
  partialBatches: number;
  failedBatches: number;
  cancelledBatches: number;
  totalJobsInBatches: number;
}> {
  const result = await pool.query(`
    SELECT
      COUNT(*)::INT AS total_batches,
      COUNT(*) FILTER (WHERE status = 'pending')::INT AS pending_batches,
      COUNT(*) FILTER (WHERE status = 'running')::INT AS running_batches,
      COUNT(*) FILTER (WHERE status = 'completed')::INT AS completed_batches,
      COUNT(*) FILTER (WHERE status = 'partial')::INT AS partial_batches,
      COUNT(*) FILTER (WHERE status = 'failed')::INT AS failed_batches,
      COUNT(*) FILTER (WHERE status = 'cancelled')::INT AS cancelled_batches,
      COALESCE(SUM(total_jobs), 0)::INT AS total_jobs_in_batches
    FROM job_batches
  `);

  const row = result.rows[0] || {};
  return {
    totalBatches: Number(row.total_batches) || 0,
    pendingBatches: Number(row.pending_batches) || 0,
    runningBatches: Number(row.running_batches) || 0,
    completedBatches: Number(row.completed_batches) || 0,
    partialBatches: Number(row.partial_batches) || 0,
    failedBatches: Number(row.failed_batches) || 0,
    cancelledBatches: Number(row.cancelled_batches) || 0,
    totalJobsInBatches: Number(row.total_jobs_in_batches) || 0,
  };
}


