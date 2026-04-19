import { executeJob } from "./engines/index.js";

export function createJobRunner({ store, executionMode, logger }) {
  const queue = [];
  let running = false;

  async function processQueue() {
    if (running) {
      return;
    }

    running = true;

    while (queue.length > 0) {
      const jobId = queue.shift();
      const current = store.get(jobId);
      if (!current) {
        continue;
      }

      await store.update(jobId, {
        status: "running",
        startedAt: new Date().toISOString()
      });

      const onLog = async (message) => {
        const timestamp = new Date().toISOString();
        await store.appendLog(jobId, `[${timestamp}] ${message}`);
      };

      try {
        logger.info({ jobId }, "Executing job");
        const result = await executeJob(current, executionMode, onLog);

        await store.update(jobId, {
          status: "completed",
          endedAt: new Date().toISOString(),
          result,
          error: null
        });
      } catch (error) {
        logger.error({ err: error, jobId }, "Job execution failed");
        await store.update(jobId, {
          status: "failed",
          endedAt: new Date().toISOString(),
          error: {
            message: error.message,
            code: error.code || null,
            stdout: error.stdout || "",
            stderr: error.stderr || ""
          }
        });

        await onLog(`Job failed: ${error.message}`);
      }
    }

    running = false;
  }

  function enqueue(jobId) {
    queue.push(jobId);
    void processQueue();
  }

  function stats() {
    return {
      pending: queue.length,
      running
    };
  }

  return {
    enqueue,
    stats
  };
}
