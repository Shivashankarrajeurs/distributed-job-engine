import { redis } from "../config/redis.js";
import { JOB_STATES } from "../queue/jobStates.js";

const VISIBILITY_TIMEOUT = 15000; 

let isShuttingDown = false;
let currentJobId = null;


//for circuit breaker

const FAILURE_THRESHOLD = 5;
const COOLDOWN_TIME = 30000; 

//rate limitting 
const RATE_LIMIT = 1; // max executions per second

//Handler simulation

async function fakeEmail(job) {
  // processing time
 await new Promise(resolve => setTimeout(resolve, 5000));

  if (Math.random() < 0.5) {
    throw new Error("Simulated email failure");
  }
}

function getHandler(type) {
  switch (type) {
    case "SEND_EMAIL":
      return fakeEmail;
    default:
      throw new Error("Unknown job type");
  }
}


//On starting recovering stale jobs

async function recoverStaleJobs() {
  console.log("Checking for stale jobs...");

  const processingJobs = await redis.lrange("queue:processing", 0, -1);

  for (const jobId of processingJobs) {
    console.log(`Recovering job ${jobId}`);

    await redis.lrem("queue:processing", 1, jobId);
    await redis.lpush("queue:jobs", jobId);

    await redis.hset(`job:${jobId}`, {
      status: JOB_STATES.FAILED,
      lockedAt: "",
      updatedAt: Date.now()
    });
  }

  if (processingJobs.length === 0) {
    console.log("No stale jobs found.");
  }
}


//circuit breaker helper 

async function isRequestAllowed() {
  const state = await redis.get("breaker:state");

  if (!state || state === "CLOSED") {
    return true;
  }

  if (state === "OPEN") {
    const openedAt = await redis.get("breaker:openedAt");
    const now = Date.now();

    if (now - openedAt > COOLDOWN_TIME) {
      console.log("Circuit moving to HALF_OPEN");
      await redis.set("breaker:state", "HALF_OPEN");
      return true;
    }

    return false;
  }

  if (state === "HALF_OPEN") {
    return true;
  }

  return true;
}


async function recordSuccess() {
  const state = await redis.get("breaker:state");

  if (state === "HALF_OPEN") {
    console.log("Circuit CLOSED after successful test");
    await redis.set("breaker:state", "CLOSED");
    await redis.set("breaker:failures", 0);
  }
}


async function recordFailure() {
  const failures = await redis.incr("breaker:failures");

  if (failures >= FAILURE_THRESHOLD) {
    console.log("Circuit OPENED due to failures");
    await redis.set("breaker:state", "OPEN");
    await redis.set("breaker:openedAt", Date.now());
  }
}

//Rate limit checker

async function isRateAllowed() {
  const currentSecond = Math.floor(Date.now() / 1000);
  const key = `rate:current:${currentSecond}`;

  const count = await redis.incr(key);

  if (count === 1) {
    await redis.expire(key, 1);
  }

  if (count > RATE_LIMIT) {
    return false;
  }

  return true;
}



//retrying with schedular(delayed queue)

async function retryScheduler() {
  while (!isShuttingDown) {
    const now = Date.now();

    const readyJobs = await redis.zrangebyscore(
      "queue:delayed",
      0,
      now
    );

    const queueMap = {
    HIGH: "queue:high",
    NORMAL: "queue:normal",
    LOW: "queue:low"
     };

    for (const jobId of readyJobs) {
      console.log(`Requeuing delayed job ${jobId}`);

      const job = await redis.hgetall(`job:${jobId}`);
      const queueName = queueMap[job.priority] || "queue:normal";

      await redis.lpush(queueName, jobId);
      await redis.zrem("queue:delayed", jobId);
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}


//Timeout monitor

async function visibilityMonitor() {
  while (!isShuttingDown) {
    const now = Date.now();

    const processingJobs = await redis.lrange("queue:processing", 0, -1);

    for (const jobId of processingJobs) {
      const job = await redis.hgetall(`job:${jobId}`);
      if (!job.lockedAt) continue;

      const lockedAt = parseInt(job.lockedAt);

      if (now - lockedAt > VISIBILITY_TIMEOUT) {
        console.log(`Visibility timeout for job ${jobId}`);

        await redis.lrem("queue:processing", 1, jobId);
        await redis.lpush("queue:jobs", jobId);

        await redis.hset(`job:${jobId}`, {
          status: JOB_STATES.FAILED,
          lockedAt: "",
          updatedAt: Date.now()
        });
      }
    }

    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}

//reservation of job(atomic)

async function getNextJob() {
  while (!isShuttingDown) {

    // Try HIGH
    let jobId = await redis.rpoplpush(
      "queue:high",
      "queue:processing"
    );
    if (jobId) return jobId;

    //  Try NORMAL
    jobId = await redis.rpoplpush(
      "queue:normal",
      "queue:processing"
    );
    if (jobId) return jobId;

    // Try LOW
    jobId = await redis.rpoplpush(
      "queue:low",
      "queue:processing"
    );
    if (jobId) return jobId;

    await new Promise(resolve => setTimeout(resolve, 500));
  }

  return null;
}

//Main worker loop

async function processJobs() {
  console.log("Worker started...");

  await recoverStaleJobs();

  // Start background systems
  retryScheduler();
  visibilityMonitor();

  while (!isShuttingDown) {
    console.log("Waiting for jobs...");

    const jobId = await getNextJob();
    if (!jobId) break;

    currentJobId = jobId;
    const startTime = Date.now();

    const job = await redis.hgetall(`job:${jobId}`);

    console.log(`Processing job: ${jobId}`);

    // Idempotency check
    if (job.executed === "true") {
      console.log(`Skipping already executed job ${jobId}`);
      await redis.lrem("queue:processing", 1, jobId);
      currentJobId = null;
      continue;
    }

    // Lock job
    await redis.hset(`job:${jobId}`, {
      lockedAt: Date.now(),
      status: JOB_STATES.IN_PROGRESS
    });

    try {

      //circuit breaker 

     const allowed = await isRequestAllowed();

if (!allowed) {
  console.log("Circuit OPEN - delaying job until cooldown ends");

  const openedAt = await redis.get("breaker:openedAt");

  if (openedAt) {
    const reopenTime = Number(openedAt) + COOLDOWN_TIME;

    await redis.lrem("queue:processing", 1, jobId);

    await redis.zadd("queue:delayed", reopenTime, jobId);
  } else {
    // fallback safety
    await redis.lrem("queue:processing", 1, jobId);
    await redis.zadd("queue:delayed", Date.now() + COOLDOWN_TIME, jobId);
  }

  currentJobId = null;
  continue;
} //


  //rate limit handling  

  const rateAllowed = await isRateAllowed();

if (!rateAllowed) {
  console.log("Rate limit exceeded - delaying job");

  await redis.lrem("queue:processing", 1, jobId);
  await redis.zadd("queue:delayed", Date.now() + 1000, jobId);

  currentJobId = null;
  continue;
} //
      const handler = getHandler(job.type);
      await handler(job);

      await redis.hset(`job:${jobId}`, {
        status: JOB_STATES.COMPLETED,
        executed: "true",
        lockedAt: "",
        nextRetryAt: "",
        updatedAt: Date.now()
      });
      
      await recordSuccess();
      await redis.incr("metrics:processed");

      //Auto delete all jobs after 1 day
      await redis.expire(`job:${jobId}`, 86400); // 24 hours


      console.log(`Job ${jobId} completed`);

    } catch (error) {
      console.log(`Job failed: ${error.message}`);
      await recordFailure();

      await redis.incr("metrics:failed");

      const attempts = await redis.hincrby(
        `job:${jobId}`,
        "attempts",
        1
      );

      const maxAttempts = parseInt(job.maxAttempts);

      if (attempts >= maxAttempts) {
        console.log(`Job ${jobId} moved to DEAD`);

        await redis.hset(`job:${jobId}`, {
          status: JOB_STATES.DEAD,
          lockedAt: "",
          updatedAt: Date.now()
        });

        await redis.incr("metrics:dead");

      } else {
        const delay = Math.pow(2, attempts) * 1000;
        const retryTime = Date.now() + delay;

        console.log(`Retrying job ${jobId} in ${delay} ms`);

        await redis.hset(`job:${jobId}`, {
          status: JOB_STATES.FAILED,
          nextRetryAt: retryTime,
          lockedAt: "",
          updatedAt: Date.now()
        });

        await redis.zadd("queue:delayed", retryTime, jobId);
        await redis.incr("metrics:retried");
      }
    }

    const duration = Date.now() - startTime;
    await redis.incrby("metrics:totalTime", duration);


    await redis.lrem("queue:processing", 1, jobId);
    currentJobId = null;
  }

  
 

  //Shutdown gracefully

  console.log("Waiting for current job to finish...");

  while (currentJobId !== null) {
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log("Shutdown complete.");
  process.exit(0);
}

//shutdown signals

process.on("SIGINT", () => {
  console.log("Graceful shutdown initiated (SIGINT)...");
  isShuttingDown = true;
});

process.on("SIGTERM", () => {
  console.log("Graceful shutdown initiated (SIGTERM)...");
  isShuttingDown = true;
});

processJobs();