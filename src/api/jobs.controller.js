import { enqueueJob } from "../queue/queue.js";
//import { getAllJobs } from "../storage/jobRepository.js";
import { redis } from "../config/redis.js";
export async function createJob(req, res) {
  const { type, payload, priority } = req.body;

  const jobPriority = (priority || "NORMAL").toUpperCase();

  const validPriorities = ["HIGH", "NORMAL", "LOW"];

  if (!validPriorities.includes(jobPriority)) {
  return res.status(400).json({ error: "Invalid priority" });
 }

  if (!type) {
    return res.status(400).json({ error: "Job type is required" });
  }

  const job = await enqueueJob(type, payload || {},priority);

  res.status(201).json({
    message: "Job created",
    jobId: job.id
  });
}


export async function sendJobs(req, res) {
  const { status, priority } = req.query;

  const keys = await redis.keys("job:*");

  const jobs = [];

  for (const key of keys) {
    const job = await redis.hgetall(key);

    if (status && job.status !== status) continue;
    if (priority && job.priority !== priority) continue;

    jobs.push(job);
  }

  res.json({ jobs });
}



export async function metrics(req,res){

   const [
    processed,
    failed,
    retried,
    dead,
    totalTime,
    highLen,
    normalLen,
    lowLen
  ] = await Promise.all([
    redis.get("metrics:processed"),
    redis.get("metrics:failed"),
    redis.get("metrics:retried"),
    redis.get("metrics:dead"),
    redis.get("metrics:totalTime"),
    redis.llen("queue:high"),
    redis.llen("queue:normal"),
    redis.llen("queue:low"),

  ]);

  const queueLength = await redis.llen("queue:jobs");
  const processingLength = await redis.llen("queue:processing");

  const avgTime =
    processed > 0
      ? Math.round(totalTime / processed)
      : 0;

  res.json({
    processed: Number(processed || 0),
    failed: Number(failed || 0),
    retried: Number(retried || 0),
    dead: Number(dead || 0),
    queueLength,
    processingLength,
    avgProcessingTimeMs: avgTime,
    highQueue:highLen,
    normalQueue:normalLen,
    lowQueue:lowLen
  });
}


export async function cancelJob(req, res) {
  const { id } = req.params;

  const job = await redis.hgetall(`job:${id}`);
  if (!job || !job.id) {
    return res.status(404).json({ error: "Job not found" });
  }

  if (job.status === "COMPLETED" || job.status === "DEAD") {
    return res.status(400).json({ error: "Cannot cancel finished job" });
  }

  // Remove from queues
  await redis.lrem("queue:high", 0, id);
  await redis.lrem("queue:normal", 0, id);
  await redis.lrem("queue:low", 0, id);
  await redis.lrem("queue:processing", 0, id);
  await redis.zrem("queue:delayed", id);

  await redis.hset(`job:${id}`, {
    status: "CANCELLED",
    updatedAt: Date.now()
  });

  res.json({ message: "Job cancelled" });
}


export async function retryDeadJob(req, res) {
  const { id } = req.params;

  const job = await redis.hgetall(`job:${id}`);
  if (!job || !job.id) {
    return res.status(404).json({ error: "Job not found" });
  }

  if (job.status !== "DEAD") {
    return res.status(400).json({ error: "Only DEAD jobs can be retried" });
  }

  await redis.hset(`job:${id}`, {
    status: "PENDING",
    attempts: 0,
    updatedAt: Date.now()
  });

  const queueMap = {
    HIGH: "queue:high",
    NORMAL: "queue:normal",
    LOW: "queue:low"
  };

  await redis.lpush(queueMap[job.priority], id);

  res.json({ message: "Dead job requeued" });
}