import crypto from "crypto";
import { JOB_STATES } from "./jobStates.js";
import { redis } from "../config/redis.js";



export async function enqueueJob(type, payload,priority) {
  const now = Date.now();
   const id = crypto.randomUUID();

  const job = {
    id,
    type,
    payload:JSON.stringify(payload || {}),
    status: JOB_STATES.PENDING,
    attempts: 0,
    maxAttempts: 5,
    priority,
    nextRetryAt: "",
    executed: "false",
    lockedAt: "",
    createdAt: now,
    updatedAt: now
  };

  const queueMap = {
  HIGH: "queue:high",
  NORMAL: "queue:normal",
  LOW: "queue:low"
};

  await redis.hset(`job:${id}`, job);

  // Push job ID into queue list
  await redis.lpush(queueMap[priority], id);
  return job;
}



