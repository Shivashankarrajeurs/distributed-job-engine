import express from "express";
import { createJob } from "./jobs.controller.js";
import { sendJobs } from "./jobs.controller.js";
//import { testing } from "./jobs.controller.js";
import { redis } from "../config/redis.js";
import { metrics } from "./jobs.controller.js";
import { cancelJob } from "./jobs.controller.js";
import { retryDeadJob } from "./jobs.controller.js";

const router = express.Router();

router.post("/jobs", createJob);
router.get("/getJobs",sendJobs);

router.get("/metrics",metrics);
router.delete("/jobs/:id",cancelJob);
router.post("/jobs/:id/retry", retryDeadJob);
  

export default router;
