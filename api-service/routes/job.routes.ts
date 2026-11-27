import { Router } from "express";
import {
  createBatchJobs,
  createJob,
  getJobById,
  listJobs,
  replayDLQJob,
} from "../controllers/job.Controller";
import { authMiddleware } from "../middleware/auth.middleware";

const router = Router();

// Apply authentication to all job endpoints
router.use(authMiddleware);

router.post("/", createJob);
router.post("/batch", createBatchJobs);
router.get("/", listJobs);
router.get("/:id", getJobById);
router.post("/:id/replay", replayDLQJob);

export default router;
