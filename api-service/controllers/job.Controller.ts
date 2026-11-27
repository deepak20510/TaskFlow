import { Request, Response } from "express";
import { v4 as uuidv4 } from "uuid";
import prisma from "../dbclient";
import { JobPriority } from "../../generated/prisma/index";
import { getPartitionedStream } from "../../shared/utils/partition";
import { OutboxPublisher } from "../../shared/outbox/outboxPublisher";
import { JobStateService } from "../../shared/lifecycle/jobStateService";
import {
  createJobSchema,
  createBatchJobsSchema,
  JobPriorityInput,
} from "../validators/job.validator";

const PRIORITY_DB_MAP: Record<JobPriorityInput, JobPriority> = {
  high: JobPriority.HIGH,
  medium: JobPriority.MEDIUM,
  low: JobPriority.LOW,
};

export async function createJob(req: Request, res: Response) {
  try {
    const parseResult = createJobSchema.safeParse(req.body);
    if (!parseResult.success) {
      const issues = parseResult.error.issues || [];
      const message = issues.length > 0 ? issues.map((e) => e.message).join(", ") : parseResult.error.message;
      return res.status(400).json({
        success: false,
        error: {
          code: "VALIDATION_ERROR",
          message,
          details: issues,
        },
      });
    }

    const validData = parseResult.data;
    // Derive verified user identity from authenticated request context
    const verifiedUserId = req.user?.userId || req.body.userId || "anonymous";
    const jobId = uuidv4();
    const outboxId = uuidv4();
    const streamName = getPartitionedStream(validData.priority, verifiedUserId);

    const jobPayload = {
      id: jobId,
      userId: verifiedUserId,
      type: validData.type,
      priority: validData.priority,
      payload: validData.payload,
      maxRetries: validData.maxRetries,
      attempts: 0,
      createdAt: new Date().toISOString(),
    };

    /**
     * TRANSACTIONAL OUTBOX:
     * Save Job entity and OutboxEvent atomically within PostgreSQL transaction
     */
    await prisma.$transaction(async (tx) => {
      await tx.job.create({
        data: {
          id: jobId,
          type: validData.type,
          userId: verifiedUserId,
          payload: validData.payload as any,
          priority: PRIORITY_DB_MAP[validData.priority],
          status: "QUEUED",
          attempts: 0,
          maxRetries: validData.maxRetries,
        },
      });

      await tx.outboxEvent.create({
        data: {
          id: outboxId,
          jobId,
          streamName,
          payload: jobPayload as any,
          status: "PENDING",
        },
      });
    });

    // Attempt immediate publication; non-blocking fallback to background outbox scanner
    void OutboxPublisher.publishEventImmediately(outboxId);

    return res.status(201).json({
      success: true,
      data: {
        jobId,
        status: "QUEUED",
        userId: verifiedUserId,
        priority: validData.priority,
      },
    });
  } catch (error: any) {
    console.error("[JobController] createJob error:", error);
    return res.status(500).json({
      success: false,
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Failed to create job",
      },
    });
  }
}

export async function createBatchJobs(req: Request, res: Response) {
  try {
    const parseResult = createBatchJobsSchema.safeParse(req.body);
    if (!parseResult.success) {
      const issues = parseResult.error.issues || [];
      const message = issues.length > 0 ? issues.map((e) => e.message).join(", ") : parseResult.error.message;
      return res.status(400).json({
        success: false,
        error: {
          code: "VALIDATION_ERROR",
          message,
          details: issues,
        },
      });
    }

    const { jobs } = parseResult.data;
    const verifiedUserId = req.user?.userId || req.body.userId || "anonymous";

    const preparedJobs = jobs.map((job) => {
      const jobId = uuidv4();
      const outboxId = uuidv4();
      const streamName = getPartitionedStream(job.priority, verifiedUserId);

      return {
        jobId,
        outboxId,
        streamName,
        data: {
          id: jobId,
          userId: verifiedUserId,
          type: job.type,
          priority: job.priority,
          payload: job.payload,
          maxRetries: job.maxRetries,
          attempts: 0,
          createdAt: new Date().toISOString(),
        },
      };
    });

    /**
     * Atomic batch transaction inserting all jobs and outbox events
     */
    await prisma.$transaction(async (tx) => {
      await tx.job.createMany({
        data: preparedJobs.map(({ data }) => ({
          id: data.id,
          type: data.type,
          userId: verifiedUserId,
          payload: data.payload as any,
          priority: PRIORITY_DB_MAP[data.priority as JobPriorityInput],
          status: "QUEUED",
          attempts: 0,
          maxRetries: data.maxRetries,
        })),
      });

      await tx.outboxEvent.createMany({
        data: preparedJobs.map(({ outboxId, jobId, streamName, data }) => ({
          id: outboxId,
          jobId,
          streamName,
          payload: data as any,
          status: "PENDING",
        })),
      });
    });

    // Trigger immediate async publication for the batch
    for (const { outboxId } of preparedJobs) {
      void OutboxPublisher.publishEventImmediately(outboxId);
    }

    return res.status(201).json({
      success: true,
      message: "Batch submission completed successfully",
      totalSubmitted: preparedJobs.length,
      jobs: preparedJobs.map((j) => ({
        jobId: j.jobId,
        priority: j.data.priority,
      })),
    });
  } catch (error: any) {
    console.error("[JobController] createBatchJobs error:", error);
    return res.status(500).json({
      success: false,
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Failed to process batch submission",
      },
    });
  }
}

export async function getJobById(req: Request, res: Response) {
  try {
    const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
    const verifiedUserId = req.user?.userId;

    const job = await prisma.job.findUnique({
      where: { id },
    });

    // Tenant isolation: allow only the job owner or admin to view the job
    if (!job || (verifiedUserId && job.userId !== verifiedUserId && req.user?.role !== "admin")) {
      return res.status(404).json({
        success: false,
        error: {
          code: "NOT_FOUND",
          message: "Job not found",
        },
      });
    }

    return res.status(200).json({
      success: true,
      data: job,
    });
  } catch (error: any) {
    console.error("[JobController] getJobById error:", error);
    return res.status(500).json({
      success: false,
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Failed to fetch job",
      },
    });
  }
}

export async function listJobs(req: Request, res: Response) {
  try {
    const verifiedUserId = req.user?.userId;
    const page = Math.max(1, parseInt(req.query.page as string, 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string, 10) || 20));
    const status = req.query.status as any;

    const where: any = {};
    if (verifiedUserId && req.user?.role !== "admin") {
      where.userId = verifiedUserId;
    }
    if (status) {
      where.status = status;
    }

    const [total, jobs] = await Promise.all([
      prisma.job.count({ where }),
      prisma.job.findMany({
        where,
        skip: (page - 1) * limit,
        take: limit,
        orderBy: { createdAt: "desc" },
      }),
    ]);

    return res.status(200).json({
      success: true,
      data: {
        total,
        page,
        limit,
        jobs,
      },
    });
  } catch (error: any) {
    console.error("[JobController] listJobs error:", error);
    return res.status(500).json({
      success: false,
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Failed to list jobs",
      },
    });
  }
}

export async function replayDLQJob(req: Request, res: Response) {
  try {
    const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
    const replayedJob = await JobStateService.replayDLQJob(id);

    if (!replayedJob) {
      return res.status(400).json({
        success: false,
        error: {
          code: "CANNOT_REPLAY",
          message: "Job is not in DLQ state or does not exist",
        },
      });
    }

    // Insert an Outbox event to dispatch the replayed job
    const streamName = getPartitionedStream(replayedJob.priority.toLowerCase(), replayedJob.userId);
    const outboxEvent = await prisma.outboxEvent.create({
      data: {
        jobId: replayedJob.id,
        streamName,
        payload: {
          id: replayedJob.id,
          userId: replayedJob.userId,
          type: replayedJob.type,
          priority: replayedJob.priority.toLowerCase(),
          payload: replayedJob.payload,
          maxRetries: replayedJob.maxRetries,
          attempts: 0,
          createdAt: new Date().toISOString(),
        },
        status: "PENDING",
      },
    });

    void OutboxPublisher.publishEventImmediately(outboxEvent.id);

    return res.status(200).json({
      success: true,
      message: "Job successfully replayed and queued for execution",
      data: replayedJob,
    });
  } catch (error: any) {
    console.error("[JobController] replayDLQJob error:", error);
    return res.status(500).json({
      success: false,
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Failed to replay DLQ job",
      },
    });
  }
}
