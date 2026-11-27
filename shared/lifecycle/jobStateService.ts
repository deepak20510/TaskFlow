import prisma from "../../api-service/dbclient";
import { Job, JobStatus } from "../../generated/prisma/client";

export class JobStateService {
  /**
   * Safe conditional transition: QUEUED -> PROCESSING
   * Returns the updated Job record if claimed, or null if already taken.
   */
  static async transitionToProcessing(
    jobId: string,
    workerId: string,
  ): Promise<Job | null> {
    const updated = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "QUEUED",
      },
      data: {
        status: "PROCESSING",
        startedAt: new Date(),
        workerId,
        errorMessage: null,
      },
    });

    if (updated.count === 0) {
      return null;
    }

    return await prisma.job.findUnique({
      where: { id: jobId },
    });
  }

  /**
   * Revert job state if lock acquisition or validation fails right after claiming
   */
  static async revertToQueued(jobId: string, workerId: string): Promise<boolean> {
    const reverted = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "PROCESSING",
        workerId,
      },
      data: {
        status: "QUEUED",
        startedAt: null,
        workerId: null,
      },
    });

    return reverted.count > 0;
  }

  /**
   * Safe conditional transition: PROCESSING -> COMPLETED
   * Ensures that only the current processing worker can mark it completed.
   */
  static async transitionToCompleted(
    jobId: string,
    workerId: string,
  ): Promise<boolean> {
    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "PROCESSING",
        workerId,
      },
      data: {
        status: "COMPLETED",
        completedAt: new Date(),
        errorMessage: null,
      },
    });

    return result.count > 0;
  }

  /**
   * Safe conditional transition: PROCESSING -> QUEUED (Retry)
   */
  static async transitionToRetry(
    jobId: string,
    workerId: string,
    errorMessage: string,
    newAttempts: number,
  ): Promise<boolean> {
    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "PROCESSING",
        workerId,
      },
      data: {
        status: "QUEUED",
        attempts: newAttempts,
        errorMessage,
        startedAt: null,
        workerId: null,
      },
    });

    return result.count > 0;
  }

  /**
   * Safe conditional transition: PROCESSING -> DLQ
   */
  static async transitionToDLQ(
    jobId: string,
    workerId: string,
    errorMessage: string,
    finalAttempts: number,
  ): Promise<boolean> {
    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "PROCESSING",
        workerId,
      },
      data: {
        status: "DLQ",
        attempts: finalAttempts,
        errorMessage,
        failedAt: new Date(),
        startedAt: null,
        workerId: null,
      },
    });

    return result.count > 0;
  }

  /**
   * Replay a DLQ job back into QUEUED state
   */
  static async replayDLQJob(jobId: string): Promise<Job | null> {
    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: "DLQ",
      },
      data: {
        status: "QUEUED",
        attempts: 0,
        errorMessage: null,
        failedAt: null,
        startedAt: null,
        workerId: null,
      },
    });

    if (result.count === 0) {
      return null;
    }

    return await prisma.job.findUnique({
      where: { id: jobId },
    });
  }

  static async getJob(jobId: string): Promise<Job | null> {
    return await prisma.job.findUnique({
      where: { id: jobId },
    });
  }
}
