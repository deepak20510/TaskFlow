import { RedisStreamQueue } from "../../shared/redis/streamQueue";
import { getPartitionedStream } from "../../shared/utils/partition";

export async function addJobToQueue(queueOrStreamName: string, job: any): Promise<string> {
  // If a legacy queue name was provided (e.g., 'high_priority_queue:0'), translate to stream name
  const streamName = queueOrStreamName.includes("_priority_queue:")
    ? queueOrStreamName.replace("_priority_queue:", "_priority_stream:")
    : queueOrStreamName;

  return await RedisStreamQueue.publishJob(streamName, {
    id: job.id,
    userId: job.userId,
    type: job.type,
    payload: job.payload,
    priority: job.priority,
    maxRetries: job.maxRetries,
    attempts: job.attempts,
    fencingToken: job.fencingToken,
    createdAt: job.createdAt,
  });
}

export async function addJobToStreamByPriority(
  priority: string,
  userId: string,
  job: any,
): Promise<string> {
  const streamName = getPartitionedStream(priority, userId);
  return addJobToQueue(streamName, job);
}
