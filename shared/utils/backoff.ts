export interface BackoffOptions {
  baseDelayMs?: number;
  maxDelayMs?: number;
  enableJitter?: boolean;
}

/**
 * Calculates exponential backoff delay with randomized jitter:
 * delay = min(maxDelay, baseDelay * 2^(attempt - 1)) + random_jitter
 */
export function calculateBackoffDelay(
  attempt: number,
  options: BackoffOptions = {},
): number {
  const baseDelay = options.baseDelayMs ?? 1000;
  const maxDelay = options.maxDelayMs ?? 30000;
  const enableJitter = options.enableJitter ?? true;

  // Normalized attempt count starting at 1
  const safeAttempt = Math.max(1, attempt);

  // Exponential factor: 2^(attempt - 1)
  const exponential = Math.min(
    maxDelay,
    baseDelay * Math.pow(2, safeAttempt - 1),
  );

  if (!enableJitter) {
    return exponential;
  }

  // Full jitter: uniformly distributed random delay between 0 and exponential factor
  const jitter = Math.floor(Math.random() * (baseDelay * 0.5));
  return Math.min(maxDelay, exponential + jitter);
}
