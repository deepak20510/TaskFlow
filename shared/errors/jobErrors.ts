export class NonRetryableError extends Error {
  public readonly isNonRetryable = true;

  constructor(message: string) {
    super(message);
    this.name = "NonRetryableError";
  }
}

export class RetryableError extends Error {
  public readonly isRetryable = true;

  constructor(message: string) {
    super(message);
    this.name = "RetryableError";
  }
}

export function isNonRetryableError(error: any): boolean {
  return error?.isNonRetryable === true || error instanceof NonRetryableError;
}
