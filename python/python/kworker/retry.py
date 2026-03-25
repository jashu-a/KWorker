"""Retry policy configuration for failed tasks."""

from dataclasses import dataclass


@dataclass
class RetryPolicy:
    """Configures how failed tasks are retried.

    Uses exponential backoff: delay = backoff_base * 2^(attempt-1)
    capped at backoff_max seconds.

    Example progression with defaults (base=2, max=300):
        Attempt 1: 2s
        Attempt 2: 4s
        Attempt 3: 8s
        Attempt 4: 16s
        Attempt 5: 32s
        ...
        Attempt 8+: 300s (capped)
    """
    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 300.0  # 5 minutes

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate the backoff delay in seconds for a given attempt number."""
        delay = self.backoff_base * (2 ** (attempt - 1))
        return min(delay, self.backoff_max)

    def should_retry(self, attempt: int) -> bool:
        """Whether a task should be retried at this attempt number."""
        return attempt < self.max_attempts
