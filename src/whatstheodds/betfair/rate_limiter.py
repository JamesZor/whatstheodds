import logging
import threading
import time
from collections import deque
from functools import wraps
from typing import Optional

from ..utils import load_config

cfg = load_config()

logger = logging.getLogger(__name__)


class BetfairHistoricalRateLimiter:
    """
    Rate limiter for Betfair Historical Data API.
    Manages both request rate (e.g., 100 reqs/10 sec) and the number of
    concurrently active downloads.
    """

    def __init__(
        self,
        max_requests: int = cfg.betfair_rate_limit.max_request,  # type: ignore[union-attr]
        time_window: int = cfg.betfair_rate_limit.time_window,  # type: ignore[union-attr]
        # NEW: Add a limit for concurrent downloads from the config.
        max_concurrent: int = cfg.betfair_rate_limit.get("max_concurrent", 22),  # type: ignore[union-attr]
    ):
        """
        Initialize rate limiter

        Args:
            max_requests: Maximum requests allowed in the time window.
            time_window: The time window in seconds.
            max_concurrent: Maximum number of simultaneously active downloads.
        """
        # Time-window rate limiting attributes
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()

        # NEW: Add a semaphore for concurrency control.
        self.max_concurrent = max_concurrent
        self.concurrency_limiter = threading.Semaphore(self.max_concurrent)

        logger.info(
            f"Rate limiter initialized: {max_requests} requests per {time_window}s, "
            f"with a max of {self.max_concurrent} concurrent downloads."
        )

    def wait_if_needed(self) -> None:
        """
        Blocks if the number of recent requests would exceed the rate limit.
        This handles the "requests per time window" limit.
        """
        with self.lock:
            now = time.time()
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()

            if len(self.requests) >= self.max_requests:
                oldest_request = self.requests[0]
                wait_time = self.time_window - (now - oldest_request) + 0.1
                if wait_time > 0:
                    logger.info(f"Time-window limit reached, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    now = time.time()
                    while self.requests and self.requests[0] <= now - self.time_window:
                        self.requests.popleft()

            self.requests.append(now)
            logger.debug(
                f"Request allowed. Window count: {len(self.requests)}/{self.max_requests}"
            )

    def get_current_usage(self) -> dict:
        """
        Get current rate limiter status for both time-window and concurrency.
        """
        with self.lock:
            now = time.time()
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()

            # NEW: Calculate current number of active concurrent downloads.
            # The semaphore's internal value tracks available slots.
            # (max_concurrent - available_slots) gives us the active count.
            active_concurrent = self.max_concurrent - self.concurrency_limiter._value

            return {
                "current_requests_in_window": len(self.requests),
                "max_requests_in_window": self.max_requests,
                "time_window": self.time_window,
                "window_usage_percent": (len(self.requests) / self.max_requests) * 100,
                # NEW: Add concurrency status to the output.
                "active_concurrent_downloads": active_concurrent,
                "max_concurrent_downloads": self.max_concurrent,
                "concurrent_usage_percent": (active_concurrent / self.max_concurrent)
                * 100,
            }


# Global rate limiter instance (no changes needed here)
betfair_rate_limiter = BetfairHistoricalRateLimiter()


# Context manager for manual control (MODIFIED)
class RateLimitedContext:
    """
    Context manager that applies both time-window and concurrency rate limiting.
    """

    def __init__(self, limiter: Optional[BetfairHistoricalRateLimiter] = None):
        self.limiter = limiter or betfair_rate_limiter

    def __enter__(self):
        # Step 1: Limit the rate of starting new requests.
        self.limiter.wait_if_needed()

        # Step 2: Limit the number of active downloads. This will block if
        # all concurrency slots are currently in use.
        logger.debug("Acquiring concurrency slot...")
        self.limiter.concurrency_limiter.acquire()
        logger.debug("Concurrency slot acquired.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Step 3: Release the concurrency slot so another thread can proceed.
        # This is guaranteed to run when the 'with' block is exited.
        self.limiter.concurrency_limiter.release()
        logger.debug("Concurrency slot released.")


# The decorator is less ideal for managing start/end events of a long-running
# task, so the context manager is the recommended approach. No changes are
# needed to the decorator itself.
def rate_limited(limiter: Optional[BetfairHistoricalRateLimiter] = None):
    """
    Decorator to rate limit any function

    Usage:
        @rate_limited()
        def some_api_call():
            # This will be rate limited
            pass
    """
    if limiter is None:
        limiter = betfair_rate_limiter

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter.wait_if_needed()
            return func(*args, **kwargs)

        return wrapper

    return decorator
