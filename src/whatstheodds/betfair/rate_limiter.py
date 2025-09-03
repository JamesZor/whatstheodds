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
    Rate limiter for Betfair Historical Data API
    Limit: 100 requests per 10 seconds
    """

    def __init__(
        self,
        max_requests: int = cfg.betfair_rate_limit.max_request,  # type: ignore[union-attr]
        time_window: int = cfg.betfair_rate_limit.time_window,  # type: ignore[union-attr]
    ):
        """
        Initialize rate limiter

        Args:
            max_requests: Maximum requests allowed (default 90 to leave buffer)
            time_window: Time window in seconds (default 10)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()  # type: ignore[var-annotated]
        self.lock = threading.Lock()  # Thread-safe

        logger.info(
            f"Rate limiter initialized: {max_requests} requests per {time_window}s"
        )

    def wait_if_needed(self) -> None:
        """
        Block if we would exceed rate limit, otherwise allow request to proceed
        """
        with self.lock:
            now = time.time()

            # Remove requests outside the time window
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()

            # Check if we need to wait
            if len(self.requests) >= self.max_requests:
                # Calculate how long to wait
                oldest_request = self.requests[0]
                wait_time = (
                    self.time_window - (now - oldest_request) + 0.1
                )  # Small buffer

                if wait_time > 0:
                    logger.info(f"Rate limit reached, waiting {wait_time:.2f}s")
                    # print(f"Rate limit reached, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)

                    # Clean up again after waiting
                    now = time.time()
                    while self.requests and self.requests[0] <= now - self.time_window:
                        self.requests.popleft()

            # Record this request
            self.requests.append(now)
            # print(
            #     f"Request allowed. Current count: {len(self.requests)}/{self.max_requests}"
            # )
            logger.debug(
                f"Request allowed. Current count: {len(self.requests)}/{self.max_requests}"
            )

    def decorator(self, func):
        """
        Decorator to automatically rate limit function calls
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            self.wait_if_needed()
            return func(*args, **kwargs)

        return wrapper

    def get_current_usage(self) -> dict:
        """
        Get current rate limiter status
        """
        with self.lock:
            now = time.time()
            # Clean old requests
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()

            return {
                "current_requests": len(self.requests),
                "max_requests": self.max_requests,
                "time_window": self.time_window,
                "usage_percent": (len(self.requests) / self.max_requests) * 100,
                "can_make_request": len(self.requests) < self.max_requests,
            }


# Global rate limiter instance (shared across all components)
betfair_rate_limiter = BetfairHistoricalRateLimiter()


# Decorator for easy use
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


# Context manager for manual control
class RateLimitedContext:
    """
    Context manager for rate limiting

    Usage:
        with RateLimitedContext():
            # Make API call here
            result = api.get_file_list(...)
    """

    def __init__(self, limiter: Optional[BetfairHistoricalRateLimiter] = None):
        self.limiter = limiter or betfair_rate_limiter

    def __enter__(self):
        self.limiter.wait_if_needed()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
