"""
API Resilience Utilities for Microsoft Fabric Monitoring.

This module provides robust API call handling with:
- Exponential backoff with jitter for rate limiting
- Circuit breaker pattern for cascading failure prevention
- Centralized retry configuration

Usage:
    from usf_fabric_monitoring.core.api_resilience import (
        RetryConfig,
        CircuitBreaker,
        make_resilient_request,
        exponential_backoff_with_jitter
    )
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TypeVar

import requests

logger = logging.getLogger(__name__)

T = TypeVar("T")


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass
class RetryConfig:
    """Configuration for API retry behavior."""

    max_retries: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 120.0
    jitter_factor: float = 0.5  # Random jitter up to 50% of delay
    retry_status_codes: tuple = (429, 500, 502, 503, 504)
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> RetryConfig:
        """Load configuration from environment variables."""
        return cls(
            max_retries=int(os.getenv("API_MAX_RETRIES", "5")),
            base_delay_seconds=float(os.getenv("API_BASE_DELAY", "1.0")),
            max_delay_seconds=float(os.getenv("API_MAX_DELAY", "120.0")),
            jitter_factor=float(os.getenv("API_JITTER_FACTOR", "0.5")),
            timeout_seconds=int(os.getenv("API_REQUEST_TIMEOUT", "30")),
        )


# =============================================================================
# EXPONENTIAL BACKOFF
# =============================================================================


def exponential_backoff_with_jitter(
    attempt: int, base_delay: float = 1.0, max_delay: float = 120.0, jitter_factor: float = 0.5
) -> float:
    """
    Calculate delay with exponential backoff and random jitter.

    This prevents the "thundering herd" problem when many clients retry
    at exactly the same time after a rate limit.

    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        jitter_factor: Random jitter factor (0.0 to 1.0)

    Returns:
        Delay in seconds with jitter applied

    Example:
        >>> exponential_backoff_with_jitter(0)  # ~1.0-1.5s
        >>> exponential_backoff_with_jitter(1)  # ~2.0-3.0s
        >>> exponential_backoff_with_jitter(2)  # ~4.0-6.0s
    """
    # Calculate base exponential delay: 2^attempt * base_delay
    delay = min(base_delay * (2**attempt), max_delay)

    # Add random jitter
    jitter = delay * jitter_factor * random.random()

    return delay + jitter


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "CLOSED"  # Normal operation, requests allowed
    OPEN = "OPEN"  # Failing, requests blocked
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered


@dataclass
class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.

    When too many requests fail, the circuit "opens" and blocks further
    requests for a cooldown period. After cooldown, it allows a test
    request through (half-open state) to check if the service recovered.

    Attributes:
        name: Identifier for this circuit breaker
        failure_threshold: Number of failures before opening
        recovery_timeout: Seconds to wait before half-open test
        success_threshold: Successes in half-open to close circuit
    """

    name: str = "default"
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 2

    # Internal state
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: datetime | None = field(default=None, init=False)

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, checking for timeout."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = (datetime.now() - self._last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    logger.info(f"Circuit '{self.name}' entering HALF_OPEN state after {elapsed:.0f}s")
        return self._state

    def is_request_allowed(self) -> bool:
        """Check if a request should be allowed through."""
        return self.state != CircuitState.OPEN

    def record_success(self) -> None:
        """Record a successful request."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                logger.info(f"Circuit '{self.name}' CLOSED after {self._success_count} successes")
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed request."""
        self._failure_count += 1
        self._last_failure_time = datetime.now()

        if self._state == CircuitState.HALF_OPEN:
            # Any failure in half-open immediately opens the circuit
            self._state = CircuitState.OPEN
            logger.warning(f"Circuit '{self.name}' OPENED (half-open test failed)")
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit '{self.name}' OPENED after {self._failure_count} failures")

    def reset(self) -> None:
        """Reset circuit to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""

    def __init__(self, circuit_name: str, recovery_time: float):
        self.circuit_name = circuit_name
        self.recovery_time = recovery_time
        super().__init__(f"Circuit '{circuit_name}' is OPEN. Try again in {recovery_time:.0f} seconds.")


# =============================================================================
# RESILIENT REQUEST FUNCTION
# =============================================================================


def make_resilient_request(
    session: requests.Session,
    method: str,
    url: str,
    config: RetryConfig | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    **kwargs,
) -> requests.Response:
    """
    Make an HTTP request with automatic retry and circuit breaker protection.

    Args:
        session: Requests session to use
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        config: Retry configuration (uses defaults if None)
        circuit_breaker: Circuit breaker instance (optional)
        **kwargs: Additional arguments passed to session.request()

    Returns:
        Response object from successful request

    Raises:
        CircuitBreakerOpen: If circuit is open
        requests.RequestException: On final failure after retries
    """
    config = config or RetryConfig.from_env()

    # Check circuit breaker
    if circuit_breaker and not circuit_breaker.is_request_allowed():
        elapsed = 0.0
        if circuit_breaker._last_failure_time:
            elapsed = (datetime.now() - circuit_breaker._last_failure_time).total_seconds()
        remaining = max(0, circuit_breaker.recovery_timeout - elapsed)
        raise CircuitBreakerOpen(circuit_breaker.name, remaining)

    # Set timeout if not provided
    kwargs.setdefault("timeout", config.timeout_seconds)

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            response = session.request(method, url, **kwargs)

            # Check if we should retry based on status code
            if response.status_code in config.retry_status_codes:
                # Get retry-after header if available (for 429s)
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after) + 1  # Add 1 second buffer
                    except ValueError:
                        delay = exponential_backoff_with_jitter(
                            attempt, config.base_delay_seconds, config.max_delay_seconds, config.jitter_factor
                        )
                else:
                    delay = exponential_backoff_with_jitter(
                        attempt, config.base_delay_seconds, config.max_delay_seconds, config.jitter_factor
                    )

                if attempt < config.max_retries:
                    logger.warning(
                        f"Request to {url} returned {response.status_code}. "
                        f"Retrying in {delay:.1f}s (attempt {attempt + 1}/{config.max_retries + 1})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    # Final attempt failed
                    if circuit_breaker:
                        circuit_breaker.record_failure()
                    response.raise_for_status()

            # Success!
            if circuit_breaker:
                circuit_breaker.record_success()
            return response

        except requests.exceptions.Timeout as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = exponential_backoff_with_jitter(
                    attempt, config.base_delay_seconds, config.max_delay_seconds, config.jitter_factor
                )
                logger.warning(f"Request timeout. Retrying in {delay:.1f}s")
                time.sleep(delay)
            else:
                if circuit_breaker:
                    circuit_breaker.record_failure()

        except requests.exceptions.ConnectionError as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = exponential_backoff_with_jitter(
                    attempt, config.base_delay_seconds, config.max_delay_seconds, config.jitter_factor
                )
                logger.warning(f"Connection error. Retrying in {delay:.1f}s")
                time.sleep(delay)
            else:
                if circuit_breaker:
                    circuit_breaker.record_failure()

    # All retries exhausted
    raise last_exception or requests.exceptions.RequestException(
        f"Request to {url} failed after {config.max_retries + 1} attempts"
    )


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


def get_default_circuit_breaker(name: str = "fabric_api") -> CircuitBreaker:
    """Get a circuit breaker with sensible defaults for Fabric APIs."""
    return CircuitBreaker(
        name=name,
        failure_threshold=int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "5")),
        recovery_timeout=float(os.getenv("CIRCUIT_RECOVERY_TIMEOUT", "60.0")),
        success_threshold=int(os.getenv("CIRCUIT_SUCCESS_THRESHOLD", "2")),
    )


def sleep_with_jitter(base_seconds: float, jitter_factor: float = 0.25) -> None:
    """Sleep for a duration with random jitter to prevent thundering herd."""
    jitter = base_seconds * jitter_factor * random.random()
    time.sleep(base_seconds + jitter)
