"""
Tests for API Resilience Utilities.

Tests for exponential backoff, circuit breaker, and resilient request handling.
"""
import pytest
import time
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

import requests

from usf_fabric_monitoring.core.api_resilience import (
    RetryConfig,
    CircuitBreaker,
    CircuitState,
    CircuitBreakerOpen,
    exponential_backoff_with_jitter,
    make_resilient_request,
    get_default_circuit_breaker,
    sleep_with_jitter,
)


class TestExponentialBackoff:
    """Tests for exponential backoff calculation."""
    
    def test_backoff_increases_exponentially(self):
        """Delay should increase exponentially with attempts."""
        delay0 = exponential_backoff_with_jitter(0, base_delay=1.0, jitter_factor=0)
        delay1 = exponential_backoff_with_jitter(1, base_delay=1.0, jitter_factor=0)
        delay2 = exponential_backoff_with_jitter(2, base_delay=1.0, jitter_factor=0)
        
        assert delay0 == 1.0
        assert delay1 == 2.0
        assert delay2 == 4.0
    
    def test_backoff_respects_max_delay(self):
        """Delay should not exceed max_delay."""
        delay = exponential_backoff_with_jitter(10, base_delay=1.0, max_delay=10.0, jitter_factor=0)
        assert delay == 10.0
    
    def test_jitter_adds_randomness(self):
        """Jitter should add randomness to the delay."""
        delays = [
            exponential_backoff_with_jitter(1, base_delay=1.0, jitter_factor=0.5)
            for _ in range(10)
        ]
        # With jitter, not all delays should be identical
        assert len(set(delays)) > 1
    
    def test_jitter_bounds(self):
        """Jitter should keep delay within expected bounds."""
        for _ in range(100):
            # attempt=0 with base_delay=2.0 gives 2^0 * 2.0 = 2.0 base
            delay = exponential_backoff_with_jitter(0, base_delay=2.0, jitter_factor=0.5)
            # Base delay is 2.0, max jitter adds 0.5 * 2.0 = 1.0
            assert 2.0 <= delay <= 3.0


class TestCircuitBreaker:
    """Tests for circuit breaker pattern."""
    
    def test_initial_state_is_closed(self):
        """Circuit should start in CLOSED state."""
        cb = CircuitBreaker(name="test")
        assert cb.state == CircuitState.CLOSED
        assert cb.is_request_allowed()
    
    def test_opens_after_threshold_failures(self):
        """Circuit should OPEN after failure_threshold failures."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert not cb.is_request_allowed()
    
    def test_success_resets_failure_count(self):
        """Success should reset failure count in CLOSED state."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        
        # Count should be reset, so 2 more failures won't open
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
    
    def test_half_open_after_recovery_timeout(self):
        """Circuit should go HALF_OPEN after recovery timeout."""
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.1)
        
        cb.record_failure()
        assert cb._state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(0.15)
        
        # State check should transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.is_request_allowed()
    
    def test_half_open_to_closed_on_success(self):
        """Enough successes in HALF_OPEN should close circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.01, success_threshold=2)
        
        cb.record_failure()
        time.sleep(0.02)
        
        cb.state  # Trigger state check
        assert cb._state == CircuitState.HALF_OPEN
        
        cb.record_success()
        assert cb._state == CircuitState.HALF_OPEN  # Not yet closed
        
        cb.record_success()
        assert cb._state == CircuitState.CLOSED  # Now closed
    
    def test_half_open_to_open_on_failure(self):
        """Failure in HALF_OPEN should immediately OPEN circuit."""
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.01)
        
        cb.record_failure()
        time.sleep(0.02)
        cb.state  # Trigger transition to HALF_OPEN
        
        cb.record_failure()
        assert cb._state == CircuitState.OPEN
    
    def test_reset(self):
        """Reset should return circuit to closed state."""
        cb = CircuitBreaker(name="test", failure_threshold=1)
        
        cb.record_failure()
        assert cb._state == CircuitState.OPEN
        
        cb.reset()
        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == 0


class TestCircuitBreakerOpen:
    """Tests for CircuitBreakerOpen exception."""
    
    def test_exception_message(self):
        """Exception should have informative message."""
        exc = CircuitBreakerOpen("test_circuit", 30.0)
        assert "test_circuit" in str(exc)
        assert "30" in str(exc)
        assert exc.circuit_name == "test_circuit"
        assert exc.recovery_time == 30.0


class TestRetryConfig:
    """Tests for RetryConfig."""
    
    def test_default_values(self):
        """Config should have sensible defaults."""
        config = RetryConfig()
        assert config.max_retries == 5
        assert config.base_delay_seconds == 1.0
        assert 429 in config.retry_status_codes
    
    @patch.dict("os.environ", {"API_MAX_RETRIES": "10", "API_BASE_DELAY": "2.0"})
    def test_from_env(self):
        """Config should load from environment variables."""
        config = RetryConfig.from_env()
        assert config.max_retries == 10
        assert config.base_delay_seconds == 2.0


class TestMakeResilientRequest:
    """Tests for make_resilient_request function."""
    
    def test_successful_request(self):
        """Successful request should return response."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        session.request.return_value = response
        
        result = make_resilient_request(session, "GET", "https://example.com")
        
        assert result == response
        session.request.assert_called_once()
    
    def test_retries_on_429(self):
        """Should retry on 429 status code."""
        session = MagicMock()
        
        # First call returns 429, second returns 200
        response_429 = MagicMock()
        response_429.status_code = 429
        response_429.headers = {"Retry-After": "0.01"}
        
        response_200 = MagicMock()
        response_200.status_code = 200
        
        session.request.side_effect = [response_429, response_200]
        
        config = RetryConfig(max_retries=2, base_delay_seconds=0.01)
        result = make_resilient_request(session, "GET", "https://example.com", config=config)
        
        assert result == response_200
        assert session.request.call_count == 2
    
    def test_circuit_breaker_blocks_when_open(self):
        """Should raise CircuitBreakerOpen when circuit is open."""
        session = MagicMock()
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=60.0)
        
        # Open the circuit
        cb.record_failure()
        
        with pytest.raises(CircuitBreakerOpen) as exc_info:
            make_resilient_request(session, "GET", "https://example.com", circuit_breaker=cb)
        
        assert "test" in str(exc_info.value)


class TestConvenienceFunctions:
    """Tests for convenience functions."""
    
    def test_get_default_circuit_breaker(self):
        """Should return a configured circuit breaker."""
        cb = get_default_circuit_breaker("test_api")
        assert cb.name == "test_api"
        assert isinstance(cb, CircuitBreaker)
    
    def test_sleep_with_jitter(self):
        """Should sleep for approximately the right amount of time."""
        start = time.time()
        sleep_with_jitter(0.1, jitter_factor=0.1)
        elapsed = time.time() - start
        
        # Should sleep between 0.1 and 0.11 seconds (with some tolerance)
        assert 0.09 <= elapsed <= 0.15
