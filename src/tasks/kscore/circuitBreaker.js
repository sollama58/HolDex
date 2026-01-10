/**
 * Helius API Circuit Breaker
 *
 * Prevents cascading failures when Helius API is overloaded or down.
 * Three states: CLOSED (normal) -> OPEN (blocking) -> HALF_OPEN (testing)
 */

const logger = require('../../services/logger');

// Circuit breaker state
const circuitBreaker = {
    failures: 0,
    lastFailure: 0,
    state: 'closed', // 'closed', 'open', 'half-open'
    threshold: 5,     // Open after 5 failures
    cooldown: 30000,  // 30 seconds before trying again
    halfOpenRequests: 0,
    halfOpenMax: 2    // Test with 2 requests in half-open
};

/**
 * Check if requests should be allowed through the circuit breaker
 */
function checkCircuitBreaker() {
    const now = Date.now();

    if (circuitBreaker.state === 'open') {
        // Check if cooldown has passed
        if (now - circuitBreaker.lastFailure > circuitBreaker.cooldown) {
            circuitBreaker.state = 'half-open';
            circuitBreaker.halfOpenRequests = 0;
            logger.info('[CircuitBreaker] Entering half-open state (testing)');
            return true;
        }
        return false;
    }

    if (circuitBreaker.state === 'half-open') {
        if (circuitBreaker.halfOpenRequests >= circuitBreaker.halfOpenMax) {
            return false;
        }
        circuitBreaker.halfOpenRequests++;
        return true;
    }

    return true; // closed state - allow all
}

/**
 * Record a successful request
 */
function recordSuccess() {
    if (circuitBreaker.state === 'half-open') {
        circuitBreaker.state = 'closed';
        circuitBreaker.failures = 0;
        logger.info('[CircuitBreaker] Closed - Helius recovered');
    }
    circuitBreaker.failures = 0;
}

/**
 * Record a failed request
 */
function recordFailure() {
    circuitBreaker.failures++;
    circuitBreaker.lastFailure = Date.now();

    if (circuitBreaker.state === 'half-open') {
        circuitBreaker.state = 'open';
        logger.warn('[CircuitBreaker] Reopened after test failure');
    } else if (circuitBreaker.failures >= circuitBreaker.threshold) {
        circuitBreaker.state = 'open';
        logger.warn(`[CircuitBreaker] Opened after ${circuitBreaker.failures} failures`);
    }
}

/**
 * Get current circuit breaker state for health checks
 */
function getState() {
    return {
        state: circuitBreaker.state,
        failures: circuitBreaker.failures,
        threshold: circuitBreaker.threshold,
        lastFailure: circuitBreaker.lastFailure > 0
            ? new Date(circuitBreaker.lastFailure).toISOString()
            : null,
        cooldownMs: circuitBreaker.cooldown
    };
}

/**
 * Force reset the circuit breaker (for admin use)
 */
function reset() {
    circuitBreaker.state = 'closed';
    circuitBreaker.failures = 0;
    circuitBreaker.halfOpenRequests = 0;
    logger.info('[CircuitBreaker] Manually reset');
}

module.exports = {
    checkCircuitBreaker,
    recordSuccess,
    recordFailure,
    getState,
    reset,
    // Expose internal state for testing
    _circuitBreaker: circuitBreaker
};
