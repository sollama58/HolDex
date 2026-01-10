/**
 * Circuit Breaker Pattern Implementation
 *
 * Prevents cascading failures by stopping requests to failing services.
 * Three states: CLOSED (normal), OPEN (blocking), HALF_OPEN (testing)
 *
 * Usage:
 *   const breaker = createCircuitBreaker('database', { threshold: 5, cooldown: 30000 });
 *   const result = await breaker.execute(() => db.query(...));
 */

const logger = require('../services/logger');

// Global registry of circuit breakers for health checks
const circuitBreakers = new Map();

/**
 * Circuit Breaker States
 */
const STATES = {
    CLOSED: 'closed',     // Normal operation - requests pass through
    OPEN: 'open',         // Failing - requests are blocked
    HALF_OPEN: 'half-open' // Testing - limited requests allowed
};

/**
 * Create a new circuit breaker instance
 *
 * @param {string} name - Identifier for this circuit breaker
 * @param {Object} options - Configuration options
 * @param {number} options.threshold - Number of failures before opening circuit (default: 5)
 * @param {number} options.cooldown - Time in ms to wait before testing (default: 30000)
 * @param {number} options.halfOpenMax - Max test requests in half-open state (default: 2)
 * @param {number} options.successThreshold - Successes needed to close circuit (default: 2)
 * @param {Function} options.onStateChange - Callback when state changes
 */
function createCircuitBreaker(name, options = {}) {
    const config = {
        threshold: options.threshold || 5,
        cooldown: options.cooldown || 30000,
        halfOpenMax: options.halfOpenMax || 2,
        successThreshold: options.successThreshold || 2,
        onStateChange: options.onStateChange || null
    };

    const state = {
        current: STATES.CLOSED,
        failures: 0,
        successes: 0,
        lastFailure: 0,
        halfOpenRequests: 0,
        totalRequests: 0,
        totalFailures: 0,
        lastStateChange: Date.now()
    };

    function setState(newState) {
        if (state.current !== newState) {
            const oldState = state.current;
            state.current = newState;
            state.lastStateChange = Date.now();

            logger.info(`[CircuitBreaker:${name}] State: ${oldState} -> ${newState}`);

            if (config.onStateChange) {
                config.onStateChange(name, oldState, newState);
            }
        }
    }

    /**
     * Check if request should be allowed
     */
    function canExecute() {
        const now = Date.now();

        if (state.current === STATES.OPEN) {
            // Check if cooldown period has passed
            if (now - state.lastFailure > config.cooldown) {
                setState(STATES.HALF_OPEN);
                state.halfOpenRequests = 0;
                state.successes = 0;
                return true;
            }
            return false;
        }

        if (state.current === STATES.HALF_OPEN) {
            if (state.halfOpenRequests >= config.halfOpenMax) {
                return false;
            }
            state.halfOpenRequests++;
            return true;
        }

        return true; // CLOSED state
    }

    /**
     * Record a successful operation
     */
    function recordSuccess() {
        state.totalRequests++;

        if (state.current === STATES.HALF_OPEN) {
            state.successes++;
            if (state.successes >= config.successThreshold) {
                setState(STATES.CLOSED);
                state.failures = 0;
                state.successes = 0;
            }
        } else {
            state.failures = 0; // Reset on success in closed state
        }
    }

    /**
     * Record a failed operation
     */
    function recordFailure(error) {
        state.failures++;
        state.totalFailures++;
        state.totalRequests++;
        state.lastFailure = Date.now();

        if (state.current === STATES.HALF_OPEN) {
            setState(STATES.OPEN);
            logger.warn(`[CircuitBreaker:${name}] Reopened after test failure: ${error?.message || 'unknown'}`);
        } else if (state.failures >= config.threshold) {
            setState(STATES.OPEN);
            logger.warn(`[CircuitBreaker:${name}] Opened after ${state.failures} failures: ${error?.message || 'unknown'}`);
        }
    }

    /**
     * Execute a function with circuit breaker protection
     *
     * @param {Function} fn - Async function to execute
     * @param {*} fallback - Optional fallback value if circuit is open
     * @returns {Promise<*>} Result of fn or fallback
     */
    async function execute(fn, fallback = null) {
        if (!canExecute()) {
            logger.debug(`[CircuitBreaker:${name}] Request blocked (circuit open)`);
            if (fallback !== null) {
                return typeof fallback === 'function' ? fallback() : fallback;
            }
            throw new Error(`Circuit breaker ${name} is open`);
        }

        try {
            const result = await fn();
            recordSuccess();
            return result;
        } catch (error) {
            recordFailure(error);
            if (fallback !== null) {
                return typeof fallback === 'function' ? fallback() : fallback;
            }
            throw error;
        }
    }

    /**
     * Get current state information
     */
    function getState() {
        return {
            name,
            state: state.current,
            failures: state.failures,
            totalRequests: state.totalRequests,
            totalFailures: state.totalFailures,
            lastFailure: state.lastFailure,
            lastStateChange: state.lastStateChange,
            config: {
                threshold: config.threshold,
                cooldown: config.cooldown
            }
        };
    }

    /**
     * Force reset the circuit breaker
     */
    function reset() {
        setState(STATES.CLOSED);
        state.failures = 0;
        state.successes = 0;
        state.halfOpenRequests = 0;
        logger.info(`[CircuitBreaker:${name}] Manually reset`);
    }

    const breaker = {
        execute,
        canExecute,
        recordSuccess,
        recordFailure,
        getState,
        reset,
        get isOpen() { return state.current === STATES.OPEN; },
        get isClosed() { return state.current === STATES.CLOSED; },
        get isHalfOpen() { return state.current === STATES.HALF_OPEN; }
    };

    // Register in global registry
    circuitBreakers.set(name, breaker);

    return breaker;
}

/**
 * Get a circuit breaker by name
 */
function getCircuitBreaker(name) {
    return circuitBreakers.get(name);
}

/**
 * Get all circuit breaker states (for health check endpoint)
 */
function getAllCircuitBreakerStates() {
    const states = {};
    for (const [name, breaker] of circuitBreakers) {
        states[name] = breaker.getState();
    }
    return states;
}

/**
 * Check if any circuit breaker is open (system degraded)
 */
function isSystemDegraded() {
    for (const [_name, breaker] of circuitBreakers) {
        if (breaker.isOpen) {
            return true;
        }
    }
    return false;
}

module.exports = {
    createCircuitBreaker,
    getCircuitBreaker,
    getAllCircuitBreakerStates,
    isSystemDegraded,
    STATES
};
