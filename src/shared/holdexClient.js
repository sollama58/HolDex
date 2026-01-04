/**
 * HOLDEX ORACLE CLIENT
 *
 * Client module for GASdf to communicate with HolDex Oracle API.
 * Copy this file to GASdf alongside harmony.js.
 *
 * Usage in GASdf:
 * const { HoldexClient } = require('./shared/holdexClient');
 * const client = new HoldexClient({
 *     baseUrl: process.env.HOLDEX_API_URL,
 *     webhookSecret: process.env.HOLDEX_WEBHOOK_SECRET
 * });
 *
 * const { accepted } = await client.checkKScore(mintAddress);
 * const { discount } = await client.getDiscount(wallet, 'gasdf_submit_standard');
 * await client.notifyBurn(wallet, amount, txSignature);
 */

'use strict';

const crypto = require('crypto');

const DEFAULT_BASE_URL = 'https://holdex.alonisthe.dev';
const DEFAULT_TIMEOUT = 10000;

class HoldexClient {
    constructor(options = {}) {
        this.baseUrl = (options.baseUrl || DEFAULT_BASE_URL).replace(/\/$/, '');
        this.timeout = options.timeout || DEFAULT_TIMEOUT;
        this.apiKey = options.apiKey || null;
        this.webhookSecret = options.webhookSecret || null;
        this.logger = options.logger || console;
    }

    /**
     * Sign payload with HMAC-SHA256 for webhook authentication
     */
    _signPayload(payload) {
        if (!this.webhookSecret) {
            throw new Error('webhookSecret is required for webhook operations');
        }
        const signature = crypto
            .createHmac('sha256', this.webhookSecret)
            .update(JSON.stringify(payload))
            .digest('hex');
        return `sha256=${signature}`;
    }

    /**
     * Make HTTP request to HolDex Oracle
     */
    async _request(method, path, body = null) {
        const url = `${this.baseUrl}/oracle${path}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const options = {
                method,
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'GASdf/1.0'
                },
                signal: controller.signal
            };

            if (this.apiKey) {
                options.headers['x-api-key'] = this.apiKey;
            }

            if (body) {
                options.body = JSON.stringify(body);
            }

            const response = await fetch(url, options);
            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Request failed');
            }

            return data.data;

        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error(`HolDex Oracle timeout: ${path}`);
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // K-SCORE: Token Acceptance
    // ═══════════════════════════════════════════════════════════════

    /**
     * Check if a token is accepted for payment (K-Score >= threshold)
     *
     * @param {string} mint - Token mint address
     * @returns {Promise<{
     *   mint: string,
     *   k_score: number,
     *   tier: string,
     *   accepted: boolean,
     *   reason: string | null
     * }>}
     */
    async checkKScore(mint) {
        return this._request('GET', `/kscore/${mint}`);
    }

    /**
     * Quick acceptance check (boolean only)
     */
    async isTokenAccepted(mint) {
        try {
            const result = await this.checkKScore(mint);
            return result.accepted;
        } catch (error) {
            this.logger.warn(`[HoldexClient] K-Score check failed: ${error.message}`);
            return false; // Fail closed
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // E-SCORE: Participant Benefits
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get E-Score and benefits for a wallet
     *
     * @param {string} wallet - Wallet address
     * @returns {Promise<{
     *   wallet: string,
     *   e_score: number,
     *   tier: { name, icon, color },
     *   benefits: { discount, freeCalls, rateLimit, priority }
     * }>}
     */
    async getEScore(wallet) {
        return this._request('GET', `/escore/${wallet}`);
    }

    /**
     * Get discount for a specific operation
     *
     * @param {string} wallet - Wallet address
     * @param {string} operation - Operation type (e.g., 'gasdf_submit_standard')
     * @returns {Promise<{
     *   wallet: string,
     *   operation: string,
     *   e_score: number,
     *   discounts: { theoretical, maxAllowed, effective },
     *   finalFee: number,
     *   baseFee: number,
     *   isViable: boolean
     * }>}
     */
    async getDiscount(wallet, operation) {
        return this._request('GET', `/discount/${wallet}/${operation}`);
    }

    // ═══════════════════════════════════════════════════════════════
    // OPERATION COSTS: Local Calculation Support
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get all operation costs and constants
     * Cache this locally for faster lookups
     *
     * @returns {Promise<{
     *   operations: { [type]: { baseFee, cost, minFee, maxDiscount } },
     *   constants: { PHI, RATIOS, SAFETY_MARGIN },
     *   acceptance: { threshold, hardcodedTokens }
     * }>}
     */
    async getOperationCosts() {
        return this._request('GET', '/costs');
    }

    // ═══════════════════════════════════════════════════════════════
    // BURN NOTIFICATIONS: Webhook to HolDex
    // ═══════════════════════════════════════════════════════════════

    /**
     * Notify HolDex of a burn transaction
     * This updates the user's E-Score with their contribution
     *
     * SECURITY: Requires webhookSecret to be configured for HMAC signing
     *
     * @param {string} wallet - Wallet that burned
     * @param {number} amount - Amount burned (in $ASDF)
     * @param {string} txSignature - Transaction signature for verification
     * @returns {Promise<{
     *   wallet: string,
     *   amount: number,
     *   newEScore: number,
     *   tier: { name, icon }
     * }>}
     */
    async notifyBurn(wallet, amount, txSignature) {
        const payload = {
            wallet,
            amount,
            txSignature,
            source: 'gasdf'
        };

        // Sign the payload with HMAC-SHA256
        const signature = this._signPayload(payload);

        const url = `${this.baseUrl}/oracle/webhook/burns`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'GASdf/1.0',
                    'x-holdex-signature': signature
                },
                body: JSON.stringify(payload),
                signal: controller.signal
            });

            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Burn notification failed');
            }

            return data.data;

        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error('HolDex Oracle timeout: burn notification');
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // PARTICIPANT MANAGEMENT
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get full participant profile
     */
    async getParticipant(wallet) {
        return this._request('GET', `/participant/${wallet}`);
    }

    /**
     * Register a new participant
     */
    async registerParticipant(wallet, type = 'user') {
        return this._request('POST', '/participant/register', {
            wallet,
            type
        });
    }

    // ═══════════════════════════════════════════════════════════════
    // STATS & LEADERBOARD
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get E-Score leaderboard
     */
    async getLeaderboard(limit = 50) {
        return this._request('GET', `/leaderboard?limit=${limit}`);
    }

    /**
     * Get ecosystem stats
     */
    async getStats() {
        return this._request('GET', '/stats');
    }

    // ═══════════════════════════════════════════════════════════════
    // HEALTH CHECK
    // ═══════════════════════════════════════════════════════════════

    /**
     * Check if HolDex Oracle is reachable
     */
    async healthCheck() {
        try {
            await this.getOperationCosts();
            return true;
        } catch (_error) {
            return false;
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// SINGLETON FACTORY
// ═══════════════════════════════════════════════════════════════

let instance = null;

function getHoldexClient(options = {}) {
    if (!instance) {
        instance = new HoldexClient(options);
    }
    return instance;
}

module.exports = {
    HoldexClient,
    getHoldexClient
};
