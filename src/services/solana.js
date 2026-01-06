const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection = null;

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const TOKEN_2022_PROGRAM_ID = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');

/**
 * Creates a NEW Connection instance.
 *
 * SECURITY NOTE: WebSocket connections require API key in URL (protocol limitation).
 * Most WS clients don't support custom headers during handshake.
 * HTTP RPC calls should use Authorization headers where possible.
 */
function createConnection() {
    const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';

    // PRIORITY 1: Explicit Env Var
    let wsUrl = config.SOLANA_WSS_URL;

    // PRIORITY 2: Helius API Key (Most Robust)
    // NOTE: WSS requires API key in URL - this is a WebSocket protocol limitation
    // The key is only sent during handshake, not visible in subsequent frames
    if (!wsUrl && config.HELIUS_API_KEY) {
        wsUrl = `wss://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
    }

    // PRIORITY 3: Auto-Derive from RPC URL
    if (!wsUrl) {
        if (rpcUrl.includes('helius') || rpcUrl.includes('quicknode') || rpcUrl.includes('alchemy') || rpcUrl.includes('devnet')) {
             wsUrl = rpcUrl.replace('https://', 'wss://').replace('http://', 'ws://');
        }
    }

    // --- CRITICAL CHECK ---
    if (!wsUrl && process.env.SERVICE_TYPE === 'listener') {
        logger.warn("⚠️  WARNING: No explicit WSS URL found. Listeners might fail on Public RPC.");
    }

    const confirmTimeout = 60000;

    const conn = new Connection(rpcUrl, {
        commitment: 'confirmed',
        confirmTransactionInitialTimeout: confirmTimeout,
        wsEndpoint: wsUrl,
        disableRetryOnRateLimit: false,
    });
    
    // Logging (Masked)
    const logUrl = rpcUrl.replace(/\?api-key=[^&]+/, '?api-key=***');
    const logWs = wsUrl ? wsUrl.replace(/\?api-key=[^&]+/, '?api-key=***') : 'Standard (Auto)';
    
    // Log at INFO level so we can see it in production logs
    logger.info(`🔌 Solana Connection Init: ${logUrl}`);
    logger.info(`🔌 WSS Endpoint: ${logWs}`);

    return conn;
}

/**
 * Singleton connection provider.
 * @param {boolean} forceNew - If true, creates a fresh connection instance (useful for listener restarts)
 */
function getSolanaConnection(forceNew = false) {
    if (!connection || forceNew) {
        connection = createConnection();
    }
    return connection;
}

/**
 * Retry RPC calls with exponential backoff and jitter
 * @param {Function} fn - Async function to retry
 * @param {number} retries - Max retry attempts (default: 5)
 * @param {number} baseDelay - Base delay in ms (default: 1000)
 */
async function retryRPC(fn, retries = 5, baseDelay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (e) {
            // Don't retry client errors (invalid params, etc.)
            if (e.message && (e.message.includes('400') || e.message.includes('Invalid param'))) throw e;

            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'));
            if (i === retries - 1) throw e;

            // Exponential backoff with jitter to avoid thundering herd
            const exponentialDelay = baseDelay * Math.pow(2, i);
            const jitter = Math.random() * baseDelay * 0.5; // 0-50% of baseDelay
            const waitTime = isRateLimit
                ? exponentialDelay * 3 + jitter  // Extra delay for rate limits
                : exponentialDelay + jitter;

            logger.debug(`[RPC] Retry ${i + 1}/${retries} in ${Math.round(waitTime)}ms: ${e.message?.slice(0, 50)}`);
            await new Promise(r => setTimeout(r, waitTime));
        }
    }
}

async function fetchAccountsForProgram(conn, programId, mintAddress) {
    try {
        const filters = [
            programId.equals(TOKEN_PROGRAM_ID) ? { dataSize: 165 } : null,
            { memcmp: { offset: 0, bytes: mintAddress } }
        ].filter(Boolean);

        let accounts = await retryRPC(() => conn.getProgramAccounts(programId, {
            filters: filters,
            dataSlice: { offset: 64, length: 8 }
        }), 2, 500); 

        let activeHolders = 0;
        if (accounts) {
            for (const acc of accounts) {
                if (acc.account.data && acc.account.data.length === 8) {
                    const balance = acc.account.data.readBigUInt64LE(0);
                    if (balance > 0n) activeHolders++;
                }
            }
        }
        return activeHolders;
    } catch (_e) {
        return 0;
    }
}

/**
 * Get holder count using Helius API (paginated, works for any token size)
 * Falls back to standard RPC if Helius unavailable
 */
async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    const cleanMint = mintAddress.trim();

    // Try Helius API first (paginated, works for millions of holders)
    if (config.HELIUS_API_KEY) {
        try {
            let count = 0;
            let cursor = null;
            // SECURITY: Use base URL without API key - key goes in Authorization header
            const HELIUS_RPC = 'https://mainnet.helius-rpc.com';
            const headers = {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${config.HELIUS_API_KEY}`
            };

            // Paginate through holders (cap at 10 pages = 10k holders for efficiency)
            // Large tokens (USDT/USDC) have millions - sampling top 10k is sufficient
            const MAX_PAGES = 10;
            let page = 0;

            while (page < MAX_PAGES) {
                const params = { mint: cleanMint, limit: 1000 };
                if (cursor) params.cursor = cursor;

                const response = await fetch(HELIUS_RPC, {
                    method: 'POST',
                    headers,
                    body: JSON.stringify({
                        jsonrpc: '2.0',
                        id: Date.now(),
                        method: 'getTokenAccounts',
                        params
                    })
                });

                const data = await response.json();
                if (data.error || !data.result?.token_accounts) break;

                // Count accounts with balance > 0
                for (const acc of data.result.token_accounts) {
                    if (acc.amount > 0) count++;
                }

                cursor = data.result.cursor;
                page++;
                if (!cursor) break;
            }

            if (count > 0) {
                return count;
            }
        } catch (e) {
            logger.warn(`[Holders] Helius API failed for ${cleanMint.slice(0,8)}: ${e.message}`);
        }
    }

    // Fallback to standard RPC (may fail for large tokens)
    const conn = getSolanaConnection();
    let count = await fetchAccountsForProgram(conn, TOKEN_PROGRAM_ID, cleanMint);
    if (count === 0) {
        const count2022 = await fetchAccountsForProgram(conn, TOKEN_2022_PROGRAM_ID, cleanMint);
        count += count2022;
    }
    return count;
}

async function analyzeTokenHolders(mintAddress, excludeAddresses = []) {
    const conn = getSolanaConnection();
    try {
        const mint = new PublicKey(mintAddress);
        const largest = await retryRPC(() => conn.getTokenLargestAccounts(mint), 2, 2000);
        if (!largest || !largest.value || largest.value.length === 0) return { avgHoldHours: 0 };

        const topAccounts = largest.value;
        const nowSec = Math.floor(Date.now() / 1000);
        let totalDuration = 0;
        let validSamples = 0;
        const excludeSet = new Set(excludeAddresses.map(a => a ? a.toString() : ''));

        for (const acc of topAccounts) {
            if (validSamples >= 15) break; 
            if (excludeSet.has(acc.address.toString())) continue;

            try {
                const pubkey = new PublicKey(acc.address);
                const signatures = await retryRPC(() => conn.getSignaturesForAddress(pubkey, { limit: 50 }), 2, 1000);
                
                if (signatures.length > 0) {
                    const oldestTx = signatures[signatures.length - 1];
                    const txTime = oldestTx.blockTime || nowSec;
                    totalDuration += (nowSec - txTime);
                    validSamples++;
                } else {
                    totalDuration += (24 * 3600);
                    validSamples++;
                }
            } catch (_err) { /* ignore */ }
        }
        if (validSamples === 0) return { avgHoldHours: 0 };
        return { avgHoldHours: (totalDuration / validSamples) / 3600 };
    } catch (_e) {
        return { avgHoldHours: 0 };
    }
}

module.exports = { 
    getSolanaConnection, 
    analyzeTokenHolders,
    retryRPC,
    getHolderCountFromRPC
};
