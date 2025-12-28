const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const logger = require('./logger');

let connection = null;

const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const TOKEN_2022_PROGRAM_ID = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');

/**
 * Creates a NEW Connection instance.
 */
function createConnection() {
    const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';
    
    if (rpcUrl.includes('api.mainnet-beta.solana.com')) {
        logger.warn("⚠️  WARNING: Using Public Solana RPC. Listeners (WebSockets) will likely fail.");
    }

    // PRIORITY 1: Explicit Env Var
    let wsUrl = config.SOLANA_WSS_URL;

    // PRIORITY 2: Auto-Derive for known providers if not set
    if (!wsUrl) {
        if (rpcUrl.includes('helius') || rpcUrl.includes('quicknode') || rpcUrl.includes('alchemy')) {
             wsUrl = rpcUrl.replace('https://', 'wss://').replace('http://', 'ws://');
        }
    }

    // --- CRITICAL CHECK ---
    // If we are intended to be a Listener, we MUST have a WSS URL.
    // If we don't, we log a loud error.
    if (!wsUrl && process.env.SERVICE_TYPE === 'listener') {
        logger.error("❌ FATAL: No WebSocket URL found! Listeners will not work.");
        logger.error("   -> Set SOLANA_WSS_URL in your environment.");
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
    
    logger.info(`🔌 Solana Connection Init: ${logUrl}`);
    logger.info(`🔌 WSS Endpoint: ${logWs}`);

    return conn;
}

/**
 * Singleton connection provider.
 */
function getSolanaConnection(forceNew = false) {
    if (!connection || forceNew) {
        connection = createConnection();
    }
    return connection;
}

async function retryRPC(fn, retries = 3, delay = 1000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (e) {
            if (e.message && (e.message.includes('400') || e.message.includes('Invalid param'))) throw e;
            const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'));
            if (i === retries - 1) throw e; 
            const waitTime = isRateLimit ? delay * 3 * (i + 1) : delay * (i + 1);
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
    } catch (e) {
        return 0;
    }
}

async function getHolderCountFromRPC(mintAddress) {
    if (!mintAddress) return 0;
    const cleanMint = mintAddress.trim();
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
            } catch (err) {}
        }
        if (validSamples === 0) return { avgHoldHours: 0 };
        return { avgHoldHours: (totalDuration / validSamples) / 3600 };
    } catch (e) {
        return { avgHoldHours: 0 };
    }
}

module.exports = { 
    getSolanaConnection, 
    analyzeTokenHolders,
    retryRPC,
    getHolderCountFromRPC
};
