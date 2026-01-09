const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');
const { queueNewToken } = require('../services/tokenQueue');

// --- CONSTANTS ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');
const _PENDING_KEY = 'pending_growers'; 

// System Addresses to Ignore
const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1', // Pump Authority
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    '11111111111111111111111111111111',             // System Program
]);

const processedSigs = new Set();
let subscriptionIds = [];
let lastLogTime = Date.now();
let logCounter = 0;
let watchdogInterval = null;
let statusInterval = null;
let currentConnection = null;
let isReconnecting = false; // Guard against concurrent reconnection

function isIgnored(mint) {
    if (!mint) return "Null Mint";
    const m = mint.toString().trim();
    if (IGNORED_MINTS.has(m)) return "System Address";
    if (m.length < 30 || m.length > 45) return "Invalid Length"; 
    return false;
}

/**
 * Processes a detected transaction to find new mints.
 */
async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    // LRU-style eviction: Remove oldest 20% when limit reached (prevents full clear)
    if (processedSigs.size > 10000) {
        const toDelete = 2000;
        let deleted = 0;
        for (const sig of processedSigs) {
            if (deleted >= toDelete) break;
            processedSigs.delete(sig);
            deleted++;
        }
    }

    logger.info(`🔍 [${source}] POTENTIAL NEW POOL: ${signature}`);

    try {
        let tx = null;
        // RETRY LOOP: Wait up to 20s for the TX to be confirmed
        for (let i = 0; i < 10; i++) {
            try {
                await new Promise(r => setTimeout(r, 1000 + (i * 1000)));
                tx = await connection.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed' 
                });
            } catch (_err) { /* ignore */ }
            if (tx && tx.meta && !tx.meta.err) break;
        }

        if (!tx) {
            logger.warn(`   ⚠️ Failed to fetch TX body for ${signature}`);
            return;
        }

        const candidateMints = new Set();

        // --- STRATEGY A: Post Token Balances ---
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                const ignoreReason = isIgnored(bal.mint);
                if (bal.mint && !ignoreReason) {
                    candidateMints.add(bal.mint);
                }
            });
        }

        // --- STRATEGY B: Inner Instructions (MintTo / InitializeMint) ---
        if (tx.meta.innerInstructions) {
             tx.meta.innerInstructions.forEach(inner => {
                 inner.instructions.forEach(inst => {
                     if (inst.program === 'spl-token' || inst.programId.toString() === 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') {
                         if (inst.parsed) {
                             if (inst.parsed.type === 'initializeMint' || inst.parsed.type === 'mintTo') {
                                 const mint = inst.parsed.info.mint;
                                 const ignoreReason = isIgnored(mint);
                                 if (mint && !ignoreReason) candidateMints.add(mint);
                             }
                         }
                     }
                 });
             });
        }

        // --- STRATEGY C: Account Keys (Pump.fun Fallback) ---
        if (candidateMints.size === 0 && source === 'Pump.fun') {
             const message = tx.transaction.message;
             const keyList = message.accountKeys.staticAccountKeys || message.accountKeys;
             if (Array.isArray(keyList)) {
                 keyList.forEach((keyObj, index) => {
                     if (index === 0) return; 
                     const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                     const ignoreReason = isIgnored(pubkey);
                     if (pubkey && !ignoreReason) {
                          candidateMints.add(pubkey);
                     }
                 });
             }
        }

        if (candidateMints.size === 0) {
            logger.debug(`   ❌ No candidate mints found in ${signature}`);
            return;
        }

        const _redis = getClient();
        
        for (const mint of candidateMints) {
            const ignoreReason = isIgnored(mint);
            if (ignoreReason) {
                logger.info(`   🚫 [IGNORED] ${mint} Reason: ${ignoreReason}`);
                continue;
            }
            
            // CHECK DATABASE DEDUPLICATION
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            if (exists) {
                logger.info(`   ⚠️ [SKIPPED] ${mint} (Already tracked)`);
                continue;
            }

            logger.info(`🚀 [QUEUING] ${mint} from ${source}`);

            // Queue token for processing - will only add to DB once metadata is available
            await queueNewToken(mint, source);
        }
    } catch (e) {
        logger.error(`Listener Logic Error: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    logger.info("📡 Setting up WebSocket Subscriptions...");

    // RAYDIUM
    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, _ctx) => {
                logCounter++; 
                lastLogTime = Date.now();
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                const isInit = safeLogs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2') || l.includes('InitializeMint'));
                if (isInit) await processNewPoolTx(safeSig, connection, db, 'Raydium');
            },
            "processed"
        );
        subscriptionIds.push(id1);
        logger.info(`✅ Subscribed to Raydium (ID: ${id1})`);
    } catch (e) { logger.error(`Raydium Sub Error: ${e.message}`); }

    // PUMP.FUN
    try {
        const id2 = connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, _ctx) => {
                logCounter++;
                lastLogTime = Date.now();
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                // Broader check for Pump.fun events
                const isCreate = safeLogs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Program log: Create') || 
                    l.includes('MintTo') 
                );

                if (isCreate) await processNewPoolTx(safeSig, connection, db, 'Pump.fun');
            },
            "processed"
        );
        subscriptionIds.push(id2);
        logger.info(`✅ Subscribed to Pump.fun (ID: ${id2})`);
    } catch (e) { logger.error(`Pump.fun Sub Error: ${e.message}`); }
}

async function startNewTokenListener() {
    const redis = getClient();
    if (!redis) {
        logger.warn("Redis not ready, retrying...");
        setTimeout(startNewTokenListener, 2000);
        return;
    } 
    logger.info("✅ Listener: Redis Verified.");

    const db = getDB();
    
    // FORCE NEW CONNECTION
    if (currentConnection) {
        try { subscriptionIds.forEach(id => currentConnection.removeOnLogsListener(id)); } catch(_e) { /* ignore */ }
        subscriptionIds = [];
    }

    currentConnection = getSolanaConnection(true); 
    logger.info(`🔌 Listener connected to ${currentConnection.rpcEndpoint}`);

    await setupSubscriptions(currentConnection, db);

    // STATUS LOGGER
    if (statusInterval) clearInterval(statusInterval);
    statusInterval = setInterval(() => {
        const timeSince = (Date.now() - lastLogTime) / 1000;
        logger.info(`💓 Listener Status: ${logCounter} logs processed. Last activity: ${timeSince.toFixed(1)}s ago.`);
        logCounter = 0; 
    }, 30000);

    // WATCHDOG - with guard against concurrent reconnection
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        if (timeSinceLastLog > 120000 && !isReconnecting) {
            isReconnecting = true;
            logger.warn(`⚠️ LISTENERS DEAD? Reconnecting...`);
            try {
                await startNewTokenListener();
            } catch (err) {
                logger.error(`❌ Reconnection Failed: ${err.message}`);
            } finally {
                isReconnecting = false;
            }
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
