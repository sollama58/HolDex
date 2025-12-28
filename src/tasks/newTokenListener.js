const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis'); 
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');
const { indexTokenOnChain } = require('../services/indexer');

// --- CONSTANTS ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');
const PENDING_KEY = 'pending_growers'; 

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
let logCounter = 0; // GLOBAL RAW LOG COUNTER
let watchdogInterval = null;
let statusInterval = null;
let currentConnection = null;

function isIgnored(mint) {
    if (!mint) return true;
    const m = mint.toString().trim();
    if (IGNORED_MINTS.has(m)) return true;
    // Relaxed validation: just check standard base58 chars and reasonable length
    // Some valid mints might be slightly outside strict 32-44, but 30-45 is safe.
    if (m.length < 30 || m.length > 45) return true; 
    return false;
}

/**
 * Processes a detected transaction to find new mints.
 */
async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    if (processedSigs.size > 10000) processedSigs.clear();

    logger.info(`🔍 [${source}] POTENTIAL NEW POOL: ${signature}`);

    try {
        let tx = null;
        // RETRY LOOP: Wait up to 20s for the TX to be confirmed and visible on RPC
        for (let i = 0; i < 10; i++) {
            try {
                // Exponential backoff
                await new Promise(r => setTimeout(r, 1000 + (i * 1000)));
                
                tx = await connection.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed' 
                });
            } catch (err) {
                // Ignore network errors during polling
            }
            if (tx && tx.meta && !tx.meta.err) break;
        }

        if (!tx) {
            logger.warn(`   ⚠️ Failed to fetch TX body for ${signature} after retries.`);
            return;
        }

        const candidateMints = new Set();

        // --- UNIVERSAL STRATEGY: Post Token Balances ---
        // This is the most reliable method for both Raydium and Pump.fun.
        // A "Create" event ALWAYS implies a minting or transfer of the new token.
        // We look for any mint in postTokenBalances that matches our criteria.
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) {
                    // For Pump.fun, the mint usually has a large initial supply change or balance
                    candidateMints.add(bal.mint);
                }
            });
        }

        // --- FALLBACK STRATEGY: Account Keys (Pump.fun Specific) ---
        // If postTokenBalances is empty (rare but possible with some RPC config), fallback to keys.
        if (candidateMints.size === 0 && source === 'Pump.fun') {
             const message = tx.transaction.message;
             const keyList = message.accountKeys.staticAccountKeys || message.accountKeys;

             if (Array.isArray(keyList)) {
                 keyList.forEach((keyObj, index) => {
                     if (index === 0) return; // Skip Payer
                     const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                     
                     if (pubkey && !isIgnored(pubkey)) {
                          candidateMints.add(pubkey);
                     }
                 });
             }
        }

        // --- FALLBACK STRATEGY: Inner Instructions (MintTo) ---
        if (candidateMints.size === 0 && tx.meta.innerInstructions) {
             tx.meta.innerInstructions.forEach(inner => {
                 inner.instructions.forEach(inst => {
                     if (inst.program === 'spl-token' && inst.parsed && inst.parsed.type === 'initializeMint') {
                         const mint = inst.parsed.info.mint;
                         if (mint && !isIgnored(mint)) candidateMints.add(mint);
                     }
                     // Also catch MintTo, as Pump.fun mints immediately after create
                     if (inst.program === 'spl-token' && inst.parsed && inst.parsed.type === 'mintTo') {
                         const mint = inst.parsed.info.mint;
                         if (mint && !isIgnored(mint)) candidateMints.add(mint);
                     }
                 });
             });
        }

        if (candidateMints.size === 0) {
            logger.debug(`   ❌ No candidate mints found in verified TX ${signature}`);
            return;
        }

        const redis = getClient();
        
        for (const mint of candidateMints) {
            if (isIgnored(mint)) continue;
            
            // CHECK DATABASE DEDUPLICATION
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            if (exists) continue;

            logger.info(`🚀 [NEW TOKEN DETECTED] ${mint} on ${source}`);

            // 1. ADD TO DB IMMEDIATELY
            await db.run(`
                INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap, hasCommunityUpdate, updated_at) 
                VALUES ($1, 'New Discovery', 'NEW', $2, 10, 0, FALSE, NOW()) 
                ON CONFLICT (mint) DO NOTHING
            `, [mint, Date.now()]);

            // 2. TRIGGER INDEXER
            indexTokenOnChain(mint).catch(e => logger.error(`Indexer failed for ${mint}: ${e.message}`));

            // 3. ADD TO GROWER SCANNER
            if (redis) {
                const data = JSON.stringify({ mint, addedAt: Date.now(), source });
                await redis.sadd(PENDING_KEY, data);
            }
        }
    } catch (e) {
        logger.error(`Listener Processing Error: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    logger.info("📡 Setting up WebSocket Subscriptions...");

    // RAYDIUM LISTENER
    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                logCounter++; 
                lastLogTime = Date.now();
                
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                const isInit = safeLogs.some(l => 
                    l.includes('InitializeInstruction2') || 
                    l.includes('initialize2') ||
                    l.includes('InitializeMint')
                );
                
                if (isInit) await processNewPoolTx(safeSig, connection, db, 'Raydium');
            },
            "processed"
        );
        subscriptionIds.push(id1);
        logger.info(`✅ Subscribed to Raydium (ID: ${id1})`);
    } catch (e) { logger.error(`Raydium Sub Error: ${e.message}`); }

    // PUMP.FUN LISTENER
    try {
        const id2 = connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, ctx) => {
                logCounter++;
                lastLogTime = Date.now();

                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                // Broader check for Pump.fun creation events
                // "Instruction: Create" is the standard, but sometimes it appears nested.
                // We also check for "MintTo" which happens during creation.
                const isCreate = safeLogs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Program log: Instruction: Create') ||
                    l.includes('Program log: Create')
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
        try {
            subscriptionIds.forEach(id => currentConnection.removeOnLogsListener(id));
        } catch(e) {}
        subscriptionIds = [];
    }

    currentConnection = getSolanaConnection(true); 
    logger.info(`🔌 Listener connected. Waiting for logs...`);

    await setupSubscriptions(currentConnection, db);

    // STATUS LOGGER: Runs every 30 seconds
    if (statusInterval) clearInterval(statusInterval);
    statusInterval = setInterval(() => {
        const timeSince = (Date.now() - lastLogTime) / 1000;
        logger.info(`💓 Listener Status: ${logCounter} raw logs processed. Last activity: ${timeSince.toFixed(1)}s ago.`);
        logCounter = 0; // Reset counter for rate calculation
    }, 30000);

    // WATCHDOG: Reconnect if silence > 2 minutes
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        const mins = (timeSinceLastLog / 60000).toFixed(1);
        
        if (timeSinceLastLog > 120000) { 
            logger.warn(`⚠️ LISTENERS DEAD? No logs for ${mins} mins. Reconnecting...`);
            try {
                // Hard Restart
                startNewTokenListener();
            } catch (err) { logger.error(`❌ Reconnection Failed: ${err.message}`); }
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
