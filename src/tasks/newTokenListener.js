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
let watchdogInterval = null;
let currentConnection = null;

function isIgnored(mint) {
    if (!mint) return true;
    const m = mint.toString().trim();
    if (IGNORED_MINTS.has(m)) return true;
    if (m.length < 30 || m.length > 45) return true; // Basic base58 validation
    return false;
}

/**
 * Processes a detected transaction to find new mints.
 */
async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    if (processedSigs.size > 10000) processedSigs.clear();

    lastLogTime = Date.now(); // Heartbeat update
    logger.info(`🔍 [${source}] SIGNAL DETECTED: ${signature}`);

    try {
        let tx = null;
        // RETRY LOOP: Wait up to 15s for the TX to be confirmed and visible on RPC
        // "processed" logs come in fast, but "getParsedTransaction" needs "confirmed"
        for (let i = 0; i < 8; i++) {
            try {
                // Exponential backoff: 2s, 3s, 4s...
                await new Promise(r => setTimeout(r, 2000 + (i * 1000)));
                
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

        // --- STRATEGY A: Post Token Balances ---
        // Good for Raydium
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        // --- STRATEGY B: Account Keys (Pump.fun Specific) ---
        // Pump.fun creation often involves the Mint being a Signer and Writable
        if (source === 'Pump.fun') {
             const message = tx.transaction.message;
             const keyList = message.accountKeys.staticAccountKeys || message.accountKeys;

             if (Array.isArray(keyList)) {
                 keyList.forEach((keyObj, index) => {
                     // Index 0 is Payer, usually not the mint
                     if (index === 0) return;
                     
                     const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                     
                     let isSigner = false;
                     let isWritable = false;

                     // Handle both raw keys (Legacy) and object keys (some RPC responses)
                     if (typeof keyObj === 'object') {
                         isSigner = keyObj.signer;
                         isWritable = keyObj.writable;
                     } else {
                         // If it's just a string/PublicKey, we have to rely on header (complex for V0)
                         // But usually getParsedTransaction returns objects in accountKeys for Legacy
                         // For V0, we check header. For now, rely on parsed structure.
                     }
                     
                     // In Pump.fun V0 transactions, the mint is often the 2nd or 3rd signer
                     if (!isIgnored(pubkey)) {
                          candidateMints.add(pubkey);
                     }
                 });
             }
        }

        // --- STRATEGY C: Inner Instructions (MintTo) ---
        // Catches any 'initializeMint' instruction
        if (tx.meta.innerInstructions) {
             tx.meta.innerInstructions.forEach(inner => {
                 inner.instructions.forEach(inst => {
                     if (inst.program === 'spl-token' && inst.parsed && inst.parsed.type === 'initializeMint') {
                         const mint = inst.parsed.info.mint;
                         if (mint && !isIgnored(mint)) candidateMints.add(mint);
                     }
                 });
             });
        }

        if (candidateMints.size === 0) {
            logger.debug(`   ℹ️ No valid mints found in ${signature}`);
            return;
        }

        const redis = getClient();
        
        for (const mint of candidateMints) {
            if (isIgnored(mint)) continue;
            
            // CHECK DATABASE DEDUPLICATION
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            if (exists) continue;

            logger.info(`🚀 [NEW TOKEN] ${mint} detected on ${source}`);

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
        if (e.message && !e.message.includes('429')) logger.error(`Listener Logic Error: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    logger.info("📡 Subscribing to Raydium & Pump.fun logs...");

    // RAYDIUM LISTENER
    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                // Heartbeat check (every 50 logs)
                if (Math.random() < 0.02) logger.debug(`💓 Raydium Heartbeat: Logs flowing...`);

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
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                if (!safeSig) return;

                // Heartbeat check
                if (Math.random() < 0.02) logger.debug(`💓 Pump.fun Heartbeat: Logs flowing...`);

                const isCreate = safeLogs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Create') || 
                    l.includes('InitializeMint') || 
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
    currentConnection = getSolanaConnection(true); 
    logger.info(`🔌 Listener Connection Endpoint: ${currentConnection.rpcEndpoint}`);

    await setupSubscriptions(currentConnection, db);

    // WATCHDOG: Reconnect if silence > 2 minutes
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        const mins = (timeSinceLastLog / 60000).toFixed(1);
        
        if (timeSinceLastLog > 120000) { 
            logger.warn(`⚠️ LISTENERS DEAD? No logs for ${mins} mins. Reconnecting...`);
            try {
                try { 
                    subscriptionIds.forEach(id => currentConnection.removeOnLogsListener(id)); 
                } catch(e) {}
                
                currentConnection = getSolanaConnection(true); 
                await setupSubscriptions(currentConnection, db);
                lastLogTime = Date.now();
            } catch (err) { logger.error(`❌ Reconnection Failed: ${err.message}`); }
        } else {
            // logger.info(`💓 Listener Alive. Last log: ${mins} mins ago.`);
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
