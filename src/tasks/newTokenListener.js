const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis'); 
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

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

    lastLogTime = Date.now();
    logger.info(`🔍 [${source}] MATCH FOUND: ${signature}`);

    try {
        // Wait briefly for indexing
        await new Promise(r => setTimeout(r, 2000));

        let tx = null;
        for (let i = 0; i < 5; i++) {
            try {
                tx = await connection.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed'
                });
            } catch (err) {}
            if (tx && tx.meta && !tx.meta.err) break;
            await new Promise(r => setTimeout(r, 1500));
        }

        if (!tx) {
            logger.warn(`   ⚠️ Failed to fetch TX body for ${signature}`);
            return;
        }

        const candidateMints = new Set();

        // --- STRATEGY A: Post Token Balances ---
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        // --- STRATEGY B: Account Keys (Pump.fun Specific) ---
        // Handles both Legacy and V0 transaction structures
        if (source === 'Pump.fun' && candidateMints.size === 0) {
             const message = tx.transaction.message;
             const keyList = message.accountKeys.staticAccountKeys || message.accountKeys;

             if (Array.isArray(keyList)) {
                 keyList.forEach((keyObj, index) => {
                     if (index === 0) return; // Skip Payer
                     
                     const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                     
                     let isSigner = false;
                     let isWritable = false;

                     if (typeof keyObj === 'object' && keyObj.signer) {
                         isSigner = keyObj.signer;
                         isWritable = keyObj.writable;
                     } 

                     // Pump Mint is typically a Signer AND Writable during creation
                     if (isSigner && isWritable && !isIgnored(pubkey)) {
                          candidateMints.add(pubkey);
                     }
                 });
             }
        }

        // --- STRATEGY C: Inner Instructions ---
        if (candidateMints.size === 0 && tx.meta.innerInstructions) {
             tx.meta.innerInstructions.forEach(inner => {
                 inner.instructions.forEach(inst => {
                     if (inst.program === 'spl-token' && inst.parsed && inst.parsed.type === 'initializeMint') {
                         const mint = inst.parsed.info.mint;
                         if (mint && !isIgnored(mint)) candidateMints.add(mint);
                     }
                 });
             });
        }

        const redis = getClient();
        if (!redis) {
             logger.error("❌ FATAL: Redis disconnected in Listener loop.");
             return;
        }

        for (const mint of candidateMints) {
            if (isIgnored(mint)) continue;
            
            const data = JSON.stringify({ mint, addedAt: Date.now(), source });
            
            try {
                const added = await redis.sadd(PENDING_KEY, data);
                if (added) {
                    console.log(`\n--------------------------------------------------`);
                    logger.info(`🌱 [PENDING LIST] Added: ${mint}`);
                    logger.info(`   📝 Source: ${source}`);
                    logger.info(`   🔗 Tx: ${signature}`);
                    console.log(`--------------------------------------------------\n`);
                }
            } catch (redisErr) {
                logger.error(`❌ Redis Write Failed: ${redisErr.message}`);
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
                lastLogTime = Date.now();
                // Safe Parsing for nested objects
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                const safeErr = logs.err || (logs.value && logs.value.err) || null;

                if (safeErr || !safeSig) return;

                const isInit = safeLogs.some(l => 
                    l.includes('InitializeInstruction2') || 
                    l.includes('initialize2') ||
                    l.includes('InitializeMint')
                );
                if (isInit) await processNewPoolTx(safeSig, connection, db, 'Raydium');
            },
            "processed" // Fast commitment
        );
        subscriptionIds.push(id1);
        logger.info(`✅ Subscribed to Raydium (ID: ${id1})`);
    } catch (e) { logger.error(`Raydium Sub Error: ${e.message}`); }

    // PUMP.FUN LISTENER
    try {
        const id2 = connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now();
                // Safe Parsing
                const safeLogs = logs.logs || (logs.value && logs.value.logs) || [];
                const safeSig = logs.signature || (logs.value && logs.value.signature) || null;
                const safeErr = logs.err || (logs.value && logs.value.err) || null;

                if (safeErr || !safeSig) return;

                // Wide Net Filter
                const isCreate = safeLogs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Create') || 
                    l.includes('InitializeMint') || 
                    l.includes('MintTo')
                );

                if (isCreate) await processNewPoolTx(safeSig, connection, db, 'Pump.fun');
            },
            "processed" // Fast commitment
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
            logger.info(`💓 Listener Alive. Last log: ${mins} mins ago.`);
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
