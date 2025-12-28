const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis'); 
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// --- CONSTANTS ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');
const PENDING_KEY = 'pending_growers'; 

const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', 
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', 
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 
    '11111111111111111111111111111111',             
    'SysvarRent111111111111111111111111111111111',  
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1',
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
    if (m.length < 30 || m.length > 45) return true;
    return false;
}

async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    if (processedSigs.size > 10000) processedSigs.clear();

    lastLogTime = Date.now();
    
    // DEBUG LOG: Signal Received
    logger.info(`🔍 [${source}] Signal Detected: ${signature}`);

    try {
        await new Promise(r => setTimeout(r, 2000));

        let tx = null;
        for (let i = 0; i < 5; i++) {
            try {
                tx = await connection.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed'
                });
            } catch (err) {
                 logger.warn(`   ⚠️ RPC Error fetching ${signature}: ${err.message}`);
            }
            if (tx && tx.meta && !tx.meta.err) break;
            await new Promise(r => setTimeout(r, 1500));
        }

        if (!tx) {
            logger.warn(`   ❌ Failed to fetch TX body for ${signature} (gave up).`);
            return;
        }
        if (tx.meta && tx.meta.err) {
            // logger.warn(`   ❌ TX Reverted/Failed on-chain: ${signature}`);
            return;
        }

        const candidateMints = new Set();

        // STRATEGY A: Post Token Balances
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        // STRATEGY B: Pump.fun "Create" Specific
        if (source === 'Pump.fun' && candidateMints.size === 0) {
             const keys = tx.transaction.message.accountKeys;
             const keyList = Array.isArray(keys) ? keys : (keys.staticAccountKeys || []);

             keyList.forEach((keyObj, index) => {
                 if (index === 0) return; 
                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || (typeof keyObj === 'object' && keyObj.signer);
                 const isWritable = keyObj.writable || (typeof keyObj === 'object' && keyObj.writable);

                 if (isSigner && isWritable && !isIgnored(pubkey)) {
                      candidateMints.add(pubkey);
                 }
             });
        }

        // STRATEGY C: Inner Instructions
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

        if (candidateMints.size === 0) {
            logger.warn(`   ⚠️ ANALYZED TX ${signature} BUT FOUND 0 MINTS.`);
            return;
        }

        const redis = getClient();
        if (!redis) {
             logger.error(`   ❌ Redis Disconnected! Cannot queue mints.`);
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
                } else {
                    // logger.info(`   (Duplicate skipped: ${mint})`);
                }
            } catch (redisErr) {
                logger.error(`   ❌ Redis Write Failed: ${redisErr.message}`);
            }
        }
    } catch (e) {
        if (e.message && !e.message.includes('429')) logger.error(`Listener Error ${signature}: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    if (subscriptionIds.length > 0) {
        subscriptionIds.forEach(id => connection.removeOnLogsListener(id).catch(() => {}));
        subscriptionIds = [];
    }

    logger.info("📡 Subscribing to Raydium & Pump.fun logs...");

    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now();
                if (logs.err) return;
                const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2'));
                if (isInit) await processNewPoolTx(logs.signature, connection, db, 'Raydium');
            },
            "confirmed"
        );
        subscriptionIds.push(id1);
        logger.info(`✅ Subscribed to Raydium (ID: ${id1})`);
    } catch (e) { logger.error(`Raydium Sub Error: ${e.message}`); }

    try {
        const id2 = connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now();
                if (logs.err) return;
                const isCreate = logs.logs.some(l => l.includes('Instruction: Create') || l.includes('Program log: Instruction: Create'));
                if (isCreate) await processNewPoolTx(logs.signature, connection, db, 'Pump.fun');
            },
            "confirmed"
        );
        subscriptionIds.push(id2);
        logger.info(`✅ Subscribed to Pump.fun (ID: ${id2})`);
    } catch (e) { logger.error(`Pump.fun Sub Error: ${e.message}`); }
}

async function startNewTokenListener() {
    const redis = getClient();
    if (!redis) {
        logger.error("❌ FATAL: Listener started but Redis is NOT connected. Retrying in 5s...");
        setTimeout(startNewTokenListener, 5000);
        return;
    } else {
        logger.info("✅ Listener: Redis Connection Verified.");
    }

    const db = getDB();
    currentConnection = getSolanaConnection(true); 
    await setupSubscriptions(currentConnection, db);

    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        if (timeSinceLastLog > 120000) { 
            logger.warn(`⚠️ LISTENERS DEAD? No logs for ${(timeSinceLastLog / 60000).toFixed(1)} mins. Reconnecting...`);
            try {
                currentConnection = getSolanaConnection(true); 
                await setupSubscriptions(currentConnection, db);
                lastLogTime = Date.now();
            } catch (err) { logger.error(`❌ Reconnection Failed: ${err.message}`); }
        } else {
            logger.info(`💓 Listener Alive. Last log: ${(timeSinceLastLog / 60000).toFixed(1)} mins ago.`);
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
