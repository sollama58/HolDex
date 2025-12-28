const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis'); 
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');
const PENDING_KEY = 'pending_growers'; 

const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', 
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1',
]);

const processedSigs = new Set();
let subscriptionIds = [];
let lastLogTime = Date.now();
let watchdogInterval = null;
let currentConnection = null;
let debugLogCounter = 0; 

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
    logger.info(`🔍 [${source}] MATCH FOUND: ${signature}`);

    try {
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

        if (!tx) return;

        const candidateMints = new Set();

        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        if (source === 'Pump.fun' && candidateMints.size === 0) {
             const message = tx.transaction.message;
             const keyList = message.accountKeys.staticAccountKeys || message.accountKeys;

             if (Array.isArray(keyList)) {
                 keyList.forEach((keyObj, index) => {
                     if (index === 0) return; 
                     const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                     
                     let isSigner = false;
                     let isWritable = false;

                     if (typeof keyObj === 'object' && keyObj.signer) {
                         isSigner = keyObj.signer;
                         isWritable = keyObj.writable;
                     } 

                     if (isSigner && isWritable && !isIgnored(pubkey)) candidateMints.add(pubkey);
                 });
             }
        }

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
             logger.error("❌ Redis is NULL inside process loop!");
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
        // Suppress errors
    }
}

async function setupSubscriptions(connection, db) {
    logger.info("📡 Subscribing to Raydium & Pump.fun logs...");

    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now();
                // SAFE PARSING: Handle nested structure
                const safeLogs = logs.logs ? logs.logs : (logs.value && logs.value.logs ? logs.value.logs : []);
                const safeSig = logs.signature ? logs.signature : (logs.value && logs.value.signature ? logs.value.signature : null);
                const safeErr = logs.err ? logs.err : (logs.value && logs.value.err ? logs.value.err : null);

                if (safeErr || !safeSig) return;

                const isInit = safeLogs.some(l => 
                    l.includes('InitializeInstruction2') || 
                    l.includes('initialize2') ||
                    l.includes('InitializeMint')
                );
                if (isInit) await processNewPoolTx(safeSig, connection, db, 'Raydium');
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

                // --- PAYLOAD INSPECTION START ---
                debugLogCounter++;
                if (debugLogCounter <= 3) {
                    console.log(`🕵️ [PAYLOAD INSPECTION ${debugLogCounter}]: Keys found ->`, Object.keys(logs));
                    if (logs.value) console.log(`   -> .value Keys:`, Object.keys(logs.value));
                }
                // --------------------------------

                // SAFE PARSING: Handle nested structure
                const safeLogs = logs.logs ? logs.logs : (logs.value && logs.value.logs ? logs.value.logs : []);
                const safeSig = logs.signature ? logs.signature : (logs.value && logs.value.signature ? logs.value.signature : null);
                const safeErr = logs.err ? logs.err : (logs.value && logs.value.err ? logs.value.err : null);

                if (safeErr || !safeSig) return;

                const isCreate = safeLogs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Create') || 
                    l.includes('InitializeMint') || 
                    l.includes('MintTo')
                );

                if (isCreate) await processNewPoolTx(safeSig, connection, db, 'Pump.fun');
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
        setTimeout(startNewTokenListener, 2000);
        return;
    } 
    logger.info("✅ Listener: Redis Verified.");

    const db = getDB();
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
