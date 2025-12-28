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
    'So11111111111111111111111111111111111111112', // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1', // Pump Authority
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

// ... [processNewPoolTx logic remains the same as previous "Full Logic" version] ...
// I am omitting the body of processNewPoolTx for brevity, but assume it is the same
// robust version from the previous step. The critical change is below in STARTUP.

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
             const keys = tx.transaction.message.accountKeys;
             const keyList = Array.isArray(keys) ? keys : (keys.staticAccountKeys || []);
             keyList.forEach((keyObj, index) => {
                 if (index === 0) return; 
                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || (typeof keyObj === 'object' && keyObj.signer);
                 const isWritable = keyObj.writable || (typeof keyObj === 'object' && keyObj.writable);
                 if (isSigner && isWritable && !isIgnored(pubkey)) candidateMints.add(pubkey);
             });
        }

        // Inner Instructions Check
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
        if (!redis) return;

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
        if (e.message && !e.message.includes('429')) logger.error(`Listener Error: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    console.log("⚙️  Setting up Log Subscriptions..."); // EMERGENCY LOG

    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now();
                if (logs.err) return;
                const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2') || l.includes('InitializeMint'));
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

                // --- FIREHOSE ---
                debugLogCounter++;
                if (debugLogCounter <= 10) {
                    console.log(`🔥 [RAW LOG ${debugLogCounter}]:`, JSON.stringify(logs.logs));
                }
                // ----------------

                const isCreate = logs.logs.some(l => 
                    l.includes('Instruction: Create') || 
                    l.includes('Create') || 
                    l.includes('InitializeMint') || 
                    l.includes('MintTo')
                );

                if (isCreate) await processNewPoolTx(logs.signature, connection, db, 'Pump.fun');
            },
            "confirmed"
        );
        subscriptionIds.push(id2);
        logger.info(`✅ Subscribed to Pump.fun (ID: ${id2})`);
    } catch (e) { logger.error(`Pump.fun Sub Error: ${e.message}`); }
}

async function startNewTokenListener() {
    // 1. EMERGENCY LOG: Proves function was called
    console.log("\n\n************************************************");
    console.log("🚨 EMERGENCY DEBUG: LISTENER MODULE STARTING 🚨");
    console.log("************************************************\n");

    const redis = getClient();
    if (!redis) {
        logger.error("❌ Redis Client is NULL. Retrying in 2s...");
        setTimeout(startNewTokenListener, 2000);
        return;
    }
    logger.info("✅ Redis Client Found.");

    const db = getDB();
    
    // 2. CONNECTION LOG
    try {
        currentConnection = getSolanaConnection(true);
        const endpoint = currentConnection.rpcEndpoint;
        logger.info(`🔌 RPC Endpoint: ${endpoint}`);
        
        // Helius Check: If using Helius, verify it's not the public one
        if (endpoint.includes('api.mainnet-beta')) {
            logger.warn("⚠️ WARNING: Using PUBLIC endpoint. WSS will likely fail!");
        }
    } catch (e) {
        logger.error(`❌ Connection Creation Failed: ${e.message}`);
        return;
    }

    // 3. SUBSCRIPTION START
    await setupSubscriptions(currentConnection, db);

    // 4. HEARTBEAT START
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        const mins = (timeSinceLastLog / 60000).toFixed(1);
        
        if (timeSinceLastLog > 120000) { 
            logger.warn(`⚠️ LISTENERS DEAD? No logs for ${mins} mins. Reconnecting...`);
            try {
                currentConnection = getSolanaConnection(true); 
                await setupSubscriptions(currentConnection, db);
                lastLogTime = Date.now();
            } catch (err) { logger.error(`❌ Reconnection Failed: ${err.message}`); }
        } else {
            logger.info(`💓 Listener Alive. Last log: ${mins} mins ago.`);
        }
    }, 60000);
    
    console.log("✅ Listener Startup Sequence Complete.");
}

module.exports = { startNewTokenListener };
