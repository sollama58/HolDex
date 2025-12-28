const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { getClient } = require('../services/redis'); 
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// --- CONSTANTS ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');
const PENDING_KEY = 'pending_growers'; 

// Known System Addresses to Ignore
const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', // Raydium Authority
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1', // Raydium Authority
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    '11111111111111111111111111111111',             // System Program
    'SysvarRent111111111111111111111111111111111',  // Rent
    'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1', // Pump Authority
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
    // Basic format check (base58 length ~32-44 chars)
    if (m.length < 30 || m.length > 45) return true;
    return false;
}

async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    if (processedSigs.size > 10000) processedSigs.clear();

    lastLogTime = Date.now();

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
            await new Promise(r => setTimeout(r, 2000));
        }

        if (!tx || !tx.meta || tx.meta.err) {
            // logger.debug(`⚠️ Failed to fetch TX ${signature}`); // Too noisy for prod
            return;
        }

        const candidateMints = new Set();

        // STRATEGY A: Post Token Balances (The Gold Standard)
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        // STRATEGY B: Pump.fun "Create" Specific (Account Keys)
        // In a "Create" instruction, the Mint is a Signer (index 1 or 2 usually).
        // It is also Writable.
        if (source === 'Pump.fun' && candidateMints.size === 0) {
             const keys = tx.transaction.message.accountKeys;
             const keyList = Array.isArray(keys) ? keys : (keys.staticAccountKeys || []);

             keyList.forEach((keyObj, index) => {
                 if (index === 0) return; // Skip Payer
                 
                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || (typeof keyObj === 'object' && keyObj.signer);
                 const isWritable = keyObj.writable || (typeof keyObj === 'object' && keyObj.writable);

                 // Pump Mint is always a Signer AND Writable during creation
                 if (isSigner && isWritable && !isIgnored(pubkey)) {
                      candidateMints.add(pubkey);
                 }
             });
        }

        // STRATEGY C: Inner Instructions (Deep Scan)
        // Sometimes Raydium/Pump initialization happens via CPI.
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
             logger.error(`❌ FATAL: Redis disconnected in Listener loop.`);
             return;
        }

        if (candidateMints.size === 0) {
            // Uncomment for deep debugging if needed
            // logger.warn(`⚠️ No mints found in ${source} TX: ${signature}`);
            return;
        }

        for (const mint of candidateMints) {
            if (isIgnored(mint)) continue;
            
            const data = JSON.stringify({ mint, addedAt: Date.now(), source });
            
            try {
                const added = await redis.sadd(PENDING_KEY, data);
                if (added) {
                    // CONSOLE BLOCK FOR VISIBILITY
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
            logger.warn(`⚠️ LISTENERS DEAD? Reconnecting...`);
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
