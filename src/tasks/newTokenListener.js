const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// --- CONSTANTS ---
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', 
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', 
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 
    '11111111111111111111111111111111',             
    'SysvarRent111111111111111111111111111111111',  
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
    return false;
}

async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    if (processedSigs.size > 10000) processedSigs.clear();

    // UPDATE HEARTBEAT
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

        if (!tx || !tx.meta || tx.meta.err) return;

        const candidateMints = new Set();

        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) candidateMints.add(bal.mint);
            });
        }

        if (source === 'Pump.fun' && candidateMints.size === 0 && tx.transaction.message.accountKeys) {
             const keys = tx.transaction.message.accountKeys;
             const keyList = Array.isArray(keys) ? keys : (keys.staticAccountKeys || []);

             keyList.forEach((keyObj, index) => {
                 if (index === 0) return;
                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || (typeof keyObj === 'object' && keyObj.signer);
                 if (isSigner && !isIgnored(pubkey)) candidateMints.add(pubkey);
             });
        }

        for (const mint of candidateMints) {
            if (isIgnored(mint)) continue;
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`✨ Discovery [${source}]: Found new token ${mint} (Tx: ${signature})`);
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, updated_at, k_score, marketCap, hasCommunityUpdate) 
                    VALUES ($1, 'New Discovery', 'NEW', NOW(), 10, 0, FALSE) 
                    ON CONFLICT (mint) DO NOTHING
                `, [mint]);
                
                indexTokenOnChain(mint).catch(e => logger.warn(`Indexing failed for ${mint}: ${e.message}`));
            }
        }
    } catch (e) {
        if (e.message && !e.message.includes('429')) logger.error(`Listener Error ${signature}: ${e.message}`);
    }
}

async function setupSubscriptions(connection, db) {
    // Clear old subscriptions
    if (subscriptionIds.length > 0) {
        subscriptionIds.forEach(id => connection.removeOnLogsListener(id).catch(() => {}));
        subscriptionIds = [];
    }

    logger.info("📡 Subscribing to Raydium & Pump.fun logs...");

    try {
        const id1 = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                lastLogTime = Date.now(); // Heartbeat on ANY log
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
                lastLogTime = Date.now(); // Heartbeat on ANY log
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
    const db = getDB();
    
    // Initial Start
    currentConnection = getSolanaConnection(true); // Force fresh connection
    await setupSubscriptions(currentConnection, db);

    // WATCHDOG: Checks every 60s
    if (watchdogInterval) clearInterval(watchdogInterval);
    
    watchdogInterval = setInterval(async () => {
        const timeSinceLastLog = Date.now() - lastLogTime;
        const minutesSilence = (timeSinceLastLog / 60000).toFixed(1);

        if (timeSinceLastLog > 120000) { // 2 Minutes Silence
            logger.warn(`⚠️ LISTENERS DEAD? No logs for ${minutesSilence} mins. Reconnecting...`);
            
            try {
                // 1. Force New Connection
                currentConnection = getSolanaConnection(true); 
                
                // 2. Re-Subscribe
                await setupSubscriptions(currentConnection, db);
                
                // 3. Reset Timer
                lastLogTime = Date.now();
                logger.info("♻️ Listener Reconnection Complete.");
            } catch (err) {
                logger.error(`❌ Reconnection Failed: ${err.message}`);
            }
        } else {
            logger.info(`💓 Listener Alive. Last log: ${minutesSilence} mins ago.`);
        }
    }, 60000);
}

module.exports = { startNewTokenListener };
