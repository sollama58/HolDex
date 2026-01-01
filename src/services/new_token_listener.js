const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// --- CONSTANTS ---

const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// Explicit Blocklist of known tokens to NEVER index as "New"
// Includes SOL, USDC, USDT, Raydium Authority, Pump Authority, etc.
const IGNORED_MINTS = new Set([
    'So11111111111111111111111111111111111111112', // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', // Raydium V4 Authority
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1', // Raydium V4 Authority
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    '11111111111111111111111111111111',             // System Program
    'SysvarRent111111111111111111111111111111111',  // Rent
]);

const processedSigs = new Set();

/**
 * Helper to check if a mint is on the ignore list.
 */
function isIgnored(mint) {
    if (!mint) return true;
    const m = mint.toString().trim();
    if (IGNORED_MINTS.has(m)) return true;
    if (m === RAYDIUM_PROGRAM_ID.toString()) return true;
    if (m === PUMP_PROGRAM_ID.toString()) return true;
    return false;
}

/**
 * Processes a detected transaction to identify and ingest new tokens.
 */
async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    
    if (processedSigs.size > 10000) processedSigs.clear();

    try {
        // Wait for RPC indexing
        await new Promise(r => setTimeout(r, 2000));

        let tx = null;
        for (let i = 0; i < 10; i++) {
            tx = await connection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0,
                commitment: 'confirmed'
            });
            if (tx && tx.meta && !tx.meta.err) break;
            await new Promise(r => setTimeout(r, 2000));
        }

        if (!tx || !tx.meta || tx.meta.err) return;

        const candidateMints = new Set();

        // --- STRATEGY A: Post Token Balances (Preferred) ---
        // Inspect token balances to find new mints.
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !isIgnored(bal.mint)) {
                    candidateMints.add(bal.mint);
                }
            });
        }

        // --- STRATEGY B: Account Key Signers (Fallback) ---
        // ONLY use this for Pump.fun. 
        // WARNING: Do NOT use for Raydium. In Raydium Init, the mint is NOT a signer.
        // The Signer is the User (Fee Payer). If we enable this for Raydium, we will index Users as Tokens.
        if (source === 'Pump.fun' && candidateMints.size === 0 && tx.transaction.message.accountKeys) {
             const keys = tx.transaction.message.accountKeys;
             const keyList = Array.isArray(keys) ? keys : keys.staticAccountKeys;

             keyList.forEach((keyObj, index) => {
                 // Skip Fee Payer (Index 0 is always fee payer)
                 if (index === 0) return;

                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || connection._isSigner(keyObj, index);
                 
                 // Pump.fun 'Create' requires Mint to be a Signer
                 if (isSigner && !isIgnored(pubkey)) {
                      candidateMints.add(pubkey);
                 }
             });
        }

        if (candidateMints.size === 0) return;

        // Process Candidates
        for (const mint of candidateMints) {
            // Final Safety Check
            if (isIgnored(mint)) {
                // logger.debug(`🛡️ Filtered ignored mint: ${mint}`);
                continue;
            }

            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`✨ Discovery [${source}]: Found new token ${mint} (Tx: ${signature})`);
                
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap, hasCommunityUpdate) 
                    VALUES ($1, 'New Discovery', 'NEW', $2, 10, 0, FALSE) 
                    ON CONFLICT (mint) DO NOTHING
                `, [mint, Date.now()]);
                
                indexTokenOnChain(mint).catch(e => 
                    logger.warn(`Indexing failed for ${mint}: ${e.message}`)
                );
            }
        }

    } catch (e) {
        if (!e.message.includes('429')) {
            logger.error(`Listener Error ${signature}: ${e.message}`);
        }
    }
}

async function startNewTokenListener() {
    const connection = getSolanaConnection();
    const db = getDB();
    
    logger.info("📡 Listener: Monitoring Raydium & Pump.fun...");

    // 1. Raydium Listener
    try {
        connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, _ctx) => {
                if (logs.err) return;
                const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2'));
                if (isInit) {
                    await processNewPoolTx(logs.signature, connection, db, 'Raydium');
                }
            },
            "confirmed"
        );
        logger.info("✅ Listener: Raydium V4 Active");
    } catch (e) {
        logger.error(`Raydium Listener Error: ${e.message}`);
    }

    // 2. Pump.fun Listener
    try {
        connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, _ctx) => {
                if (logs.err) return;
                const isCreate = logs.logs.some(l => l.includes('Instruction: Create') || l.includes('Program log: Instruction: Create'));
                if (isCreate) {
                    await processNewPoolTx(logs.signature, connection, db, 'Pump.fun');
                }
            },
            "confirmed"
        );
        logger.info("✅ Listener: Pump.fun Active");
    } catch (e) {
        logger.error(`Pump.fun Listener Error: ${e.message}`);
    }
}

module.exports = { startNewTokenListener };
