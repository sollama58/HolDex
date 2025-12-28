const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// Raydium Liquidity Pool V4 Program ID
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

// Pump.fun Bonding Curve Program ID
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';

// Minimum time between processing the same signature (deduplication)
const processedSigs = new Set();

/**
 * Processes a detected transaction to identify and ingest new tokens.
 * @param {string} signature - The transaction signature
 * @param {Connection} connection - Solana connection object
 * @param {object} db - Database instance
 * @param {string} source - 'Raydium' or 'Pump.fun'
 */
async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    
    // Clear set periodically to prevent memory leak
    if (processedSigs.size > 10000) processedSigs.clear();

    try {
        // Step 1: Aggressive Retry Loop
        // 'onLogs' is faster than the RPC indexing. We must wait for the node to catch up.
        // Increased to 10 retries x 2s = ~20 seconds max wait.
        let tx = null;
        for (let i = 0; i < 10; i++) {
            tx = await connection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0,
                commitment: 'confirmed'
            });
            
            if (tx && tx.meta && !tx.meta.err) break;
            
            // logger.debug(`⏳ Listener: Waiting for ${signature} to be indexed (Attempt ${i+1}/10)...`);
            await new Promise(r => setTimeout(r, 2000));
        }

        if (!tx || !tx.meta || tx.meta.err) {
            // logger.warn(`⚠️ Listener: Could not fetch tx ${signature} after 20s. Skipping.`);
            return;
        }

        const candidateMints = new Set();
        
        // Common tokens to ignore
        const knownMints = new Set([
            'So11111111111111111111111111111111111111112', // Wrapped SOL
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
        ]);

        // STRATEGY A: Post Token Balances (Most Accurate)
        // Look for any mint in postTokenBalances that isn't a known stable/SOL
        if (tx.meta.postTokenBalances && tx.meta.postTokenBalances.length > 0) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !knownMints.has(bal.mint)) {
                    candidateMints.add(bal.mint);
                }
            });
        }

        // STRATEGY B: Account Key Parsing (Fallback)
        // If Strategy A failed (empty balances), scan Account Keys.
        // The new Mint address MUST be a "Signer" in the creation transaction (and usually Writable).
        // We skip the Fee Payer (index 0).
        if (candidateMints.size === 0 && tx.transaction.message.accountKeys) {
             const keys = tx.transaction.message.accountKeys;
             
             // Handle both Versioned (object) and Legacy (array) key formats
             const keyList = Array.isArray(keys) ? keys : keys.staticAccountKeys;

             keyList.forEach((keyObj, index) => {
                 // Skip Fee Payer (index 0)
                 if (index === 0) return;

                 // In parsed transactions, keys might be objects with .pubkey or just Pubkeys
                 const pubkey = keyObj.pubkey ? keyObj.pubkey.toString() : keyObj.toString();
                 const isSigner = keyObj.signer || connection._isSigner(keyObj, index); // helper logic if available, else rely on prop
                 
                 // If it is a Signer and NOT a known mint/program, it might be the new mint
                 if (keyObj.signer && !knownMints.has(pubkey)) {
                      // Filter out Programs (checking if it looks like a program ID is hard, 
                      // but usually mints don't look like common program IDs).
                      // For now, we add it. The indexer will verify if it's actually a token later.
                      candidateMints.add(pubkey);
                 }
             });
        }

        if (candidateMints.size === 0) {
            // logger.debug(`❌ Listener: No candidate mints found in ${signature}`);
            return;
        }

        // Process found candidates
        for (const mint of candidateMints) {
            // Filter out obviously invalid strings or programs
            if (mint === RAYDIUM_PROGRAM_ID.toString() || mint === PUMP_PROGRAM_ID.toString()) continue;
            
            // Check DB
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`✨ Discovery [${source}]: Found new token ${mint} (Tx: ${signature})`);
                
                // Insert into DB
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap, hasCommunityUpdate) 
                    VALUES ($1, 'New Discovery', 'NEW', $2, 10, 0, FALSE) 
                    ON CONFLICT (mint) DO NOTHING
                `, [mint, Date.now()]);
                
                // Trigger Indexing
                indexTokenOnChain(mint).catch(e => 
                    logger.warn(`Indexing failed for ${mint}: ${e.message}`)
                );
            }
        }

    } catch (e) {
        if (!e.message.includes('429')) {
            logger.error(`Listener Error processing ${signature} (${source}): ${e.message}`);
        }
    }
}

/**
 * Main entry point for the Token Listener service.
 * Subscribes to Solana Logs for Raydium and Pump.fun programs.
 */
async function startNewTokenListener() {
    const connection = getSolanaConnection();
    const db = getDB();
    
    logger.info("📡 Listener: Monitoring Raydium & Pump.fun for new pools...");

    // 1. Listen for Raydium V4 "Initialize2"
    try {
        connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                if (logs.err) return;
                const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2'));
                if (isInit) {
                    await processNewPoolTx(logs.signature, connection, db, 'Raydium');
                }
            },
            "confirmed"
        );
        logger.info("✅ Listener: Subscribed to Raydium V4");
    } catch (e) {
        logger.error(`Raydium Listener Error: ${e.message}`);
    }

    // 2. Listen for Pump.fun "Create"
    try {
        connection.onLogs(
            PUMP_PROGRAM_ID,
            async (logs, ctx) => {
                if (logs.err) return;
                const isCreate = logs.logs.some(l => l.includes('Instruction: Create') || l.includes('Program log: Instruction: Create'));
                if (isCreate) {
                    await processNewPoolTx(logs.signature, connection, db, 'Pump.fun');
                }
            },
            "confirmed"
        );
        logger.info("✅ Listener: Subscribed to Pump.fun");
    } catch (e) {
        logger.error(`Pump.fun Listener Error: ${e.message}`);
    }
}

module.exports = { startNewTokenListener };
