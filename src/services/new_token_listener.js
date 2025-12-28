const { getSolanaConnection, retryRPC } = require('../services/solana');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// Raydium Liquidity Pool V4 Program ID
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

// Pump.fun Bonding Curve Program ID
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

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
    
    // Clear set periodically to prevent memory leak (reset every ~10000 txs)
    if (processedSigs.size > 10000) processedSigs.clear();

    try {
        // FIX: Add initial delay to allow RPC node to index the transaction.
        // 'onLogs' fires extremely fast, often before the transaction is retrievable via getParsedTransaction.
        await new Promise(r => setTimeout(r, 2000));

        let tx = null;
        
        // FIX: Retry loop for fetching transaction details.
        // We try 3 times with a delay to handle eventual consistency on RPC nodes.
        for (let i = 0; i < 3; i++) {
            tx = await connection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0,
                commitment: 'confirmed'
            });
            if (tx) break;
            await new Promise(r => setTimeout(r, 1500));
        }

        if (!tx || !tx.meta || tx.meta.err) {
            // Transaction failed or wasn't found after retries.
            return;
        }

        // Strategy: Look for new token balances or instructions that define the mint.
        // For Raydium/Pump, the Mint is usually one of the 'postTokenBalances' that appeared in the tx.
        // We filter out known common tokens (SOL, USDC, USDT) to isolate the new "shitcoin" or token.
        
        const knownMints = new Set([
            'So11111111111111111111111111111111111111112', // Wrapped SOL
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
        ]);

        const candidateMints = new Set();

        if (tx.meta.postTokenBalances) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && !knownMints.has(bal.mint)) {
                    candidateMints.add(bal.mint);
                }
            });
        }

        for (const mint of candidateMints) {
            // Check if we already have it in the database
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`✨ Discovery [${source}]: Found new token ${mint}`);
                
                // Insert into DB with "New" status
                // Initial K-Score 10 (Low trust until proven)
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap, hasCommunityUpdate) 
                    VALUES ($1, 'New Discovery', 'NEW', $2, 10, 0, FALSE) 
                    ON CONFLICT (mint) DO NOTHING
                `, [mint, Date.now()]);
                
                // Trigger immediate indexing (Metadata, Pools, etc.)
                // We don't await this to keep the listener loop fast and responsive
                indexTokenOnChain(mint).catch(e => 
                    logger.warn(`Indexing failed for discovered token ${mint}: ${e.message}`)
                );
            }
        }

    } catch (e) {
        // Suppress "Too Many Requests" logs to avoid spamming the console
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

    // 1. Listen for Raydium V4 "Initialize2" (New Pool Creation)
    try {
        connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                if (logs.err) return;
                // Check logs for initialization instruction patterns
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
                // Check logs for Pump.fun creation instruction patterns
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
