const { getSolanaConnection } = require('../services/solana');
const { getDB } = require('../services/database');
const { indexTokenOnChain } = require('../services/indexer');
const logger = require('../services/logger');
const { PublicKey } = require('@solana/web3.js');

// Raydium Liquidity Pool V4
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

// Pump.fun Bonding Curve
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// Minimum time between processing the same signature (dedup)
const processedSigs = new Set();

async function processNewPoolTx(signature, connection, db, source) {
    if (processedSigs.has(signature)) return;
    processedSigs.add(signature);
    
    // Clear set periodically to prevent memory leak
    if (processedSigs.size > 10000) processedSigs.clear();

    try {
        // Fetch the parsed transaction to get the accounts (Mints)
        // Rate limit protection: simple wait if needed, but here we rely on the provider's capacity
        const tx = await connection.getParsedTransaction(signature, {
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        });

        if (!tx || !tx.meta || tx.meta.err) return;

        // Strategy: Look for new token balances or instructions that define the mint
        // For Raydium/Pump, the Mint is usually one of the 'postTokenBalances' that appeared.
        // We filter out the WSOL and USDC mints to find the "new" coin.
        
        const knownMints = new Set([
            'So11111111111111111111111111111111111111112', // SOL
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
            // Check if we already have it
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
                // We don't await this to keep the listener loop fast
                indexTokenOnChain(mint).catch(e => 
                    logger.warn(`Indexing failed for discovered token ${mint}: ${e.message}`)
                );
            }
        }

    } catch (e) {
        // Suppress "Too Many Requests" logs to avoid spamming
        if (!e.message.includes('429')) {
            logger.error(`Listener Error processing ${signature} (${source}): ${e.message}`);
        }
    }
}

async function startNewTokenListener() {
    const connection = getSolanaConnection();
    const db = getDB();
    
    logger.info("📡 Listener: Monitoring Raydium & Pump.fun for new pools...");

    // 1. Listen for Raydium V4 "Initialize2" (New Pool)
    try {
        connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            async (logs, ctx) => {
                if (logs.err) return;
                // Check logs for initialization patterns
                // "InitializeInstruction2" is specific to Raydium V4 pool creation
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
                // Pump.fun 'Create' instruction log pattern
                const isCreate = logs.logs.some(l => l.includes('Instruction: Create'));
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
