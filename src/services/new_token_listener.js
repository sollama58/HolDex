const { Connection, PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('./solana');
const { getDB } = require('./database');
const { indexTokenOnChain } = require('./indexer'); // Ensure this is imported correctly
const logger = require('./logger');

// Raydium Liquidity Pool V4
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

// Pump.fun Bonding Curve Program
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

let isListening = false;

async function startNewTokenListener() {
    if (isListening) return;
    
    const connection = getSolanaConnection();
    const db = getDB();

    logger.info('🛰️ Listener: Starting On-Chain Discovery Service (Raydium & Pump.fun)...');

    // 1. LISTEN TO RAYDIUM POOL CREATIONS
    connection.onLogs(
        RAYDIUM_PROGRAM_ID,
        async (logs, ctx) => {
            if (logs.err) return;
            // Raydium "initialize2" instruction often indicates new pool creation
            const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2'));
            if (isInit) {
                await processNewPoolTx(logs.signature, connection, db, 'Raydium');
            }
        },
        'confirmed'
    );

    // 2. LISTEN TO PUMP.FUN TOKEN CREATIONS
    connection.onLogs(
        PUMP_PROGRAM_ID,
        async (logs, ctx) => {
            if (logs.err) return;
            // Pump.fun "Create" instruction creates a new token and bonding curve
            // The logs usually contain "Instruction: Create"
            const isCreate = logs.logs.some(l => l.includes('Instruction: Create'));
            if (isCreate) {
                await processNewPoolTx(logs.signature, connection, db, 'Pump.fun');
            }
        },
        'confirmed'
    );

    isListening = true;
    logger.info('✅ Listener: Connected to Solana WebSocket.');
}

async function processNewPoolTx(signature, connection, db, source) {
    try {
        // Fetch parsed transaction to get account keys
        const tx = await connection.getParsedTransaction(signature, {
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        });

        if (!tx || !tx.meta || tx.meta.err) return;

        const mints = new Set();
        
        // Strategy: Scan all postTokenBalances. 
        // Newly created tokens will appear here. We filter out SOL (So111...)
        if (tx.meta.postTokenBalances) {
            tx.meta.postTokenBalances.forEach(bal => {
                if (bal.mint && bal.mint !== 'So11111111111111111111111111111111111111112') {
                    mints.add(bal.mint);
                }
            });
        }

        // Additional Strategy for Pump.fun:
        // The Mint address is usually the first or second account in the instruction keys, 
        // but scanning balances is more robust across different program versions.

        for (const mint of mints) {
            // Check existence to save resources
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`✨ Discovery [${source}]: Found new token ${mint} in tx ${signature}`);
                
                // 1. Quick Insert (Placeholder) to make it searchable immediately
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, timestamp, k_score) 
                    VALUES ($1, 'New Discovery', 'NEW', $2, 50) 
                    ON CONFLICT DO NOTHING
                `, [mint, Date.now()]);
                
                // 2. Trigger Deep Indexing (Async)
                // We call this without awaiting so the listener loop doesn't block
                indexTokenOnChain(mint).catch(e => 
                    logger.error(`Indexing failed for discovered token ${mint}: ${e.message}`)
                );
            }
        }

    } catch (e) {
        logger.error(`Listener Error processing ${signature} (${source}): ${e.message}`);
    }
}

module.exports = { startNewTokenListener };
