const { Connection, PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('./solana');
const { getDB } = require('./database');
const { getClient } = require('./redis'); // Need Redis for the pending list
const { indexTokenOnChain } = require('./indexer'); 
const logger = require('./logger');
const axios = require('axios');

// Raydium Liquidity Pool V4
const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

// Pump.fun Bonding Curve Program
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5DkzonjNwu78hRvfCKubJ14M5uBEwF6P');

// MINIMUM MARKET CAP CONFIG
const MIN_MCAP_USD = 20000;
const PENDING_GROWERS_KEY = 'pending_growers'; // Redis Set Key

let isListening = false;

async function startNewTokenListener() {
    if (isListening) return;
    
    const connection = getSolanaConnection();
    const db = getDB();

    logger.info('🛰️ Listener: Starting On-Chain Discovery Service (Raydium & Pump.fun)...');

    connection.onLogs(
        RAYDIUM_PROGRAM_ID,
        async (logs, ctx) => {
            if (logs.err) return;
            const isInit = logs.logs.some(l => l.includes('InitializeInstruction2') || l.includes('initialize2'));
            if (isInit) {
                await processNewPoolTx(logs.signature, connection, db, 'Raydium');
            }
        },
        'confirmed'
    );

    connection.onLogs(
        PUMP_PROGRAM_ID,
        async (logs, ctx) => {
            if (logs.err) return;
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

async function getQuickMarketCap(mint) {
    try {
        // Strategy: Wait 5 seconds to let price aggregators index it, then check.
        await new Promise(r => setTimeout(r, 5000));

        const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mint}`;
        const res = await axios.get(url, { timeout: 3000 });
        const attrs = res.data?.data?.attributes;
        
        const mcap = parseFloat(attrs?.fdv_usd || attrs?.market_cap_usd || 0);
        return mcap;
    } catch (e) {
        return 0; 
    }
}

async function processNewPoolTx(signature, connection, db, source) {
    try {
        const tx = await connection.getParsedTransaction(signature, {
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        });

        if (!tx || !tx.meta || tx.meta.err) return;

        const mints = new Set();
        
        if (tx.meta.postTokenBalances) {
            tx.meta.postTokenBalances.forEach(bal => {
                // Filter out SOL and known quote tokens if necessary
                if (bal.mint && bal.mint !== 'So11111111111111111111111111111111111111112') {
                    mints.add(bal.mint);
                }
            });
        }

        for (const mint of mints) {
            const exists = await db.get('SELECT mint FROM tokens WHERE mint = $1', [mint]);
            
            if (!exists) {
                logger.info(`🔍 Discovery [${source}]: Checking MCAP for ${mint}...`);
                const mcap = await getQuickMarketCap(mint);

                if (mcap < MIN_MCAP_USD) {
                    // CHANGED: Instead of skipping, add to "Pending Growers" list in Redis
                    logger.info(`🌱 Potential Grower ${mint}: MCAP $${mcap.toFixed(0)} < $${MIN_MCAP_USD}. Added to watch list.`);
                    
                    const redis = getClient();
                    if (redis) {
                        // Store as JSON with timestamp to allow expiration later
                        const payload = JSON.stringify({ mint, addedAt: Date.now() });
                        await redis.sadd(PENDING_GROWERS_KEY, payload);
                    }
                    continue; 
                }

                logger.info(`✨ Discovery [${source}]: Valid Token Found! ${mint} (MCAP: $${mcap.toFixed(0)})`);
                
                await db.run(`
                    INSERT INTO tokens (mint, name, symbol, timestamp, k_score, marketCap) 
                    VALUES ($1, 'New Discovery', 'NEW', $2, 50, $3) 
                    ON CONFLICT DO NOTHING
                `, [mint, Date.now(), mcap]);
                
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
