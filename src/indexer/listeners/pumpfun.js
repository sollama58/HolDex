const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

const PUMP_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';

// RELIABILITY FIX: Queue System
// Prevents Helius 429 errors if 50 tokens launch in 1 second.
const processingQueue = [];
let isProcessing = false;

async function processQueue() {
    if (isProcessing || processingQueue.length === 0) return;
    isProcessing = true;

    while (processingQueue.length > 0) {
        const { signature, connection, db } = processingQueue.shift();
        try {
            await processNewToken(signature, connection, db);
            // Rate Limit: Wait 250ms between processing transactions
            // This ensures we don't exceed ~4 reqs/sec for fetched transactions
            await new Promise(resolve => setTimeout(resolve, 250));
        } catch (err) {
            logger.error(`Queue Process Error: ${err.message}`);
        }
    }

    isProcessing = false;
}

async function startPumpListener() {
    const connection = getConnection();
    const db = getDB();

    logger.info("Sniper: 🟢 Listening for Pump.fun launches...");

    try {
        connection.onLogs(
            new PublicKey(PUMP_PROGRAM_ID),
            async (logs, ctx) => {
                if (logs.err) return; 

                // Simple Instruction Check
                // In production, we'd parse the base64 inner instruction more rigorously
                if (logs.logs.some(l => l.includes('Instruction: Create'))) {
                    const signature = logs.signature;
                    
                    // Add to Queue instead of processing immediately
                    processingQueue.push({ signature, connection, db });
                    processQueue();
                }
            },
            'confirmed'
        );
    } catch (err) {
        logger.error("Sniper Error:", err);
    }
}

async function processNewToken(signature, connection, db) {
    try {
        // Wait slightly for RPC consistency (TX propagation)
        // If we fetch too fast, the node might not have the TX details indexed yet
        await new Promise(r => setTimeout(r, 1000));
        
        const tx = await connection.getParsedTransaction(signature, {
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        });

        if (!tx) return;

        const accounts = tx.transaction.message.accountKeys;
        
        // Pump.fun 'Create' layout assumption:
        // 0: Mint, 1: Authority, 2: Bonding Curve
        // Note: We use try/catch in case layout changes, ensuring resilience
        const mint = accounts[0].pubkey.toString();
        const bondingCurve = accounts[2].pubkey.toString();

        logger.info(`Sniper: 🆕 Indexed Mint: ${mint} | Pool: ${bondingCurve}`);

        // 1. Save Pool
        await db.run(`
            INSERT INTO pools (address, mint, dex, token_a, token_b, created_at)
            VALUES ($1, $2, 'pump', $3, 'SOL', $4)
            ON CONFLICT(mint, dex) DO NOTHING
        `, [bondingCurve, mint, mint, Date.now()]);
        
        // 2. Add to Tracker
        // This ensures the snapshotter picks it up in the next minute cycle
        await db.run(`
            INSERT INTO active_trackers (pool_address, last_check) 
            VALUES ($1, $2) 
            ON CONFLICT (pool_address) DO NOTHING
        `, [bondingCurve, Date.now()]);

        // 3. Save Basic Token Info
        await db.run(`
            INSERT INTO tokens (mint, created_at, timestamp) 
            VALUES ($1, NOW(), $2) 
            ON CONFLICT (mint) DO NOTHING
        `, [mint, Date.now()]);

    } catch (e) {
        // Log but don't crash
        logger.error(`Sniper Parse Error (${signature}): ${e.message}`);
    }
}

module.exports = { startPumpListener };
