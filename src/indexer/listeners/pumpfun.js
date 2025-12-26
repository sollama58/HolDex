const { PublicKey } = require('@solana/web3.js');
const { getConnection } = require('../services/helius');
const { getDB } = require('../../services/database');
const logger = require('../../services/logger');

const PUMP_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';

async function startPumpListener() {
    const connection = getConnection();
    const db = getDB();

    logger.info("Sniper: 🟢 Listening for Pump.fun launches...");

    try {
        connection.onLogs(
            new PublicKey(PUMP_PROGRAM_ID),
            async (logs, ctx) => {
                if (logs.err) return; 

                // Check for 'Create' instruction in logs (conceptual check for MVP)
                // In production, parsing inner instructions is more precise
                if (logs.logs.some(l => l.includes('Instruction: Create'))) {
                    const signature = logs.signature;
                    logger.info(`Sniper: 🔫 Potential New Token detected! Sig: ${signature}`);
                    
                    // Trigger async processing
                    processNewToken(signature, connection, db);
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
        // Wait slightly for RPC consistency
        await new Promise(r => setTimeout(r, 2000));
        
        const tx = await connection.getParsedTransaction(signature, {
            maxSupportedTransactionVersion: 0,
            commitment: 'confirmed'
        });

        if (!tx) return;

        const accounts = tx.transaction.message.accountKeys;
        
        // Pump.fun 'Create' layout assumption:
        // Index 0: Mint (The new token)
        // Index 1: Mint Authority
        // Index 2: Bonding Curve (The pool address)
        const mint = accounts[0].pubkey.toString();
        const bondingCurve = accounts[2].pubkey.toString();

        logger.info(`Sniper: 🆕 Indexed Mint: ${mint} | Pool: ${bondingCurve}`);

        // 1. Save Pool
        await db.run(`
            INSERT INTO pools (address, mint, dex, token_a, token_b, created_at)
            VALUES ($1, $2, 'pump', $3, 'SOL', $4)
            ON CONFLICT(mint, dex) DO NOTHING
        `, [bondingCurve, mint, mint, Date.now()]);
        
        // 2. Add to Tracker so we start fetching prices immediately
        await db.run(`
            INSERT INTO active_trackers (pool_address, last_check) 
            VALUES ($1, $2) 
            ON CONFLICT (pool_address) DO NOTHING
        `, [bondingCurve, Date.now()]);

        // 3. Save Basic Token Info (Mint only for now, Metadata task will fill the rest)
        await db.run(`
            INSERT INTO tokens (mint, created_at) 
            VALUES ($1, NOW()) 
            ON CONFLICT (mint) DO NOTHING
        `, [mint]);

    } catch (e) {
        logger.error(`Sniper Error parsing ${signature}:`, e.message);
    }
}

module.exports = { startPumpListener };
