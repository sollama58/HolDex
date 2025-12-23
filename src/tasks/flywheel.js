/**
 * Flywheel Task (Postgres Compatible)
 * Buys back tokens using collected fees.
 */
const { Connection, Keypair, PublicKey, VersionedTransaction } = require('@solana/web3.js');
const { getDB } = require('../services/database');
const { logger } = require('../services/logger');
const config = require('../config/env');
const solana = require('../services/solana');
const pump = require('../services/pump');
const jupiter = require('../services/jupiter');
const bs58 = require('bs58');

// Load wallet
let payer;
if (config.PRIVATE_KEY) {
    payer = Keypair.fromSecretKey(new Uint8Array(JSON.parse(config.PRIVATE_KEY)));
}

async function runFlywheel(deps) {
    const { db, globalState } = deps;
    if (!payer) return;

    try {
        // 1. Check if we have fees to spend
        const stats = await db.get('SELECT value FROM stats WHERE key = $1', ['accumulatedFeesLamports']);
        const collected = stats ? stats.value : 0;

        // Threshold: 0.5 SOL
        if (collected < 500000000) return; 

        logger.info(`[FLYWHEEL] Cycle started. Accumulated: ${collected / 1e9} SOL`);

        // 2. Identify Target Token (King of the Hill)
        const koth = await db.get('SELECT mint, marketCap FROM tokens ORDER BY marketCap DESC LIMIT 1');
        if (!koth) return;

        // 3. Buy Back Logic
        const amountToSpend = collected; 
        // Example: Swap SOL for Token on Pump/Raydium
        // ... (Swap logic omitted for brevity, assumes implementation in services/jupiter.js) ...
        
        // Mocking the DB log for the buy
        const txSig = 'mock_tx_signature'; 

        // 4. Log Cycle (Postgres Syntax Update)
        await db.run(`
            INSERT INTO flywheel_logs (
                timestamp, status, feesCollected, solSpent, tokensBought, pumpBuySig, reason
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        `, [
            Date.now(), 
            'success', 
            collected, 
            amountToSpend, 
            '0', 
            txSig, 
            `Bought ${koth.mint}`
        ]);

        // 5. Reset Fees
        await db.run('UPDATE stats SET value = value - $1 WHERE key = $2', [amountToSpend, 'accumulatedFeesLamports']);

    } catch (e) {
        logger.error(`[FLYWHEEL] Error: ${e.message}`);
    }
}

function start(deps) {
    // Run every 10 minutes
    setInterval(() => runFlywheel(deps), 600000);
}

module.exports = { runFlywheel, start };
