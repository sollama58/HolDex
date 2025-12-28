const { getSolanaConnection } = require('../services/solana');
const { PublicKey } = require('@solana/web3.js');
const logger = require('../services/logger');

const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

async function startNewTokenListener() {
    console.log("☢️ STARTING NUCLEAR DEBUG LISTENER ☢️");
    
    // 1. Get Connection
    const connection = getSolanaConnection(true);
    console.log(`🔌 Connecting to: ${connection.rpcEndpoint}`);

    // 2. Simple 'onLogs' for Raydium (High Traffic)
    try {
        console.log("📡 Subscribing to Raydium...");
        const subId = connection.onLogs(
            RAYDIUM_PROGRAM_ID,
            (logs, ctx) => {
                // LOG EVERYTHING
                console.log(`📨 PACKET RECEIVED: ${logs.signature}`);
                console.log(`   -> Logs: ${logs.logs.length} lines`);
            },
            "processed" // Use 'processed' for fastest possible updates (bypasses confirmation delays)
        );
        console.log(`✅ Subscription ID: ${subId}`);
    } catch (e) {
        console.error(`❌ SUBSCRIPTION FAILED: ${e.message}`);
    }

    // 3. Keep Alive Log
    setInterval(() => {
        console.log("💓 Nuclear Listener Still Running...");
    }, 10000);
}

module.exports = { startNewTokenListener };
