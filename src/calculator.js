#!/usr/bin/env node
/**
 * HolDex Calculator - The Brain of Data Processing
 *
 * Runs as a dedicated background worker to handle:
 * - K-Score calculations (conviction analysis)
 * - Metadata updates (price/volume/liquidity)
 * - Grower scanning (token promotion)
 */

// CRITICAL: Force development mode to bypass ADMIN_PASSWORD validation in config/env
process.env.NODE_ENV = 'development';

require('dotenv').config();

const config = require('./config/env');
const logger = require('./services/logger');

// Startup info
logger.info('='.repeat(50));
logger.info('🧠 HolDex Calculator Starting');
logger.info(`   Node: ${process.version}`);
logger.info(`   DATABASE_URL: ${config.DATABASE_URL ? 'SET' : 'MISSING'}`);
logger.info(`   HELIUS_API_KEY: ${config.HELIUS_API_KEY ? 'SET' : 'MISSING'}`);
logger.info(`   REDIS_URL: ${config.REDIS_URL ? 'SET' : 'MISSING'}`);
logger.info('='.repeat(50));

// Memory monitoring
let heartbeatCount = 0;
function logHeartbeat() {
    heartbeatCount++;
    const mem = process.memoryUsage();
    logger.info(`💓 Heartbeat #${heartbeatCount} | RSS: ${Math.round(mem.rss / 1024 / 1024)}MB | Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
}

// Graceful shutdown
let isShuttingDown = false;
let distributedPollingRef = null; // Set during main()

async function shutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    logger.info(`\n📴 Received ${signal}, shutting down gracefully...`);

    // Stop distributed polling first (announces departure)
    if (distributedPollingRef) {
        try {
            await distributedPollingRef.stop();
            logger.info('✅ Distributed polling stopped');
        } catch (e) {
            logger.warn(`⚠️ Error stopping distributed polling: ${e.message}`);
        }
    }

    // Give tasks time to complete current work
    await new Promise(r => setTimeout(r, 2000));
    process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Catch unhandled errors
process.on('uncaughtException', (err) => {
    logger.error(`❌ Uncaught Exception: ${err.message}`);
    logger.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error(`❌ Unhandled Rejection at: ${promise}`);
    logger.error(`   Reason: ${reason}`);
});

// Main startup
async function main() {
    try {
        // 0. Node Identity
        const nodeId = process.env.NODE_ID || `calc-${process.pid}`;
        const nodeName = process.env.NODE_NAME || 'Calculator Node';
        const useDistributed = process.env.USE_DISTRIBUTED_POLLING === 'true';

        logger.info(`🔑 Node: ${nodeId} (${nodeName})`);
        logger.info(`   Mode: ${useDistributed ? 'DISTRIBUTED' : 'STANDALONE'}`);

        // 1. Initialize Database
        logger.info('📊 Initializing Database...');
        const { initDB, getDB } = require('./services/database');
        await initDB();
        const db = getDB();
        logger.info('✅ Database Ready');

        // 1.5. Initialize Genesis Nodes (foundation of trust)
        logger.info('🌱 Initializing Genesis Nodes...');
        const nodeApproval = require('./services/nodeApproval');
        await nodeApproval.initializeGenesisNodes(db);
        logger.info('✅ Genesis Nodes Ready');

        // 2. Initialize Redis (required for distributed mode)
        logger.info('🔴 Initializing Redis...');
        const { connectRedis } = require('./services/redis');
        const redis = await connectRedis();
        if (redis) {
            logger.info('✅ Redis Ready');
        } else {
            if (useDistributed) {
                logger.error('❌ Redis REQUIRED for distributed mode');
                process.exit(1);
            }
            logger.warn('⚠️ Redis unavailable - running in standalone mode');
        }

        // 2.1 Initialize Node Service (for distributed identity)
        const nodeService = require('./services/nodeService');
        await nodeService.initializeNode(db);
        const currentNodeId = nodeService.getNodeId();
        logger.info(`✅ Node registered: ${currentNodeId}`);

        // 2.2 Verify Node Approval Status
        const genesis = require('./config/genesis');
        const isApproved = await nodeApproval.isNodeApproved(db, currentNodeId);
        const isGenesis = genesis.isGenesisNode(currentNodeId);

        if (isApproved || isGenesis) {
            logger.info(`🔐 Node ${currentNodeId} is ${isGenesis ? 'GENESIS' : 'APPROVED'} - can participate in consensus`);
        } else {
            logger.warn(`⚠️ Node ${currentNodeId} is NOT APPROVED - running in observation mode`);
            logger.warn('   To get approved, request approval from existing nodes');
        }

        // Build dependencies object
        const deps = { db };

        // 2.5. Re-sign any tampered tokens (after secret rotation)
        // CRITICAL: Clear stale Redis snapshots FIRST to prevent Watchdog from
        // "healing" with old signatures during the re-sign process
        logger.info('🔐 Clearing stale Redis snapshots...');
        const { getClient: getRedisClient } = require('./services/redis');
        const redisClient = getRedisClient();
        if (redisClient) {
            try {
                // Get all snapshot keys and delete them
                const snapshotKeys = await redisClient.keys('kscore:snapshot:*');
                if (snapshotKeys.length > 0) {
                    await redisClient.del(...snapshotKeys);
                    logger.info(`🗑️ Cleared ${snapshotKeys.length} stale Redis snapshots`);
                } else {
                    logger.info('✅ No stale snapshots to clear');
                }
            } catch (e) {
                logger.warn(`⚠️ Failed to clear Redis snapshots: ${e.message}`);
            }
        }

        logger.info('🔐 Re-signing all verified tokens with current secret...');
        const { signAllCategories } = require('./utils/dataSignature');
        const tamperedTokens = await db.all(`
            SELECT mint, symbol, name, image, decimals,
                   k_score, conviction_score, conviction_accumulators,
                   conviction_holders, conviction_reducers, conviction_extractors,
                   conviction_analyzed, holders, priceusd, marketcap, liquidity,
                   supply, initial_supply, burned_amount, burned_percent,
                   mint_authority_revoked, freeze_authority_revoked, is_mutable_supply,
                   hasCommunityUpdate, lp_burn_pct, lp_locked_pct, lp_status,
                   is_pump_fun, bonding_curve_complete, timestamp, metadata,
                   price_source, price_timestamp, price_pool,
                   mcap_calculated, liquidity_source, liquidity_timestamp,
                   holders_source, holders_timestamp, age_days,
                   last_k_score_update
            FROM tokens WHERE hasCommunityUpdate = TRUE
        `);

        if (tamperedTokens.length > 0) {
            logger.info(`🔐 Re-signing ${tamperedTokens.length} tokens with current secret...`);
            const { saveSnapshot } = require('./tasks/integrityWatchdog');
            let resigned = 0;
            for (const token of tamperedTokens) {
                try {
                    const signatures = signAllCategories(token);
                    await db.run(`
                        UPDATE tokens SET
                            sig_identity = $1, sig_security = $2, sig_lp = $3,
                            sig_supply = $4, sig_kscore = $5, sig_market = $6,
                            sig_origin = $7, sig_full = $8, chaos_nonce = $9
                        WHERE mint = $10
                    `, [
                        signatures.sig_identity, signatures.sig_security, signatures.sig_lp,
                        signatures.sig_supply, signatures.sig_kscore, signatures.sig_market,
                        signatures.sig_origin, signatures.sig_full, signatures.chaos_nonce,
                        token.mint
                    ]);

                    // CRITICAL: Update Redis snapshot with new signatures
                    // Otherwise watchdog will "heal" by restoring OLD signatures
                    const tokenWithNewSigs = { ...token, ...signatures };
                    await saveSnapshot(token.mint, tokenWithNewSigs);

                    resigned++;
                } catch (e) {
                    logger.error(`Failed to re-sign ${token.symbol}: ${e.message}`);
                }
            }
            logger.info(`✅ Re-signed ${resigned}/${tamperedTokens.length} tokens (DB + Redis snapshots)`);
        } else {
            logger.info('✅ No verified tokens found to re-sign');
        }

        // 3. Initialize Distributed Polling (if enabled)
        let distributedPolling = null;
        if (useDistributed) {
            logger.info('🌐 Initializing Distributed Polling...');
            distributedPolling = require('./services/distributedPolling');
            const initialized = await distributedPolling.initialize({
                capabilities: (process.env.NODE_CAPABILITIES || 'polling,webhooks,verification').split(',')
            });
            if (initialized) {
                distributedPollingRef = distributedPolling; // For graceful shutdown
                logger.info('✅ Distributed Polling Ready');
            } else {
                logger.warn('⚠️ Distributed Polling failed - falling back to standalone');
            }
        }

        // 4. Start K-Score Updater
        logger.info('📈 Starting K-Score Updater...');
        const kScoreUpdater = require('./tasks/kScoreUpdater');
        kScoreUpdater.start(deps);
        logger.info('✅ K-Score Updater Running');

        // 5. Start Distributed Polling (if initialized)
        if (distributedPolling) {
            distributedPolling.start(deps);
            logger.info('✅ Distributed Polling Running');

            // Log network status periodically
            setInterval(async () => {
                const status = await distributedPolling.getNetworkStatus();
                logger.info(`🌐 Network: ${status.nodes_active} nodes | ${status.tasks_pending} tasks | credits: ${status.my_credits?.toFixed(2)}`);
            }, 30000); // Every 30s
        }

        // 6. Start Metadata Updater
        logger.info('📊 Starting Metadata Updater...');
        const metadataUpdater = require('./tasks/metadataUpdater');
        metadataUpdater.start(deps);
        logger.info('✅ Metadata Updater Running');

        // 7. Start Grower Scanner
        logger.info('🌱 Starting Grower Scanner...');
        const growerScanner = require('./tasks/growerScanner');
        growerScanner.start(deps);
        logger.info('✅ Grower Scanner Running');

        // Start heartbeat
        setInterval(logHeartbeat, 60000); // Every minute
        logHeartbeat(); // First one immediately

        logger.info('='.repeat(50));
        logger.info('🧠 Calculator Brain ACTIVE');
        logger.info(`   Mode: ${useDistributed ? 'DISTRIBUTED' : 'STANDALONE'}`);
        logger.info('   Running: K-Score, Metadata, Growers');
        if (useDistributed) {
            logger.info('   Network: Distributed polling enabled');
        }
        logger.info('='.repeat(50));

    } catch (err) {
        logger.error(`❌ Calculator Failed to Start: ${err.message}`);
        logger.error(err.stack);
        process.exit(1);
    }
}

// Keep process alive
process.stdin.resume();

// Start
main();
