/**
 * Token Queue Service
 *
 * New tokens are added to a Redis queue instead of directly to the database.
 * A processor fetches metadata and only adds tokens to DB once real data is available.
 * This prevents "New Discovery" / "Unknown" placeholder tokens.
 */

const { getClient } = require('./redis');
const { getDB } = require('./database');
const { fetchTokenMetadata } = require('../utils/metaplex');
const { getSolanaConnection } = require('./solana');
const { PublicKey } = require('@solana/web3.js');
const logger = require('./logger');

const QUEUE_KEY = 'holdex:new_token_queue';
const PROCESSING_KEY = 'holdex:processing_tokens';
const FAILED_KEY = 'holdex:failed_tokens';
const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 3000;
const PROCESS_INTERVAL_MS = 2000;

let isProcessing = false;
let processorInterval = null;

/**
 * Add a new token to the queue for processing
 * @param {string} mint - Token mint address
 * @param {string} source - Discovery source (e.g., 'Raydium', 'Pump.fun')
 */
async function queueNewToken(mint, source = 'unknown') {
    const redis = getClient();
    if (!redis) {
        logger.warn(`[TokenQueue] Redis not available, cannot queue ${mint.slice(0, 8)}`);
        return false;
    }

    try {
        // Check if already in queue, processing, or in database
        const [inQueue, inProcessing, inDb] = await Promise.all([
            redis.sismember(QUEUE_KEY, mint),
            redis.sismember(PROCESSING_KEY, mint),
            getDB().get('SELECT mint FROM tokens WHERE mint = $1', [mint])
        ]);

        if (inQueue || inProcessing || inDb) {
            logger.debug(`[TokenQueue] ${mint.slice(0, 8)} already queued/processing/exists`);
            return false;
        }

        // Add to queue with metadata
        const queueData = JSON.stringify({
            mint,
            source,
            queuedAt: Date.now(),
            retries: 0
        });

        await redis.sadd(QUEUE_KEY, mint);
        await redis.hset('holdex:queue_data', mint, queueData);

        logger.info(`📥 [TokenQueue] Queued ${mint.slice(0, 8)} from ${source}`);
        return true;
    } catch (err) {
        logger.error(`[TokenQueue] Failed to queue ${mint.slice(0, 8)}: ${err.message}`);
        return false;
    }
}

/**
 * Process a single token from the queue
 */
async function processToken(mint) {
    const redis = getClient();
    const db = getDB();
    const connection = getSolanaConnection();

    if (!redis || !db) return false;

    try {
        // Get queue data
        const rawData = await redis.hget('holdex:queue_data', mint);
        const data = rawData ? JSON.parse(rawData) : { mint, retries: 0, source: 'unknown' };

        // Move to processing set
        await redis.smove(QUEUE_KEY, PROCESSING_KEY, mint);

        logger.info(`🔄 [TokenQueue] Processing ${mint.slice(0, 8)} (attempt ${data.retries + 1}/${MAX_RETRIES})`);

        // Fetch metadata
        const meta = await fetchTokenMetadata(mint);

        // Check if we got real metadata (not placeholder)
        const hasRealName = meta && meta.name && meta.name !== 'Unknown' && meta.name !== 'New Discovery' && meta.name.length > 0;
        const hasRealSymbol = meta && meta.symbol && meta.symbol !== 'UNK' && meta.symbol !== 'UNKNOWN' && meta.symbol !== 'NEW';

        if (!hasRealName || !hasRealSymbol) {
            // Metadata not ready - retry or fail
            data.retries++;

            if (data.retries >= MAX_RETRIES) {
                logger.warn(`⚠️ [TokenQueue] Max retries reached for ${mint.slice(0, 8)}, moving to failed queue`);
                await redis.smove(PROCESSING_KEY, FAILED_KEY, mint);
                await redis.hset('holdex:queue_data', mint, JSON.stringify({ ...data, failedAt: Date.now(), reason: 'metadata_unavailable' }));
                return false;
            }

            // Re-queue for retry
            logger.info(`⏳ [TokenQueue] Metadata not ready for ${mint.slice(0, 8)}, will retry (${data.retries}/${MAX_RETRIES})`);
            await redis.smove(PROCESSING_KEY, QUEUE_KEY, mint);
            await redis.hset('holdex:queue_data', mint, JSON.stringify(data));
            return false;
        }

        // Fetch supply info
        let supply = '1000000000';
        let decimals = 9;
        try {
            const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
            supply = supplyInfo.value.amount;
            decimals = supplyInfo.value.decimals;
        } catch (e) {
            logger.warn(`[TokenQueue] Failed to fetch supply for ${mint.slice(0, 8)}: ${e.message}`);
        }

        // Insert into database with REAL metadata
        await db.run(`
            INSERT INTO tokens (mint, name, symbol, image, supply, decimals, timestamp, k_score, hasCommunityUpdate, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 0, FALSE, NOW())
            ON CONFLICT(mint) DO UPDATE SET
                name = CASE WHEN tokens.name IN ('Unknown', 'New Discovery', '') OR tokens.name IS NULL THEN EXCLUDED.name ELSE tokens.name END,
                symbol = CASE WHEN tokens.symbol IN ('UNKNOWN', 'UNK', 'NEW', '') OR tokens.symbol IS NULL THEN EXCLUDED.symbol ELSE tokens.symbol END,
                image = CASE WHEN tokens.image IS NULL OR tokens.image = '' THEN EXCLUDED.image ELSE tokens.image END,
                supply = COALESCE(EXCLUDED.supply, tokens.supply),
                decimals = COALESCE(EXCLUDED.decimals, tokens.decimals),
                updated_at = NOW()
        `, [mint, meta.name, meta.symbol, meta.image, supply, decimals, Date.now()]);

        // Cleanup queue data
        await redis.srem(PROCESSING_KEY, mint);
        await redis.hdel('holdex:queue_data', mint);

        logger.info(`✅ [TokenQueue] Successfully added ${meta.name} (${meta.symbol}) - ${mint.slice(0, 8)}`);

        // Trigger full indexing (pools, market data) in background
        const { indexTokenOnChain } = require('./indexer');
        indexTokenOnChain(mint).catch(e => logger.warn(`[TokenQueue] Background indexing failed for ${mint.slice(0, 8)}: ${e.message}`));

        return true;
    } catch (err) {
        logger.error(`[TokenQueue] Error processing ${mint.slice(0, 8)}: ${err.message}`);

        // Move back to queue for retry
        await redis.smove(PROCESSING_KEY, QUEUE_KEY, mint).catch(() => {});
        return false;
    }
}

/**
 * Process tokens from the queue
 */
async function processQueue() {
    if (isProcessing) return;
    isProcessing = true;

    const redis = getClient();
    if (!redis) {
        isProcessing = false;
        return;
    }

    try {
        // Get tokens to process (up to 5 at a time)
        const tokens = await redis.smembers(QUEUE_KEY);

        if (tokens.length === 0) {
            isProcessing = false;
            return;
        }

        logger.info(`📋 [TokenQueue] Processing ${tokens.length} queued token(s)`);

        // Process in batches with delay between each
        for (const mint of tokens.slice(0, 5)) {
            await processToken(mint);
            await new Promise(r => setTimeout(r, RETRY_DELAY_MS));
        }
    } catch (err) {
        logger.error(`[TokenQueue] Queue processing error: ${err.message}`);
    } finally {
        isProcessing = false;
    }
}

/**
 * Start the queue processor
 */
function startQueueProcessor() {
    if (processorInterval) {
        logger.warn('[TokenQueue] Processor already running');
        return;
    }

    logger.info('🚀 [TokenQueue] Starting queue processor');
    processorInterval = setInterval(processQueue, PROCESS_INTERVAL_MS);

    // Process immediately on start
    processQueue();
}

/**
 * Stop the queue processor
 */
function stopQueueProcessor() {
    if (processorInterval) {
        clearInterval(processorInterval);
        processorInterval = null;
        logger.info('🛑 [TokenQueue] Queue processor stopped');
    }
}

/**
 * Get queue statistics
 */
async function getQueueStats() {
    const redis = getClient();
    if (!redis) return null;

    const [queued, processing, failed] = await Promise.all([
        redis.scard(QUEUE_KEY),
        redis.scard(PROCESSING_KEY),
        redis.scard(FAILED_KEY)
    ]);

    return { queued, processing, failed };
}

/**
 * Retry failed tokens
 */
async function retryFailedTokens() {
    const redis = getClient();
    if (!redis) return 0;

    const failed = await redis.smembers(FAILED_KEY);
    let retried = 0;

    for (const mint of failed) {
        const rawData = await redis.hget('holdex:queue_data', mint);
        if (rawData) {
            const data = JSON.parse(rawData);
            data.retries = 0; // Reset retries
            await redis.hset('holdex:queue_data', mint, JSON.stringify(data));
        }
        await redis.smove(FAILED_KEY, QUEUE_KEY, mint);
        retried++;
    }

    logger.info(`🔄 [TokenQueue] Retried ${retried} failed tokens`);
    return retried;
}

module.exports = {
    queueNewToken,
    processQueue,
    startQueueProcessor,
    stopQueueProcessor,
    getQueueStats,
    retryFailedTokens
};
