#!/usr/bin/env node
/**
 * Re-sign All Tokens
 *
 * Use after rotating DATA_SIGNING_SECRET to re-sign all tokens
 * with the new secret key.
 *
 * Usage: node src/scripts/resignAllTokens.js
 */

require('dotenv').config();

const { initDB, getDB } = require('../services/database');
const { connectRedis } = require('../services/redis');
const { signAllCategories } = require('../utils/dataSignature');
const { saveSnapshot } = require('../tasks/integrityWatchdog');

async function main() {
    console.log('='.repeat(60));
    console.log('RE-SIGN ALL TOKENS - After Secret Rotation');
    console.log('='.repeat(60));

    // Init DB
    await initDB();
    const db = getDB();

    // Init Redis (needed for snapshot updates)
    console.log('Connecting to Redis...');
    await connectRedis();
    console.log('✅ Redis connected');

    // Get all tokens with signatures
    // IMPORTANT: Must include ALL fields used by signAllCategories, including last_k_score_update
    const tokens = await db.all(`
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
        FROM tokens
        WHERE hasCommunityUpdate = TRUE
    `);

    console.log(`\nFound ${tokens.length} verified tokens to re-sign\n`);

    let success = 0;
    let failed = 0;

    for (const token of tokens) {
        try {
            // Generate new signatures with new secret
            const signatures = signAllCategories(token);

            // Update token with new signatures
            await db.run(`
                UPDATE tokens SET
                    sig_identity = $1,
                    sig_security = $2,
                    sig_lp = $3,
                    sig_supply = $4,
                    sig_kscore = $5,
                    sig_market = $6,
                    sig_origin = $7,
                    sig_full = $8,
                    chaos_nonce = $9
                WHERE mint = $10
            `, [
                signatures.sig_identity,
                signatures.sig_security,
                signatures.sig_lp,
                signatures.sig_supply,
                signatures.sig_kscore,
                signatures.sig_market,
                signatures.sig_origin,
                signatures.sig_full,
                signatures.chaos_nonce,
                token.mint
            ]);

            // CRITICAL: Update Redis snapshot with new signatures
            // Otherwise watchdog will "heal" by restoring OLD signatures
            const tokenWithNewSigs = { ...token, ...signatures };
            await saveSnapshot(token.mint, tokenWithNewSigs);

            success++;
            console.log(`✅ ${token.symbol.padEnd(12)} (${token.mint.slice(0, 8)}...) [DB + Redis]`);

        } catch (e) {
            failed++;
            console.log(`❌ ${token.symbol.padEnd(12)} - ${e.message}`);
        }
    }

    console.log('\n' + '='.repeat(60));
    console.log(`COMPLETE: ${success} re-signed (DB + Redis), ${failed} failed`);
    console.log('NOTE: Redis snapshots updated - watchdog will no longer "heal" back to old signatures');
    console.log('='.repeat(60));

    process.exit(0);
}

main().catch(e => {
    console.error('Fatal error:', e);
    process.exit(1);
});
