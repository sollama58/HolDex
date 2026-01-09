#!/usr/bin/env node
/**
 * HolDex Token Indexer Diagnostic Tool
 *
 * Checks all components required for new token indexing:
 * - Database connectivity
 * - Redis connectivity
 * - Solana RPC/WSS connectivity
 * - Listener worker status
 * - Token search functionality
 *
 * Usage: node scripts/diagnose-indexer.js [MINT_ADDRESS]
 */

require('dotenv').config();
const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../src/config/env');

// Color codes for terminal
const colors = {
    green: '\x1b[32m',
    red: '\x1b[31m',
    yellow: '\x1b[33m',
    blue: '\x1b[36m',
    reset: '\x1b[0m'
};

function pass(msg) {
    console.log(`${colors.green}✅ ${msg}${colors.reset}`);
}

function fail(msg) {
    console.log(`${colors.red}❌ ${msg}${colors.reset}`);
}

function warn(msg) {
    console.log(`${colors.yellow}⚠️  ${msg}${colors.reset}`);
}

function info(msg) {
    console.log(`${colors.blue}ℹ️  ${msg}${colors.reset}`);
}

async function checkRedis() {
    console.log('\n📊 Redis Connection:');
    try {
        const { initRedis, getClient } = require('../src/services/redis');
        await initRedis();
        const redis = getClient();
        if (!redis) {
            fail('Redis client is null');
            return false;
        }
        const result = await redis.ping();
        if (result === 'PONG') {
            pass('Redis connected and responding');
            return true;
        } else {
            fail('Redis ping returned unexpected response: ' + result);
            return false;
        }
    } catch (e) {
        fail('Redis connection failed: ' + e.message);
        return false;
    }
}

async function checkDatabase() {
    console.log('\n💾 Database Connection:');
    try {
        const { initDB, getDB } = require('../src/services/database');
        await initDB();
        const db = getDB();

        const result = await db.get('SELECT COUNT(*) as count FROM tokens');
        pass(`Database connected (${result.count} tokens in database)`);

        // Check for recent additions
        const recent = await db.get(`
            SELECT mint, symbol, timestamp
            FROM tokens
            ORDER BY timestamp DESC
            LIMIT 1
        `);

        if (recent) {
            const age = Math.floor((Date.now() - recent.timestamp) / (60 * 60 * 1000));
            info(`Most recent token: ${recent.symbol} (${age}h ago)`);
        }

        return true;
    } catch (e) {
        fail('Database connection failed: ' + e.message);
        return false;
    }
}

async function checkSolanaRPC() {
    console.log('\n🔌 Solana RPC Connection:');
    try {
        const rpcUrl = config.SOLANA_RPC_URL || config.RPC_URL || 'https://api.mainnet-beta.solana.com';
        info(`Testing RPC: ${rpcUrl.replace(/\?api-key=[^&]+/, '?api-key=***')}`);

        const connection = new Connection(rpcUrl, {
            commitment: 'confirmed'
        });

        const blockHeight = await connection.getBlockHeight();
        pass(`RPC connected (block height: ${blockHeight})`);
        return true;
    } catch (e) {
        fail('RPC connection failed: ' + e.message);
        return false;
    }
}

async function checkWebSocket() {
    console.log('\n🛰️  WebSocket Connection:');

    if (!config.HELIUS_API_KEY && !config.SOLANA_WSS_URL) {
        fail('No WebSocket endpoint configured (missing HELIUS_API_KEY or SOLANA_WSS_URL)');
        return false;
    }

    try {
        let wsUrl = config.SOLANA_WSS_URL;
        if (!wsUrl && config.HELIUS_API_KEY) {
            wsUrl = `wss://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
        }

        info(`Testing WSS: ${wsUrl.replace(/\?api-key=[^&]+/, '?api-key=***')}`);

        const rpcUrl = config.SOLANA_RPC_URL || 'https://mainnet.helius-rpc.com';
        const connection = new Connection(rpcUrl, {
            commitment: 'confirmed',
            wsEndpoint: wsUrl
        });

        return new Promise((resolve) => {
            let timeout;
            let subscriptionId;

            // Test subscription to Raydium program
            const RAYDIUM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

            subscriptionId = connection.onLogs(
                RAYDIUM_PROGRAM_ID,
                (logs) => {
                    clearTimeout(timeout);
                    pass('WebSocket connected and receiving logs');
                    connection.removeOnLogsListener(subscriptionId);
                    resolve(true);
                },
                'processed'
            );

            info('Waiting 10 seconds for WebSocket logs...');
            timeout = setTimeout(() => {
                warn('No logs received in 10 seconds (may be normal during low activity)');
                connection.removeOnLogsListener(subscriptionId);
                resolve(true); // Still pass - subscription worked, just no activity
            }, 10000);
        });
    } catch (e) {
        fail('WebSocket connection failed: ' + e.message);
        return false;
    }
}

async function checkListenerProcess() {
    console.log('\n🎧 Listener Worker Process:');

    const { exec } = require('child_process');
    return new Promise((resolve) => {
        exec('ps aux | grep listener_worker | grep -v grep', (error, stdout) => {
            if (stdout && stdout.trim()) {
                pass('Listener worker process is running');
                resolve(true);
            } else {
                fail('Listener worker process not found');
                info('Start with: npm run listener');
                resolve(false);
            }
        });
    });
}

async function testTokenIndexing(mint) {
    console.log(`\n🔍 Testing Token Indexing: ${mint}`);

    try {
        const { indexTokenOnChain } = require('../src/services/indexer');
        const { getDB } = require('../src/services/database');

        info('Attempting to index token...');
        const result = await indexTokenOnChain(mint);

        if (result) {
            pass(`Token indexed successfully: ${result.name || 'Unknown'} (${result.ticker || 'N/A'})`);
            if (result.pairs && result.pairs.length > 0) {
                info(`Found ${result.pairs.length} liquidity pool(s)`);
            }

            // Verify in database
            const db = getDB();
            const dbToken = await db.get('SELECT * FROM tokens WHERE mint = $1', [mint]);
            if (dbToken) {
                pass('Token exists in database');
                info(`Symbol: ${dbToken.symbol}, K-Score: ${dbToken.k_score || 0}, Community Update: ${dbToken.hascommunityupdate || false}`);
            } else {
                warn('Token not found in database after indexing');
            }

            return true;
        } else {
            fail('Token indexing returned null');
            return false;
        }
    } catch (e) {
        fail('Token indexing failed: ' + e.message);
        console.error(e.stack);
        return false;
    }
}

async function main() {
    console.log(`
╔═══════════════════════════════════════════════════════╗
║     HolDex Token Indexer Diagnostic Tool             ║
╚═══════════════════════════════════════════════════════╝
`);

    const results = [];

    // Run all checks
    results.push(await checkRedis());
    results.push(await checkDatabase());
    results.push(await checkSolanaRPC());
    results.push(await checkWebSocket());
    results.push(await checkListenerProcess());

    // Test token indexing if mint provided
    const testMint = process.argv[2];
    if (testMint) {
        results.push(await testTokenIndexing(testMint));
    } else {
        info('\n💡 To test token indexing, provide a mint address:');
        info('   node scripts/diagnose-indexer.js EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
    }

    // Summary
    const passed = results.filter(r => r === true).length;
    const total = results.length;

    console.log('\n' + '═'.repeat(55));
    if (passed === total) {
        pass(`All checks passed (${passed}/${total})`);
        console.log('\n✨ Token indexer appears to be working correctly!');
    } else {
        fail(`Some checks failed (${passed}/${total} passed)`);
        console.log('\n📖 See docs/TROUBLESHOOTING.md for detailed solutions');
    }
    console.log('═'.repeat(55) + '\n');

    process.exit(passed === total ? 0 : 1);
}

main().catch(err => {
    console.error('\n❌ Diagnostic tool crashed:', err);
    process.exit(1);
});
