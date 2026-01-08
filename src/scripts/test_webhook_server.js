#!/usr/bin/env node
/**
 * Minimal Test Server for New Token Webhook
 *
 * Serveur isolé pour tester l'endpoint webhook sans dépendances complètes.
 *
 * Usage:
 *   node src/scripts/test_webhook_server.js
 *   # Dans un autre terminal:
 *   node src/scripts/test_new_token_webhook.js
 */

const express = require('express');
const app = express();

// Mock dependencies
const mockDb = {
    get: async (query, params) => {
        console.log(`   [DB] GET: ${params?.[0]?.slice(0, 20)}...`);
        return null; // Simule "token non existant"
    },
    run: async (query, params) => {
        console.log(`   [DB] INSERT: ${params?.[0]?.slice(0, 20)}...`);
        return { changes: 1 };
    }
};

const mockRedis = {
    set: async () => true,
    get: async () => null,
    sadd: async () => 1
};

// Mock modules
const mockLogger = {
    info: (...args) => console.log('ℹ️ ', ...args),
    warn: (...args) => console.log('⚠️ ', ...args),
    error: (...args) => console.log('❌', ...args),
    debug: () => {}
};

// Mock config
const mockConfig = {
    WEBHOOK_SECRET: null, // Pas de vérification en test
    API_URL: 'http://localhost:3001'
};

// Override requires
require.cache[require.resolve('../services/logger')] = { exports: mockLogger };
require.cache[require.resolve('../config/env')] = { exports: mockConfig };
require.cache[require.resolve('../services/redis')] = {
    exports: { getClient: () => mockRedis }
};
require.cache[require.resolve('../utils/validation')] = {
    exports: {
        isValidSolanaAddress: (addr) => addr && addr.length >= 32,
        sanitizeError: (e) => e.message
    }
};
require.cache[require.resolve('../services/indexer')] = {
    exports: {
        indexTokenOnChain: async (mint) => {
            console.log(`   [Indexer] Queued: ${mint.slice(0, 20)}...`);
        }
    }
};
require.cache[require.resolve('../services/verificationService')] = {
    exports: { verifyTransaction: async () => ({ verified: true }) }
};

// Now load the actual webhook modules
const newTokenWebhook = require('../services/newTokenWebhook');

// Setup express
app.use(express.json());

// Stats tracking
const stats = {
    eventsReceived: 0,
    tokensDiscovered: 0,
    duplicatesSkipped: 0,
    errors: 0,
    lastEventTime: 0
};

const processedSignatures = new Set();

// ═══════════════════════════════════════════════════════════════
// ENDPOINT: POST /webhook/new-tokens
// ═══════════════════════════════════════════════════════════════
app.post('/webhook/new-tokens', async (req, res) => {
    stats.eventsReceived++;
    stats.lastEventTime = Date.now();

    console.log('\n' + '═'.repeat(60));
    console.log('📥 WEBHOOK RECEIVED');
    console.log('═'.repeat(60));

    try {
        const events = Array.isArray(req.body) ? req.body : [req.body];
        let discovered = 0;
        let skipped = 0;

        for (const event of events) {
            console.log(`\n📋 Event: ${event.type} (sig: ${event.signature?.slice(0, 16)}...)`);

            // Replay protection
            if (event.signature && processedSignatures.has(event.signature)) {
                console.log('   ⏭️  Skipped: duplicate signature');
                stats.duplicatesSkipped++;
                skipped++;
                continue;
            }
            processedSignatures.add(event.signature);

            // Extract mints
            const source = newTokenWebhook.detectSource(event);
            const mints = newTokenWebhook.extractMintsFromEvent(event);

            console.log(`   Source: ${source}`);
            console.log(`   Mints found: ${mints.length}`);

            if (mints.length === 0) {
                skipped++;
                continue;
            }

            for (const mint of mints) {
                // Check DB (mock always returns null = new token)
                const exists = await mockDb.get('SELECT...', [mint]);

                if (exists) {
                    skipped++;
                    continue;
                }

                // New token!
                console.log(`   ✨ NEW TOKEN: ${mint}`);
                stats.tokensDiscovered++;
                discovered++;

                // Insert to DB
                await mockDb.run('INSERT...', [mint]);

                // Queue for indexing
                require('../services/indexer').indexTokenOnChain(mint);
            }
        }

        console.log(`\n📊 Result: Discovered=${discovered}, Skipped=${skipped}`);

        res.json({
            received: true,
            discovered,
            skipped,
            stats
        });

    } catch (error) {
        stats.errors++;
        console.error('❌ Error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// ═══════════════════════════════════════════════════════════════
// ENDPOINT: GET /webhook/new-tokens/stats
// ═══════════════════════════════════════════════════════════════
app.get('/webhook/new-tokens/stats', (req, res) => {
    res.json(stats);
});

// ═══════════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════════
const PORT = 3001;

app.listen(PORT, () => {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  NEW TOKEN WEBHOOK - TEST SERVER');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`\n🚀 Server running on http://localhost:${PORT}`);
    console.log(`\n📍 Endpoints:`);
    console.log(`   POST /webhook/new-tokens     - Receive events`);
    console.log(`   GET  /webhook/new-tokens/stats - Get statistics`);
    console.log(`\n📝 Run test:`);
    console.log(`   node src/scripts/test_new_token_webhook.js --url http://localhost:${PORT}`);
    console.log('\n⏳ Waiting for events...\n');
});
