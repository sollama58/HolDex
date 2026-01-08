#!/usr/bin/env node
/**
 * Test New Token Webhook Endpoint
 *
 * Simule un payload Helius pour tester l'endpoint /webhook/new-tokens
 *
 * Usage:
 *   node src/scripts/test_new_token_webhook.js
 *   node src/scripts/test_new_token_webhook.js --url http://localhost:3000
 */

const http = require('http');
const https = require('https');

const BASE_URL = process.argv[2] === '--url'
    ? process.argv[3]
    : 'http://localhost:3000';

// Mock Helius webhook payload - CREATE_POOL event
const mockCreatePoolEvent = {
    signature: 'test_sig_' + Date.now(),
    type: 'CREATE_POOL',
    timestamp: Math.floor(Date.now() / 1000),
    tokenTransfers: [
        {
            mint: 'TestToken' + Math.random().toString(36).substring(7) + 'pump', // Random test mint
            fromUserAccount: '11111111111111111111111111111111',
            toUserAccount: 'BuyerWallet111111111111111111111111111111111',
            tokenAmount: 1000000000
        }
    ],
    accountData: [],
    instructions: [
        {
            programId: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium
            accounts: []
        }
    ]
};

// Mock TOKEN_MINT event from Pump.fun
const mockTokenMintEvent = {
    signature: 'test_mint_' + Date.now(),
    type: 'TOKEN_MINT',
    timestamp: Math.floor(Date.now() / 1000),
    tokenTransfers: [
        {
            mint: 'PumpToken' + Math.random().toString(36).substring(7) + 'xyz',
            fromUserAccount: null,
            toUserAccount: 'CreatorWallet1111111111111111111111111111111',
            tokenAmount: 1000000000000
        }
    ],
    instructions: [
        {
            programId: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', // Pump.fun
            accounts: []
        }
    ]
};

async function sendWebhook(payload, description) {
    return new Promise((resolve, reject) => {
        const url = new URL(`${BASE_URL}/webhook/new-tokens`);
        const isHttps = url.protocol === 'https:';
        const client = isHttps ? https : http;

        const data = JSON.stringify([payload]);

        const options = {
            hostname: url.hostname,
            port: url.port || (isHttps ? 443 : 80),
            path: url.pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(data)
            }
        };

        console.log(`\n📤 Sending: ${description}`);
        console.log(`   URL: ${url.href}`);
        console.log(`   Signature: ${payload.signature}`);
        console.log(`   Type: ${payload.type}`);

        const req = client.request(options, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
                console.log(`   Status: ${res.statusCode}`);
                try {
                    const json = JSON.parse(body);
                    console.log(`   Response:`, JSON.stringify(json, null, 2));
                    resolve({ status: res.statusCode, body: json });
                } catch (e) {
                    console.log(`   Response: ${body}`);
                    resolve({ status: res.statusCode, body });
                }
            });
        });

        req.on('error', (e) => {
            console.log(`   ❌ Error: ${e.message}`);
            reject(e);
        });

        req.write(data);
        req.end();
    });
}

async function testStats() {
    return new Promise((resolve, reject) => {
        const url = new URL(`${BASE_URL}/webhook/new-tokens/stats`);
        const isHttps = url.protocol === 'https:';
        const client = isHttps ? https : http;

        console.log(`\n📊 Getting stats: ${url.href}`);

        client.get(url.href, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
                console.log(`   Status: ${res.statusCode}`);
                try {
                    const json = JSON.parse(body);
                    console.log(`   Stats:`, JSON.stringify(json, null, 2));
                    resolve(json);
                } catch (e) {
                    console.log(`   Response: ${body}`);
                    resolve(body);
                }
            });
        }).on('error', (e) => {
            console.log(`   ❌ Error: ${e.message}`);
            reject(e);
        });
    });
}

async function main() {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  NEW TOKEN WEBHOOK TEST');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`Target: ${BASE_URL}`);

    try {
        // Test 1: CREATE_POOL event (Raydium)
        await sendWebhook(mockCreatePoolEvent, 'CREATE_POOL (Raydium)');

        // Test 2: TOKEN_MINT event (Pump.fun)
        await sendWebhook(mockTokenMintEvent, 'TOKEN_MINT (Pump.fun)');

        // Test 3: Duplicate (should be skipped)
        console.log('\n🔄 Testing duplicate detection...');
        await sendWebhook(mockCreatePoolEvent, 'Duplicate (same signature)');

        // Test 4: Get stats
        await testStats();

        console.log('\n═══════════════════════════════════════════════════════════════');
        console.log('✅ TEST COMPLETE');
        console.log('═══════════════════════════════════════════════════════════════');

    } catch (error) {
        console.error('\n❌ Test failed:', error.message);
        console.log('\nMake sure the server is running:');
        console.log('  cd /workspaces/HolDex && npm run dev');
        process.exit(1);
    }
}

main();
