#!/usr/bin/env node
/**
 * Setup Production Webhook for New Token Discovery
 */

const HELIUS_API_KEY = process.env.HELIUS_API_KEY || 'e7c9c7ca-f9c1-4515-ac41-ae664468506c';
const CALLBACK_URL = 'https://holdex-api.onrender.com/webhook/new-tokens';

const PROGRAMS = {
    RAYDIUM_V4: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    PUMP_FUN: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
};

const TX_TYPES = ['CREATE_POOL', 'TOKEN_MINT', 'SWAP'];

async function setup() {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  HELIUS WEBHOOK SETUP - PRODUCTION');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log();
    console.log('Callback URL:', CALLBACK_URL);
    console.log('Programs:', Object.keys(PROGRAMS).join(', '));
    console.log('Transaction Types:', TX_TYPES.join(', '));
    console.log();

    const response = await fetch(`https://api.helius.xyz/v0/webhooks?api-key=${HELIUS_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            webhookURL: CALLBACK_URL,
            transactionTypes: TX_TYPES,
            accountAddresses: Object.values(PROGRAMS),
            webhookType: 'enhanced',
            encoding: 'jsonParsed'
        })
    });

    if (!response.ok) {
        const error = await response.text();
        console.error('❌ Failed:', error);
        process.exit(1);
    }

    const data = await response.json();
    console.log('✅ WEBHOOK CREATED');
    console.log();
    console.log('Webhook ID:', data.webhookID);
    console.log();
    console.log('Add to Render environment:');
    console.log('  NEW_TOKEN_WEBHOOK_ID=' + data.webhookID);

    return data;
}

setup().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
