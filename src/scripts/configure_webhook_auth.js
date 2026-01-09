#!/usr/bin/env node
/**
 * Configure Webhook Authentication
 * Sets authHeader on Helius webhook
 */

const HELIUS_API_KEY = process.env.HELIUS_API_KEY || 'e7c9c7ca-f9c1-4515-ac41-ae664468506c';
const WEBHOOK_ID = '4a1ca70b-5932-4adf-b1f3-5e456b6cc913';
const AUTH_TOKEN = 'holdex-webhook-618711136fd7196851c94b25c3b7e9b9';

async function configureAuth() {
    console.log('Configuring webhook authentication...');
    console.log('Webhook ID:', WEBHOOK_ID);
    console.log('Auth Token:', AUTH_TOKEN.slice(0, 20) + '...');

    const response = await fetch(
        `https://api.helius.xyz/v0/webhooks/${WEBHOOK_ID}?api-key=${HELIUS_API_KEY}`,
        {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                webhookURL: 'https://holdex-api.onrender.com/webhook/new-tokens',
                transactionTypes: ['CREATE_POOL', 'TOKEN_MINT', 'SWAP'],
                accountAddresses: [
                    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
                ],
                webhookType: 'enhanced',
                authHeader: AUTH_TOKEN
            })
        }
    );

    if (!response.ok) {
        const error = await response.text();
        console.error('Failed:', error);
        process.exit(1);
    }

    const data = await response.json();
    console.log('Webhook updated:', JSON.stringify(data, null, 2));
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('IMPORTANT: Set this in Render environment:');
    console.log('  WEBHOOK_SECRET=' + AUTH_TOKEN);
    console.log('═══════════════════════════════════════════════════════════════');
    return data;
}

configureAuth().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
