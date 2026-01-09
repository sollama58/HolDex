#!/usr/bin/env node
/**
 * Update Production Webhook - Add Program Addresses
 */

const HELIUS_API_KEY = process.env.HELIUS_API_KEY || 'e7c9c7ca-f9c1-4515-ac41-ae664468506c';
const WEBHOOK_ID = '4a1ca70b-5932-4adf-b1f3-5e456b6cc913';

const PROGRAMS = [
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium V4
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'  // Pump.fun
];

async function updateWebhook() {
    console.log('Updating webhook with program addresses...');
    console.log('Webhook ID:', WEBHOOK_ID);
    console.log('Programs:', PROGRAMS);

    const response = await fetch(
        `https://api.helius.xyz/v0/webhooks/${WEBHOOK_ID}?api-key=${HELIUS_API_KEY}`,
        {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                webhookURL: 'https://holdex-api.onrender.com/webhook/new-tokens',
                transactionTypes: ['CREATE_POOL', 'TOKEN_MINT', 'SWAP'],
                accountAddresses: PROGRAMS,
                webhookType: 'enhanced'
            })
        }
    );

    if (!response.ok) {
        const error = await response.text();
        console.error('Failed:', error);
        process.exit(1);
    }

    const data = await response.json();
    console.log('Updated successfully:', JSON.stringify(data, null, 2));
    return data;
}

updateWebhook().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
