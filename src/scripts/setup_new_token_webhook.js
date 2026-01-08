#!/usr/bin/env node
/**
 * Setup New Token Discovery Webhook
 *
 * Configure Helius webhook pour recevoir CREATE_POOL et TOKEN_MINT events.
 * Remplace le WebSocket listener coûteux.
 *
 * Usage:
 *   node src/scripts/setup_new_token_webhook.js
 *   node src/scripts/setup_new_token_webhook.js --callback https://holdex.xyz/webhook/new-tokens
 *   node src/scripts/setup_new_token_webhook.js --list
 *   node src/scripts/setup_new_token_webhook.js --delete <webhookId>
 *
 * Environment:
 *   HELIUS_API_KEY - Required
 *   API_URL - Base URL for callback (default: http://localhost:3000)
 */

require('dotenv').config();

const newTokenWebhook = require('../services/newTokenWebhook');
const logger = require('../services/logger');

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const API_URL = process.env.API_URL || 'http://localhost:3000';

async function main() {
    const args = process.argv.slice(2);

    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  NEW TOKEN WEBHOOK SETUP');
    console.log('  Helius → HolDex (CREATE_POOL, TOKEN_MINT)');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log();

    if (!HELIUS_API_KEY) {
        console.error('❌ HELIUS_API_KEY not set in environment');
        process.exit(1);
    }

    console.log(`API Key: ${HELIUS_API_KEY.slice(0, 4)}...${HELIUS_API_KEY.slice(-4)}`);
    console.log();

    // Parse arguments
    if (args.includes('--list')) {
        await listWebhooks();
        return;
    }

    if (args.includes('--delete')) {
        const idx = args.indexOf('--delete');
        const webhookId = args[idx + 1];
        if (!webhookId) {
            console.error('❌ Please provide webhook ID: --delete <webhookId>');
            process.exit(1);
        }
        await deleteWebhook(webhookId);
        return;
    }

    // Get callback URL
    let callbackUrl = `${API_URL}/webhook/new-tokens`;
    const callbackIdx = args.indexOf('--callback');
    if (callbackIdx !== -1 && args[callbackIdx + 1]) {
        callbackUrl = args[callbackIdx + 1];
    }

    await createWebhook(callbackUrl);
}

async function listWebhooks() {
    console.log('📋 Listing existing webhooks...\n');

    try {
        const webhooks = await newTokenWebhook.listWebhooks();

        if (webhooks.length === 0) {
            console.log('No webhooks found.');
            return;
        }

        webhooks.forEach((wh, i) => {
            console.log(`[${i + 1}] ID: ${wh.webhookID}`);
            console.log(`    URL: ${wh.webhookURL}`);
            console.log(`    Types: ${wh.transactionTypes?.join(', ') || 'N/A'}`);
            console.log(`    Addresses: ${wh.accountAddresses?.length || 0}`);
            console.log();
        });
    } catch (error) {
        console.error(`❌ Failed to list webhooks: ${error.message}`);
        process.exit(1);
    }
}

async function deleteWebhook(webhookId) {
    console.log(`🗑️  Deleting webhook: ${webhookId}\n`);

    try {
        await newTokenWebhook.deleteWebhook(webhookId);
        console.log('✅ Webhook deleted successfully');
    } catch (error) {
        console.error(`❌ Failed to delete webhook: ${error.message}`);
        process.exit(1);
    }
}

async function createWebhook(callbackUrl) {
    console.log('🚀 Creating new token discovery webhook...\n');
    console.log(`Callback URL: ${callbackUrl}`);
    console.log(`Programs: Raydium V4, Pump.fun`);
    console.log(`Transaction Types: ${newTokenWebhook.NEW_TOKEN_TX_TYPES.join(', ')}`);
    console.log();

    try {
        const result = await newTokenWebhook.createNewTokenWebhook(callbackUrl);

        console.log('═══════════════════════════════════════════════════════════════');
        console.log('✅ WEBHOOK CREATED SUCCESSFULLY');
        console.log('═══════════════════════════════════════════════════════════════');
        console.log();
        console.log(`Webhook ID: ${result.webhookID}`);
        console.log();
        console.log('Next steps:');
        console.log('1. Ensure your server is running and accessible at:');
        console.log(`   ${callbackUrl}`);
        console.log();
        console.log('2. Set WEBHOOK_SECRET in your .env for security');
        console.log();
        console.log('3. Disable the old WebSocket listener in listener_worker.js');
        console.log('   (set USE_WEBSOCKET_LISTENER=false or just don\'t start listener worker)');
        console.log();
        console.log('4. Monitor incoming events at:');
        console.log(`   GET ${callbackUrl}/stats`);
        console.log();

        // Save webhook ID for reference
        console.log('Save this webhook ID in your .env:');
        console.log(`NEW_TOKEN_WEBHOOK_ID=${result.webhookID}`);

    } catch (error) {
        console.error(`❌ Failed to create webhook: ${error.message}`);
        process.exit(1);
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
