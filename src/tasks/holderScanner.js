/**
 * Holder Scanner Task
 * Rotates through top tokens and fetches their largest accounts
 * to populate the 'token_holders' table.
 */
const { Connection, PublicKey } = require('@solana/web3.js');
const config = require('../config/env');

// Use a shared connection
const connection = new Connection(config.SOLANA_RPC_URL, 'confirmed');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function scanHolders(deps) {
    const { db } = deps;
    
    // 1. Get Top 20 Active Tokens (by Volume) to scan
    // We only scan high activity tokens to save RPC credits
    const tokens = await db.all('SELECT mint FROM tokens ORDER BY volume24h DESC LIMIT 20');
    
    if (tokens.length === 0) return;

    console.log(`🔍 [HolderScanner] Starting scan for ${tokens.length} top tokens...`);

    for (const t of tokens) {
        try {
            const mintPubkey = new PublicKey(t.mint);
            
            // fetch 20 largest accounts for this mint
            const largestAccounts = await connection.getTokenLargestAccounts(mintPubkey);
            
            if (largestAccounts.value && largestAccounts.value.length > 0) {
                
                // Clear old holders for this mint to keep data fresh
                await db.run('DELETE FROM token_holders WHERE mint = $1', [t.mint]);

                // Insert new holders
                let rank = 1;
                for (const account of largestAccounts.value) {
                    // Note: getTokenLargestAccounts returns Account Address, not Owner Address.
                    // To get the actual Owner (User Wallet), we technically need to parse the account info.
                    // However, for speed/RPC optimization, we often just use the account address or do a quick lookup.
                    // For a full "User" check, we'd need getParsedAccountInfo, but that's heavy.
                    // Here we store the Token Account address.
                    // *Improvement*: To match user wallets, the frontend checking logic might need to check if 
                    // a user OWNS one of these accounts. 
                    // OR: We use getProgramAccounts filters (Heavy).
                    
                    // For this implementation, we will assume we want to store the wallet address.
                    // We'll do a quick fetch for the owner of these top accounts.
                    
                    try {
                        const accInfo = await connection.getParsedAccountInfo(account.address);
                        const holderOwner = accInfo.value?.data?.parsed?.info?.owner;

                        if (holderOwner) {
                            await db.run(`
                                INSERT INTO token_holders (mint, holderPubkey, balance, rank, updatedAt)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (mint, holderPubkey) DO UPDATE SET
                                balance = EXCLUDED.balance,
                                rank = EXCLUDED.rank,
                                updatedAt = EXCLUDED.updatedAt
                            `, [
                                t.mint,
                                holderOwner, // The actual user wallet
                                account.amount, // Raw amount
                                rank,
                                Date.now()
                            ]);
                            rank++;
                        }
                    } catch (innerErr) {
                        // Skip account if fetch fails
                    }
                    
                    // Rate limit protection inside the loop
                    await sleep(200); 
                }
            }
        } catch (e) {
            console.error(`Error scanning mint ${t.mint}:`, e.message);
        }
        
        // Pause between tokens
        await sleep(1000);
    }
    
    console.log(`✅ [HolderScanner] Cycle complete.`);
}

function start(deps) {
    // Run immediately on start, then interval
    setTimeout(() => scanHolders(deps), 10000);
    setInterval(() => scanHolders(deps), config.HOLDER_SCAN_INTERVAL || 300000); // Default 5m
}

module.exports = { start };
