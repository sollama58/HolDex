const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const { getSolanaConnection } = require('../services/solana');
const config = require('../config/env');

const METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

// Optional API keys from environment
const SOLSCAN_API_KEY = process.env.SOLSCAN_API_KEY || null;
const HELIUS_API_KEY = config.HELIUS_API_KEY || null;

function removeNullBytes(str) {
    return str.split('\u0000')[0];
}

/**
 * Validate metadata - ensure it's not placeholder data
 */
function isValidMetadata(meta) {
    if (!meta) return false;
    const invalidNames = ['Unknown', 'New Discovery', '', null, undefined];
    const invalidSymbols = ['UNK', 'UNKNOWN', 'NEW', '', null, undefined];
    return !invalidNames.includes(meta.name) && !invalidSymbols.includes(meta.symbol);
}

async function fetchTokenMetadata(mintAddress) {
    console.log(`[Metaplex] Fetching metadata for ${mintAddress.slice(0,8)}...`);

    // 1. Primary: GeckoTerminal API (free, no key required)
    try {
        const geckoRes = await axios.get(
            `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${mintAddress}`,
            { timeout: 5000 }
        );
        if (geckoRes.data?.data?.attributes) {
            const attrs = geckoRes.data.data.attributes;
            const geckoMeta = {
                name: attrs.name || 'Unknown',
                symbol: attrs.symbol || 'UNK',
                image: attrs.image_url || (attrs.coingecko_coin_id ? `https://assets.coingecko.com/coins/images/${attrs.coingecko_coin_id}/small` : null),
                description: attrs.description || ''
            };
            console.log(`[Metaplex] GeckoTerminal raw response for ${mintAddress.slice(0,8)}: name=${attrs.name}, symbol=${attrs.symbol}`);
            if (isValidMetadata(geckoMeta)) {
                console.log(`[Metaplex] Found metadata via GeckoTerminal for ${mintAddress.slice(0,8)}`);
                return geckoMeta;
            } else {
                console.log(`[Metaplex] GeckoTerminal returned invalid metadata for ${mintAddress.slice(0,8)}: name=${geckoMeta.name}, symbol=${geckoMeta.symbol}`);
            }
        } else {
            console.log(`[Metaplex] GeckoTerminal returned no data for ${mintAddress.slice(0,8)}`);
        }
    } catch (e) {
        console.log(`[Metaplex] GeckoTerminal failed for ${mintAddress.slice(0,8)}: ${e.message}`);
    }

    // 2. Helius DAS API (Digital Asset Standard) - best for new tokens
    // Helius indexes on-chain metadata directly, so it has data for brand new tokens
    if (HELIUS_API_KEY) {
        try {
            const heliusRes = await axios.post(
                `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`,
                {
                    jsonrpc: '2.0',
                    id: 'helius-das',
                    method: 'getAsset',
                    params: { id: mintAddress }
                },
                { timeout: 5000 }
            );

            if (heliusRes.data?.result) {
                const asset = heliusRes.data.result;
                const content = asset.content || {};
                const metadata = content.metadata || {};
                const links = content.links || {};

                const heliusMeta = {
                    name: metadata.name || content.name || null,
                    symbol: metadata.symbol || content.symbol || null,
                    image: links.image || content.image || (content.json_uri ? null : null),
                    description: metadata.description || content.description || ''
                };

                // If we have name/symbol but no image, try fetching from json_uri
                if (heliusMeta.name && !heliusMeta.image && content.json_uri) {
                    try {
                        const jsonRes = await axios.get(content.json_uri, { timeout: 3000 });
                        heliusMeta.image = jsonRes.data?.image || null;
                        if (!heliusMeta.description && jsonRes.data?.description) {
                            heliusMeta.description = jsonRes.data.description;
                        }
                    } catch (_e) {
                        // Failed to fetch json_uri, continue without image
                    }
                }

                console.log(`[Metaplex] Helius DAS raw response for ${mintAddress.slice(0,8)}: name=${heliusMeta.name}, symbol=${heliusMeta.symbol}`);
                if (isValidMetadata(heliusMeta)) {
                    console.log(`[Metaplex] Found metadata via Helius DAS API for ${mintAddress.slice(0,8)}`);
                    return heliusMeta;
                } else {
                    console.log(`[Metaplex] Helius DAS returned invalid metadata for ${mintAddress.slice(0,8)}: name=${heliusMeta.name}, symbol=${heliusMeta.symbol}`);
                }
            } else {
                console.log(`[Metaplex] Helius DAS returned no result for ${mintAddress.slice(0,8)}`);
            }
        } catch (e) {
            console.log(`[Metaplex] Helius DAS failed for ${mintAddress.slice(0,8)}: ${e.message}`);
        }
    }

    // 3. Fallback: Solscan Pro API (if API key is set)
    if (SOLSCAN_API_KEY) {
        try {
            const solscanRes = await axios.get(
                `https://pro-api.solscan.io/v2.0/token/meta?address=${mintAddress}`,
                {
                    headers: {
                        'token': SOLSCAN_API_KEY
                    },
                    timeout: 5000
                }
            );
            if (solscanRes.data?.success && solscanRes.data?.data) {
                const d = solscanRes.data.data;
                const solscanMeta = {
                    name: d.name || d.tokenName || 'Unknown',
                    symbol: d.symbol || d.tokenSymbol || 'UNK',
                    image: d.icon || d.image || d.logoURI || null,
                    description: d.description || ''
                };
                if (isValidMetadata(solscanMeta)) {
                    console.log(`[Metaplex] Found metadata via Solscan Pro API for ${mintAddress.slice(0,8)}`);
                    return solscanMeta;
                }
            }
        } catch (e) {
            console.warn(`[Metaplex] Solscan Pro API failed for ${mintAddress.slice(0,8)}: ${e.message}`);
        }
    }

    // 4. Fallback: Public Solscan API (free, no key required)
    try {
        const solscanPublicRes = await axios.get(
            `https://api.solscan.io/token/meta?token=${mintAddress}`,
            { timeout: 5000 }
        );
        console.log(`[Metaplex] Solscan Public raw response for ${mintAddress.slice(0,8)}:`, JSON.stringify(solscanPublicRes.data).slice(0, 200));
        if (solscanPublicRes.data?.success !== false && solscanPublicRes.data) {
            const d = solscanPublicRes.data;
            const solscanPublicMeta = {
                name: d.name || d.tokenName || 'Unknown',
                symbol: d.symbol || d.tokenSymbol || 'UNK',
                image: d.icon || d.image || d.logoURI || null,
                description: d.description || ''
            };
            if (isValidMetadata(solscanPublicMeta)) {
                console.log(`[Metaplex] Found metadata via Solscan Public API for ${mintAddress.slice(0,8)}`);
                return solscanPublicMeta;
            } else {
                console.log(`[Metaplex] Solscan Public returned invalid metadata for ${mintAddress.slice(0,8)}: name=${solscanPublicMeta.name}, symbol=${solscanPublicMeta.symbol}`);
            }
        }
    } catch (e) {
        console.log(`[Metaplex] Solscan Public failed for ${mintAddress.slice(0,8)}: ${e.message}`);
    }

    // 5. Last resort: On-Chain Metaplex Parse
    try {
        const connection = getSolanaConnection();
        const mint = new PublicKey(mintAddress);
        const [pda] = PublicKey.findProgramAddressSync(
            [Buffer.from('metadata'), METADATA_PROGRAM_ID.toBuffer(), mint.toBuffer()],
            METADATA_PROGRAM_ID
        );

        const info = await connection.getAccountInfo(pda);
        if (info) {
            const data = info.data;
            // Metaplex Data Layout:
            // 0: key (1)
            // 1: update_auth (32)
            // 33: mint (32)
            // 65: name len (4) + name bytes
            let offset = 65;
            const nameLen = data.readUInt32LE(offset);
            offset += 4;
            const name = data.subarray(offset, offset + nameLen).toString('utf-8');
            offset += nameLen;

            const symbolLen = data.readUInt32LE(offset);
            offset += 4;
            const symbol = data.subarray(offset, offset + symbolLen).toString('utf-8');
            offset += symbolLen;

            const uriLen = data.readUInt32LE(offset);
            offset += 4;
            const uri = data.subarray(offset, offset + uriLen).toString('utf-8');

            const onChainMeta = {
                name: removeNullBytes(name),
                symbol: removeNullBytes(symbol),
                image: null,
                description: ''
            };

            // Fetch the JSON URI for image
            const cleanUri = removeNullBytes(uri);
            if (cleanUri) {
                try {
                    const jsonRes = await axios.get(cleanUri, { timeout: 3000 });
                    onChainMeta.image = jsonRes.data.image;
                    onChainMeta.description = jsonRes.data.description || '';
                } catch (_e) {
                    // Failed to fetch JSON URI - continue with what we have
                }
            }

            console.log(`[Metaplex] On-chain parsed for ${mintAddress.slice(0,8)}: name=${onChainMeta.name}, symbol=${onChainMeta.symbol}`);
            if (isValidMetadata(onChainMeta)) {
                console.log(`[Metaplex] Found metadata via on-chain parse for ${mintAddress.slice(0,8)}`);
                return onChainMeta;
            } else {
                console.log(`[Metaplex] On-chain returned invalid metadata for ${mintAddress.slice(0,8)}: name=${onChainMeta.name}, symbol=${onChainMeta.symbol}`);
            }
        } else {
            console.log(`[Metaplex] No on-chain metadata account found for ${mintAddress.slice(0,8)}`);
        }
    } catch (e) {
        console.warn(`[Metaplex] On-chain parse failed for ${mintAddress.slice(0,8)}: ${e.message}`);
    }

    console.warn(`[Metaplex] ALL SOURCES FAILED for ${mintAddress.slice(0,8)} - returning null`);
    return null; // Truly failed
}

module.exports = { fetchTokenMetadata };
