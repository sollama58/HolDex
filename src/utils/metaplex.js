const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const { getSolanaConnection } = require('../services/solana'); // Use centralized connection

const METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

// Optional API keys from environment
const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY || null;
const SOLSCAN_API_KEY = process.env.SOLSCAN_API_KEY || null;

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
    let metadata = {
        name: 'Unknown',
        symbol: 'UNK',
        image: null,
        description: ''
    };

    // 1. Try Helius DAS API (Primary - Robust & Fast)
    if (config.HELIUS_API_KEY) {
        try {
            const url = `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
            const response = await axios.post(url, {
                jsonrpc: '2.0',
                id: 'holdex-meta',
                method: 'getAsset',
                params: { id: mintAddress }
            }, { timeout: 5000 });

            const result = response.data.result;
            if (result && result.content) {
                const c = result.content;
                const m = result.content.metadata;

                const files = c.files || [];
                // Prioritize GIF -> Helius CDN -> First File -> JSON URI
                const gifFile = files.find(f => (f.mimeType === 'image/gif' || f.mime === 'image/gif'));
                let image = gifFile?.uri || c.links?.image || files[0]?.uri || c.json_uri;

                if (!image && c.json_uri) {
                    try {
                        const jsonRes = await axios.get(c.json_uri, { timeout: 2000 });
                        image = jsonRes.data.image;
                    } catch (_e) { /* ignore */ }
                }

                const heliusMeta = {
                    name: m?.name || c.json_uri?.name || 'Unknown',
                    symbol: m?.symbol || c.json_uri?.symbol || 'UNK',
                    image: image || null,
                    description: m?.description || c.json_uri?.description || ''
                };

                if (isValidMetadata(heliusMeta)) {
                    return heliusMeta;
                }
                // If Helius returned placeholder data, continue to fallbacks
                console.log(`[Metaplex] Helius returned placeholder for ${mintAddress.slice(0,8)}, trying fallbacks...`);
            }
        } catch (e) {
            console.warn(`[Metaplex] Helius DAS failed for ${mintAddress.slice(0,8)}: ${e.message}`);
        }
    }

    // 2. Fallback: Manual On-Chain Metaplex Parse (Reliable)
    try {
        // FIX: Use the shared connection which has the correct RPC_URL
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

            if (isValidMetadata(onChainMeta)) {
                return onChainMeta;
            }
            // If on-chain returned placeholder data, continue to other fallbacks
            console.log(`[Metaplex] On-chain returned placeholder for ${mintAddress.slice(0,8)}, trying other fallbacks...`);
        }
    } catch (e) {
        console.warn(`[Metaplex] On-chain parse failed for ${mintAddress.slice(0,8)}: ${e.message}`);
    }

    // 3. Fallback: Pump.fun API (for Pump.fun tokens)
    try {
        const pumpRes = await axios.get(`https://frontend-api.pump.fun/coins/${mintAddress}`, { timeout: 3000 });
        if (pumpRes.data && pumpRes.data.name) {
            const pumpMeta = {
                name: pumpRes.data.name || 'Unknown',
                symbol: pumpRes.data.symbol || 'UNK',
                image: pumpRes.data.image_uri || pumpRes.data.metadata?.image || null,
                description: pumpRes.data.description || ''
            };
            if (isValidMetadata(pumpMeta)) {
                console.log(`[Metaplex] Found metadata via Pump.fun API for ${mintAddress.slice(0,8)}`);
                return pumpMeta;
            }
        }
    } catch (_e) {
        // Not a Pump.fun token or API failed - that's fine
    }

    // 4. Fallback: Birdeye API (if API key is set)
    if (BIRDEYE_API_KEY) {
        try {
            const birdeyeRes = await axios.get(
                `https://public-api.birdeye.so/defi/v3/token/meta-data/single?address=${mintAddress}`,
                {
                    headers: {
                        'X-API-KEY': BIRDEYE_API_KEY,
                        'x-chain': 'solana'
                    },
                    timeout: 5000
                }
            );
            if (birdeyeRes.data?.success && birdeyeRes.data?.data) {
                const d = birdeyeRes.data.data;
                const birdeyeMeta = {
                    name: d.name || 'Unknown',
                    symbol: d.symbol || 'UNK',
                    image: d.logoURI || d.logo_uri || d.image || null,
                    description: d.description || ''
                };
                if (isValidMetadata(birdeyeMeta)) {
                    console.log(`[Metaplex] Found metadata via Birdeye API for ${mintAddress.slice(0,8)}`);
                    return birdeyeMeta;
                }
            }
        } catch (e) {
            console.warn(`[Metaplex] Birdeye API failed for ${mintAddress.slice(0,8)}: ${e.message}`);
        }
    }

    // 5. Fallback: Solscan Pro API (if API key is set)
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
                    console.log(`[Metaplex] Found metadata via Solscan API for ${mintAddress.slice(0,8)}`);
                    return solscanMeta;
                }
            }
        } catch (e) {
            console.warn(`[Metaplex] Solscan API failed for ${mintAddress.slice(0,8)}: ${e.message}`);
        }
    }

    // 6. Fallback: Jupiter Token List API (public, no key required)
    try {
        const jupRes = await axios.get(
            `https://tokens.jup.ag/token/${mintAddress}`,
            { timeout: 5000 }
        );
        if (jupRes.data && jupRes.data.name) {
            const jupMeta = {
                name: jupRes.data.name || 'Unknown',
                symbol: jupRes.data.symbol || 'UNK',
                image: jupRes.data.logoURI || jupRes.data.logo_uri || null,
                description: jupRes.data.description || ''
            };
            if (isValidMetadata(jupMeta)) {
                console.log(`[Metaplex] Found metadata via Jupiter Token API for ${mintAddress.slice(0,8)}`);
                return jupMeta;
            }
        }
    } catch (_e) {
        // Jupiter token list API failed - that's fine
    }

    // 7. Last resort: GeckoTerminal API (free, no key)
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
                image: attrs.image_url || attrs.coingecko_coin_id ? `https://assets.coingecko.com/coins/images/${attrs.coingecko_coin_id}/small` : null,
                description: attrs.description || ''
            };
            if (isValidMetadata(geckoMeta)) {
                console.log(`[Metaplex] Found metadata via GeckoTerminal for ${mintAddress.slice(0,8)}`);
                return geckoMeta;
            }
        }
    } catch (_e) {
        // GeckoTerminal failed - that's fine
    }

    console.warn(`[Metaplex] All metadata fetches failed for ${mintAddress.slice(0,8)}`);
    return null; // Truly failed
}

module.exports = { fetchTokenMetadata };
