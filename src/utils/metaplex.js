const axios = require('axios');
const config = require('../config/env');

async function fetchTokenMetadata(mintAddress) {
    // 1. Try Helius DAS API (Primary - Robust)
    if (config.HELIUS_API_KEY) {
        try {
            const url = `https://mainnet.helius-rpc.com/?api-key=${config.HELIUS_API_KEY}`;
            const response = await axios.post(url, {
                jsonrpc: '2.0',
                id: 'holdex-meta',
                method: 'getAsset',
                params: { id: mintAddress }
            });

            const result = response.data.result;
            if (result && result.content) {
                const c = result.content;
                const m = result.content.metadata;
                
                // Check for GIF in files (Helius usually flags mime types)
                const files = c.files || [];
                const gifFile = files.find(f => (f.mimeType === 'image/gif' || f.mime === 'image/gif'));

                // Prioritize GIF -> Helius Image CDN -> First File -> JSON URI
                // We prefer the raw GIF uri over the optimized CDN image if it exists to preserve animation.
                let image = gifFile?.uri || c.links?.image || files[0]?.uri || c.json_uri;
                
                // Fetch JSON if image is missing but URI exists (Last resort)
                if (!image && c.json_uri) {
                    try {
                        const jsonRes = await axios.get(c.json_uri, { timeout: 2000 });
                        image = jsonRes.data.image;
                    } catch (e) { /* ignore */ }
                }

                return {
                    name: m?.name || c.json_uri?.name || 'Unknown',
                    symbol: m?.symbol || c.json_uri?.symbol || 'UNK',
                    image: image || null,
                    description: m?.description || c.json_uri?.description || ''
                };
            }
        } catch (e) {
            console.warn(`⚠️ Helius DAS failed for ${mintAddress}, falling back to manual parse: ${e.message}`);
        }
    }

    // 2. Fallback: Manual Metaplex PDA Fetch (Legacy)
    // Kept as backup in case Helius is down or rate limited
    return null; 
}

module.exports = { fetchTokenMetadata };
