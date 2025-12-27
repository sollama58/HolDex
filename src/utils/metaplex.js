const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const config = require('../config/env');
const { getSolanaConnection } = require('../services/solana'); // Use centralized connection

const METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

function removeNullBytes(str) {
    return str.split('\u0000')[0];
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
            });

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
            // console.warn(`Helius DAS failed, falling back to on-chain: ${e.message}`);
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
            
            metadata.name = removeNullBytes(name);
            metadata.symbol = removeNullBytes(symbol);
            
            // Fetch the JSON URI for image
            const cleanUri = removeNullBytes(uri);
            if (cleanUri) {
                try {
                    const jsonRes = await axios.get(cleanUri, { timeout: 3000 });
                    metadata.image = jsonRes.data.image;
                    metadata.description = jsonRes.data.description;
                } catch (e) { 
                    // console.warn('Failed to fetch JSON URI'); 
                }
            }
            return metadata;
        }
    } catch (e) {
        // console.warn(`All metadata fetches failed for ${mintAddress}: ${e.message}`);
    }

    return null; // Truly failed
}

module.exports = { fetchTokenMetadata };
