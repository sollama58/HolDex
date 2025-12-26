const { Connection, PublicKey } = require('@solana/web3.js');
const axios = require('axios');
const config = require('../config/env');

// Metaplex Token Metadata Program
const METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

async function fetchTokenMetadata(mintAddress) {
    try {
        const connection = new Connection(config.SOLANA_RPC_URL, 'confirmed');
        const mint = new PublicKey(mintAddress);

        // Find Metadata PDA
        const [pda] = PublicKey.findProgramAddressSync(
            [
                Buffer.from('metadata'),
                METADATA_PROGRAM_ID.toBuffer(),
                mint.toBuffer()
            ],
            METADATA_PROGRAM_ID
        );

        const accountInfo = await connection.getAccountInfo(pda);
        if (!accountInfo) return null;

        // Decode Metadata (Manual Buffer Parsing to avoid heavy dependencies)
        // Layout: key(1) + updateAuth(32) + mint(32) + data(...)
        // Data: name (4 + len), symbol (4 + len), uri (4 + len)
        
        const data = accountInfo.data;
        let offset = 1 + 32 + 32; // Skip Key, Auth, Mint
        
        const readString = () => {
            const len = data.readUInt32LE(offset);
            offset += 4;
            const str = data.toString('utf8', offset, offset + len).replace(/\0/g, '');
            offset += len;
            return str;
        };

        const name = readString();
        const symbol = readString();
        const uri = readString();

        // Fetch JSON from URI (Arweave/IPFS) for Image
        let image = null;
        let description = null;
        
        if (uri && uri.startsWith('http')) {
            try {
                const jsonRes = await axios.get(uri, { timeout: 2000 });
                image = jsonRes.data.image;
                description = jsonRes.data.description;
            } catch (e) { /* uri fetch failed */ }
        }

        return { name, symbol, image, description };

    } catch (e) {
        console.error("Metaplex Fetch Error:", e.message);
        return null;
    }
}

module.exports = { fetchTokenMetadata };
