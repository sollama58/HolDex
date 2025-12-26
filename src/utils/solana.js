const { PublicKey } = require('@solana/web3.js');

function isValidPubkey(str) {
    if (!str) return false;
    try {
        new PublicKey(str);
        return true;
    } catch (e) {
        return false;
    }
}

module.exports = {
    isValidPubkey
};
