#!/usr/bin/env node
/**
 * Generate Node Key
 *
 * Generates a new Ed25519 keypair for a HolDex node.
 * The private key should be stored in NODE_PRIVATE_KEY env var.
 * The public key will be auto-registered when the node starts.
 *
 * Usage:
 *   node scripts/generate-node-key.js
 *
 * Output:
 *   - Private key (keep secret, add to .env)
 *   - Public key (auto-registered on startup)
 *   - Fingerprint (for identification)
 */

const nodeKeys = require('../src/utils/nodeKeys');

console.log('\n╔════════════════════════════════════════════════════════════════╗');
console.log('║          HolDex Node Key Generator (Ed25519)                  ║');
console.log('║   "Don\'t trust. Verify." - $asdfasdfa philosophy              ║');
console.log('╚════════════════════════════════════════════════════════════════╝\n');

const keys = nodeKeys.generateKeyPair();

console.log('🔑 NEW NODE KEYPAIR GENERATED\n');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('FINGERPRINT (for identification):');
console.log(`  ${keys.fingerprint}`);
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('\n📦 ENVIRONMENT VARIABLE (add to your .env or Render secrets):\n');
console.log(`NODE_PRIVATE_KEY=${keys.privateKey}`);
console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('\n📤 PUBLIC KEY (auto-registered on node startup):\n');
console.log(`${keys.publicKey}`);
console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('\n⚠️  SECURITY NOTES:');
console.log('  1. NEVER share the private key');
console.log('  2. Store it securely in environment variables only');
console.log('  3. The public key will be registered in the database automatically');
console.log('  4. Other nodes/users can verify your signatures using the public key');
console.log('\n✅ Add NODE_PRIVATE_KEY to your environment and restart the node.\n');
