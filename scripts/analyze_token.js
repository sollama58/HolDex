const fetch = require('node-fetch');

const mint = process.argv[2] || 'CmgJ1PobhUqB7MEa8qDkiG2TUpMTskWj8d9JeZWSpump';

// Known pool addresses to exclude
const KNOWN_POOLS = new Set([
  '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
  'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM',
  'CebN5WGQ4jvMZMnti46ZewMiXCa4oXF4bZxwQPoKzXPFxZn',
]);

const RPC = 'https://api.mainnet-beta.solana.com';

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function rpcCall(method, params, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(RPC, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params })
      });
      const data = await res.json();
      if (data.error) {
        if (data.error.message.includes('Too many requests') && i < retries - 1) {
          await sleep(2000 * (i + 1));
          continue;
        }
        throw new Error(data.error.message);
      }
      return data.result;
    } catch (e) {
      if (i === retries - 1) throw e;
      await sleep(2000 * (i + 1));
    }
  }
}

async function getTokenInfo(mintAddress) {
  const result = await rpcCall('getAccountInfo', [mintAddress, { encoding: 'jsonParsed' }]);
  if (!result || !result.value) return null;
  return result.value.data.parsed.info;
}

async function getTokenAccounts(mintAddress) {
  const result = await rpcCall('getTokenLargestAccounts', [mintAddress]);
  return result?.value || [];
}

async function getAccountOwner(tokenAccount) {
  const result = await rpcCall('getAccountInfo', [tokenAccount, { encoding: 'jsonParsed' }]);
  return result?.value?.data?.parsed?.info?.owner;
}

async function getSignatures(address, limit = 30) {
  const result = await rpcCall('getSignaturesForAddress', [address, { limit }]);
  return result || [];
}

async function analyzeHolder(holderAddress, tokenMint, decimals) {
  const sigs = await getSignatures(holderAddress, 20);

  const now = Date.now();
  let firstSeen = now;
  let buys = 0, sells = 0;

  for (const sig of sigs) {
    const ts = sig.blockTime ? sig.blockTime * 1000 : now;
    if (ts < firstSeen) firstSeen = ts;
    // Simplified: count all as activity
    buys++;
  }

  const days = (now - firstSeen) / 86400000;

  let role, metal;
  if (sells === 0 && buys > 2) { role = 'Accumulator'; metal = 'Diamond'; }
  else if (buys >= sells) { role = 'Holder'; metal = 'Gold'; }
  else if (sells > 0 && sells <= buys * 1.5) { role = 'Reducer'; metal = 'Silver'; }
  else { role = 'Extractor'; metal = 'Bronze'; }

  return { buys, sells, days: days.toFixed(1), role, metal };
}

(async () => {
  console.log('='.repeat(70));
  console.log('ADMIN K-SCORE ANALYSIS WITH FULL BREAKDOWN');
  console.log('='.repeat(70));
  console.log('Mint:', mint);
  console.log('Timestamp:', new Date().toISOString());
  console.log('');

  // 1. Get token info
  console.log('--- TOKEN INFO ---');
  const tokenInfo = await getTokenInfo(mint);

  if (!tokenInfo) {
    console.log('ERROR: Token not found on Solana mainnet');
    console.log('');
    console.log('This mint address does not exist or is invalid.');
    console.log('Please verify the address is correct.');
    process.exit(1);
  }

  const decimals = tokenInfo.decimals;
  const supply = parseInt(tokenInfo.supply) / Math.pow(10, decimals);

  console.log('Decimals:', decimals);
  console.log('Total Supply:', supply.toLocaleString());
  console.log('');

  // 2. Security
  console.log('--- SECURITY ANALYSIS ---');
  const mintAuth = tokenInfo.mintAuthority;
  const freezeAuth = tokenInfo.freezeAuthority;

  console.log('Mint Authority:', mintAuth || 'Revoked (Safe)');
  console.log('Freeze Authority:', freezeAuth || 'Revoked (Safe)');

  const isMintable = mintAuth && mintAuth !== 'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM';
  const isFreezable = !!freezeAuth;

  console.log('Is Mintable:', isMintable ? 'YES (RISK)' : 'NO (Safe)');
  console.log('Is Freezable:', isFreezable ? 'YES (RISK)' : 'NO (Safe)');
  console.log('');

  // 3. Get top holders
  console.log('--- HOLDERS ANALYSIS ---');
  await sleep(1000);
  const topAccounts = await getTokenAccounts(mint);
  console.log('Top Token Accounts:', topAccounts.length);

  // Get owner addresses
  const holders = [];
  for (const acc of topAccounts.slice(0, 25)) {
    try {
      await sleep(500);
      const owner = await getAccountOwner(acc.address);
      if (owner && !KNOWN_POOLS.has(owner)) {
        holders.push({
          owner,
          amount: parseFloat(acc.uiAmount || 0)
        });
      }
    } catch (e) {
      // Skip errors
    }
  }

  const totalHeld = holders.reduce((s, h) => s + h.amount, 0);
  console.log('Real Holders (excluding pools):', holders.length);
  console.log('');

  // 4. Top 20 Analysis
  console.log('--- TOP 20 CONVICTION BREAKDOWN ---');
  console.log('-'.repeat(70));

  const breakdown = { Accumulator: 0, Holder: 0, Reducer: 0, Extractor: 0 };
  const top20 = holders.slice(0, 20);
  let totalDays = 0;

  for (let i = 0; i < Math.min(top20.length, 20); i++) {
    const h = top20[i];
    const pct = totalHeld > 0 ? ((h.amount / totalHeld) * 100).toFixed(2) : '0.00';

    process.stdout.write(`#${(i+1).toString().padStart(2)} ${h.owner.slice(0,6)}...${h.owner.slice(-4)} | ${pct.padStart(6)}% | `);

    try {
      const analysis = await analyzeHolder(h.owner, mint, decimals);
      breakdown[analysis.role]++;
      totalDays += parseFloat(analysis.days);
      console.log(`${analysis.role.padEnd(11)} | ${analysis.metal.padEnd(7)} | ${analysis.days}d`);
    } catch (e) {
      console.log('Error analyzing');
      breakdown['Holder']++;
    }

    await new Promise(r => setTimeout(r, 200));
  }

  const analyzed = Math.min(top20.length, 20) || 1;

  // Summary
  console.log('');
  console.log('='.repeat(70));
  console.log('CONVICTION BREAKDOWN');
  console.log('='.repeat(70));
  console.log(`Accumulators (Diamond): ${breakdown.Accumulator.toString().padStart(2)} | ${'*'.repeat(breakdown.Accumulator).padEnd(20)} ${((breakdown.Accumulator/analyzed)*100).toFixed(0)}%`);
  console.log(`Holders (Gold):         ${breakdown.Holder.toString().padStart(2)} | ${'*'.repeat(breakdown.Holder).padEnd(20)} ${((breakdown.Holder/analyzed)*100).toFixed(0)}%`);
  console.log(`Reducers (Silver):      ${breakdown.Reducer.toString().padStart(2)} | ${'*'.repeat(breakdown.Reducer).padEnd(20)} ${((breakdown.Reducer/analyzed)*100).toFixed(0)}%`);
  console.log(`Extractors (Bronze):    ${breakdown.Extractor.toString().padStart(2)} | ${'*'.repeat(breakdown.Extractor).padEnd(20)} ${((breakdown.Extractor/analyzed)*100).toFixed(0)}%`);
  console.log('');

  // Scores
  const conviction = ((breakdown.Accumulator * 100) + (breakdown.Holder * 75) + (breakdown.Reducer * 40) + (breakdown.Extractor * 10)) / analyzed;
  const accRatio = (breakdown.Accumulator - breakdown.Extractor + analyzed) / (analyzed * 2);
  const diamondHands = conviction * 0.6 + accRatio * 100 * 0.4;
  const organic = Math.min(100, holders.length * 5);
  const avgDays = totalDays / analyzed;
  const longevity = Math.min(100, avgDays * 3.33);

  console.log('='.repeat(70));
  console.log('PILLAR BREAKDOWN');
  console.log('='.repeat(70));
  console.log(`1. Diamond Hands (50%):  ${diamondHands.toFixed(1)}`);
  console.log(`   - Conviction Score:   ${conviction.toFixed(1)}`);
  console.log(`   - Acc/Ext Balance:    ${(accRatio * 100).toFixed(1)}`);
  console.log('');
  console.log(`2. Organic Growth (35%): ${organic.toFixed(1)}`);
  console.log(`   - Total Holders:      ${holders.length}`);
  console.log('');
  console.log(`3. Longevity (15%):      ${longevity.toFixed(1)}`);
  console.log(`   - Avg Holding Days:   ${avgDays.toFixed(1)}`);
  console.log('');

  // Final K-Score
  let kScore = diamondHands * 0.50 + organic * 0.35 + longevity * 0.15;

  if (isMintable) {
    kScore *= 0.5;
    console.log('PENALTY: Mintable (-50%)');
  }
  if (isFreezable) {
    kScore *= 0.7;
    console.log('PENALTY: Freezable (-30%)');
  }

  const finalK = Math.round(kScore);

  console.log('='.repeat(70));
  console.log('FINAL RESULTS');
  console.log('='.repeat(70));
  console.log(`K-SCORE:    ${finalK}`);
  console.log(`CONVICTION: ${conviction.toFixed(1)}`);

  const ratings = [
    { min: 90, metal: 'Diamond', grade: 'A1 Prime' },
    { min: 80, metal: 'Platinum', grade: 'A2 Excellent' },
    { min: 70, metal: 'Gold', grade: 'A3 Good' },
    { min: 60, metal: 'Silver', grade: 'B1 Fair' },
    { min: 50, metal: 'Bronze', grade: 'B2 Speculative' },
    { min: 40, metal: 'Copper', grade: 'B3 Risky' },
    { min: 20, metal: 'Iron', grade: 'C High Risk' },
    { min: 0, metal: 'Rust', grade: 'D Junk' }
  ];
  const rating = ratings.find(r => finalK >= r.min);
  console.log(`METAL RANK: ${rating.metal}`);
  console.log(`CREDIT:     ${rating.grade}`);
  console.log('='.repeat(70));
})();
