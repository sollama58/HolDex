const SUPPLY = 1_000_000_000; // 1B tokens

console.log('=== ÉLIGIBILITÉ: Minimum Holdings ===\n');

const thresholds = [
    { pct: 0.01, label: '0.01%' },
    { pct: 0.001, label: '0.001%' },
    { pct: 0.0001, label: '0.0001%' },
    { pct: 0.00001, label: '0.00001%' }
];

console.log('Threshold | Tokens Required | ~USD (@$0.001/token)');
console.log('----------|-----------------|---------------------');
thresholds.forEach(t => {
    const tokens = SUPPLY * (t.pct / 100);
    const usd = tokens * 0.001;
    console.log(`  ${t.label.padEnd(7)} |  ${tokens.toLocaleString().padStart(12)}  |  $${usd.toLocaleString()}`);
});

console.log(`

=== COMPARAISON FORMULES DE PRICING ===
`);

const kScores = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10];
const metals = ['Diamond', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze', 'Copper', 'Iron', 'Iron', 'Rust'];

console.log('K-Score | Metal    | 100/K | Tiered | Sqrt  | Recommandé');
console.log('--------|----------|-------|--------|-------|------------');

kScores.forEach((k, i) => {
    // Formula 1: Linear 100/K
    const linear = (100 / Math.max(k, 10)).toFixed(1);

    // Formula 2: Tiered (brackets)
    let tiered;
    if (k >= 90) tiered = 1;
    else if (k >= 80) tiered = 1.25;
    else if (k >= 70) tiered = 1.5;
    else if (k >= 60) tiered = 2;
    else if (k >= 50) tiered = 2.5;
    else if (k >= 40) tiered = 3;
    else if (k >= 20) tiered = 5;
    else tiered = 10;

    // Formula 3: Sqrt
    const sqrt = Math.sqrt(100 / Math.max(k, 10)).toFixed(2);

    // Recommended: 100/K but cleaner (1 decimal)
    const recommended = Math.max(1, Math.round(100 / k * 10) / 10);

    console.log(`   ${k.toString().padStart(3)}  | ${metals[i].padEnd(8)} | ${linear.padStart(5)} | ${tiered.toString().padStart(6)} | ${sqrt.padStart(5)} | ${recommended.toString().padStart(5)}`);
});

console.log(`

=== VISUALISATION: Coût par K-Score (100/K) ===

K=100 💎 █           1.0 token/call
K=90  💎 █           1.1 token/call
K=80  💠 █▒          1.3 token/call
K=70  🥇 █▒          1.4 token/call
K=60  🥈 ██          1.7 token/call
K=50  🥉 ██          2.0 token/call
K=40  🟤 ███         2.5 token/call
K=30  ⚫ ███▒        3.3 token/call
K=20  ⚫ █████       5.0 token/call
K=10  🔩 ██████████  10.0 token/call

=== FORMULE RECOMMANDÉE ===

  cost = ceil(100 / K)

  Simple | Élégant | Auto-explicatif
  "Ton K-Score EST ton discount"

=== CONVICTION MODIFIER (optionnel) ===

Sur $ASDFASDFA spécifiquement:
  💎 Accumulator: cost × 0.8 (20% discount)
  🥇 Holder:      cost × 1.0 (no change)
  🥈 Reducer:     cost × 1.2 (20% penalty)
  🔩 Extractor:   cost × 1.5 (50% penalty)

Exemple K=50, Extractor:
  Base: 100/50 = 2 tokens
  Avec modifier: 2 × 1.5 = 3 tokens/call

=== MINIMUM HOLDING RECOMMANDÉ ===

0.001% de 1B = 10,000 tokens ≈ $10 @ $0.001/token

C'est assez bas pour être accessible, assez haut pour filter les bots.
`);
