/**
 * HARMONY MODULE - Tests de Validation Mathématique
 *
 * Exécuter: node src/shared/harmony.test.js
 */

'use strict';

const harmony = require('./harmony');

// ═══════════════════════════════════════════════════════════════
// TEST HELPERS
// ═══════════════════════════════════════════════════════════════

let passed = 0;
let failed = 0;

function test(name, fn) {
    try {
        fn();
        console.log(`  ✓ ${name}`);
        passed++;
    } catch (e) {
        console.log(`  ✗ ${name}`);
        console.log(`    Error: ${e.message}`);
        failed++;
    }
}

function assertEqual(actual, expected, msg = '') {
    if (actual !== expected) {
        throw new Error(`${msg} Expected ${expected}, got ${actual}`);
    }
}

function assertApprox(actual, expected, tolerance = 0.01, msg = '') {
    if (Math.abs(actual - expected) > tolerance) {
        throw new Error(`${msg} Expected ~${expected}, got ${actual} (tolerance: ${tolerance})`);
    }
}

function assertTrue(condition, msg = '') {
    if (!condition) {
        throw new Error(msg || 'Condition was false');
    }
}

// ═══════════════════════════════════════════════════════════════
// TESTS: CONSTANTES
// ═══════════════════════════════════════════════════════════════

console.log('\n=== CONSTANTES ===\n');

test('PHI = 1.618...', () => {
    assertApprox(harmony.PHI, 1.618033988749895, 0.0000001);
});

test('RATIOS sum to ~1', () => {
    const sum = harmony.RATIOS.BURN + harmony.RATIOS.REWARDS + harmony.RATIOS.TREASURY;
    assertApprox(sum, 1.0, 0.001);
});

test('BURN ratio = 1/φ² = 38.2%', () => {
    assertApprox(harmony.RATIOS.BURN, 0.382, 0.001);
});

test('REWARDS ratio = 1/φ² = 38.2%', () => {
    assertApprox(harmony.RATIOS.REWARDS, 0.382, 0.001);
});

test('TREASURY ratio = 1/φ³ = 23.6%', () => {
    assertApprox(harmony.RATIOS.TREASURY, 0.236, 0.001);
});

test('MULTIPLIERS.BURN = φ', () => {
    assertApprox(harmony.MULTIPLIERS.BURN, harmony.PHI, 0.0001);
});

test('MULTIPLIERS.BUILD = φ²', () => {
    assertApprox(harmony.MULTIPLIERS.BUILD, harmony.PHI ** 2, 0.0001);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: E-SCORE
// ═══════════════════════════════════════════════════════════════

console.log('\n=== E-SCORE ===\n');

test('E-Score with no contributions = 0', () => {
    const result = harmony.calculateEScore({});
    assertEqual(result.score, 0);
});

test('E-Score with only holdings', () => {
    const result = harmony.calculateEScore({ holdings: 1000 });
    assertTrue(result.score > 0, 'Score should be > 0');
    assertEqual(result.activeDimensions, 1);
});

test('E-Score logarithmic: 1M tokens ≠ 1000x score of 1K tokens', () => {
    const score1K = harmony.calculateEScore({ holdings: 1000 }).score;
    const score1M = harmony.calculateEScore({ holdings: 1000000 }).score;

    // 1M = 1000x more tokens, but score should be much less than 1000x
    const ratio = score1M / score1K;
    assertTrue(ratio < 10, `Ratio should be < 10, got ${ratio}`);
});

test('E-Score burn multiplier > hold multiplier', () => {
    const holdScore = harmony.calculateEScore({ holdings: 1000 }).score;
    const burnScore = harmony.calculateEScore({ burned: 1000 }).score;

    assertTrue(burnScore > holdScore, 'Burn score should be > hold score');
    assertApprox(burnScore / holdScore, harmony.PHI, 0.1);
});

test('E-Score diversity bonus works', () => {
    // 1 dimension
    const score1Dim = harmony.calculateEScore({ holdings: 1000 });

    // 2 dimensions (same total "value")
    const score2Dim = harmony.calculateEScore({ holdings: 500, burned: 500 });

    assertEqual(score1Dim.diversityBonus, 1.0);
    assertEqual(score2Dim.diversityBonus, 1.1);
});

test('E-Score geometric mean penalizes imbalance', () => {
    // All in one basket
    const concentrated = harmony.calculateEScore({
        holdings: 10000,
        burned: 0,
        apiCalls30d: 0
    });

    // Spread across dimensions
    const diversified = harmony.calculateEScore({
        holdings: 3333,
        burned: 3333,
        apiCalls30d: 3333
    });

    // Diversified should score higher due to geometric mean + diversity bonus
    assertTrue(diversified.score > concentrated.score,
        `Diversified (${diversified.score}) should beat concentrated (${concentrated.score})`);
});

test('E-Score example: Small User Active', () => {
    const result = harmony.calculateEScore({
        holdings: 100,
        burned: 0,
        apiCalls30d: 500,
        referralsActive: 2,
        daysActive: 60
    });

    // Should have 4 active dimensions
    assertEqual(result.activeDimensions, 4);
    assertTrue(result.score > 0 && result.score < 10);
});

test('E-Score example: Passive Whale', () => {
    const result = harmony.calculateEScore({
        holdings: 1000000,
        daysActive: 180
    });

    // Only 2 dimensions
    assertEqual(result.activeDimensions, 2);
    // Score should be reasonable, not astronomical
    assertTrue(result.score < 20, `Whale score ${result.score} should be < 20`);
});

test('E-Score example: Active Dev', () => {
    const result = harmony.calculateEScore({
        holdings: 10000,
        burned: 5000,
        apiCalls30d: 50000,
        appsLive: 3,
        referralsActive: 50,
        daysActive: 120
    });

    // 6 dimensions
    assertEqual(result.activeDimensions, 6);
    // High score due to diversity
    assertTrue(result.score > 5, `Dev score ${result.score} should be > 5`);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: EFFICIENCY FLOOR
// ═══════════════════════════════════════════════════════════════

console.log('\n=== EFFICIENCY FLOOR ===\n');

test('Minimum fee calculation', () => {
    // Cost 10, treasury ratio 23.6%, safety 1.2
    // minFee = (10 / 0.236) * 1.2 = 50.85 → 51
    const minFee = harmony.calculateMinimumFee(10);
    assertEqual(minFee, 51);
});

test('Minimum fee for zero cost = 0', () => {
    const minFee = harmony.calculateMinimumFee(0);
    assertEqual(minFee, 0);
});

test('Max discount when baseFee > minFee', () => {
    // Cost 2, minFee ~10, baseFee 100
    // maxDiscount = 1 - (10/100) = 90%
    const maxDiscount = harmony.calculateMaxDiscount(100, 2);
    assertTrue(maxDiscount > 0.85 && maxDiscount < 0.95);
});

test('Max discount = 0 when baseFee <= minFee', () => {
    // Cost 50, minFee ~254, baseFee 100
    const maxDiscount = harmony.calculateMaxDiscount(100, 50);
    assertEqual(maxDiscount, 0);
});

test('Max discount capped at 95%', () => {
    // Very low cost, high base fee
    const maxDiscount = harmony.calculateMaxDiscount(10000, 0.1);
    assertTrue(maxDiscount <= 0.95);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: THEORETICAL DISCOUNT
// ═══════════════════════════════════════════════════════════════

console.log('\n=== THEORETICAL DISCOUNT ===\n');

test('Theoretical discount at E-Score 0 = 0%', () => {
    const discount = harmony.calculateTheoreticalDiscount(0);
    assertEqual(discount, 0);
});

test('Theoretical discount at E-Score 5 ≈ 25%', () => {
    const discount = harmony.calculateTheoreticalDiscount(5);
    assertApprox(discount, 0.25, 0.05);
});

test('Theoretical discount at E-Score 10 ≈ 45%', () => {
    const discount = harmony.calculateTheoreticalDiscount(10);
    assertApprox(discount, 0.45, 0.05);
});

test('Theoretical discount at E-Score 50 ≈ 86%', () => {
    const discount = harmony.calculateTheoreticalDiscount(50);
    assertApprox(discount, 0.86, 0.05);
});

test('Theoretical discount asymptotes to 90%', () => {
    const discount = harmony.calculateTheoreticalDiscount(1000);
    assertApprox(discount, 0.90, 0.01);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: FINAL FEE CALCULATION
// ═══════════════════════════════════════════════════════════════

console.log('\n=== FINAL FEE ===\n');

test('Final fee respects efficiency floor', () => {
    // High E-Score (wants 86% discount), but high cost limits it
    const result = harmony.calculateFinalFee(50, 100, 15);

    // minFee = ceil((15 / 0.236) * 1.2) = 77
    // maxDiscount = 1 - (77/100) = 23%
    // effectiveDiscount = min(86%, 23%) = 23%
    // finalFee = 100 * (1 - 0.23) = 77

    assertTrue(result.finalFee >= 77, `Final fee ${result.finalFee} should be >= 77`);
    assertTrue(result.discounts.limited, 'Discount should be limited');
});

test('Final fee calculation - low cost operation', () => {
    const result = harmony.calculateFinalFee(10, 100, 2);

    // E-Score 10 → ~45% theoretical discount
    // Cost 2 → minFee ~10 → maxDiscount ~90%
    // Effective = 45%

    assertApprox(result.finalFee, 55, 5);
    assertTrue(result.efficiency.isViable);
    assertTrue(result.efficiency.profit > 0);
});

test('Final fee is always viable', () => {
    // Random test cases
    const testCases = [
        { eScore: 0, baseFee: 100, cost: 10 },
        { eScore: 50, baseFee: 100, cost: 10 },
        { eScore: 100, baseFee: 100, cost: 10 },
        { eScore: 50, baseFee: 500, cost: 17 },
        { eScore: 100, baseFee: 1000, cost: 35 },
    ];

    for (const tc of testCases) {
        const result = harmony.calculateFinalFee(tc.eScore, tc.baseFee, tc.cost);
        assertTrue(result.efficiency.isViable,
            `Case eScore=${tc.eScore}, baseFee=${tc.baseFee}, cost=${tc.cost} should be viable`);
    }
});

// ═══════════════════════════════════════════════════════════════
// TESTS: FEE DISTRIBUTION
// ═══════════════════════════════════════════════════════════════

console.log('\n=== FEE DISTRIBUTION ===\n');

test('Fee distribution sums to total', () => {
    const dist = harmony.distributeFee(100);
    const sum = dist.burn + dist.rewards + dist.treasury;
    assertApprox(sum, 100, 0.01);
});

test('Fee distribution ratios correct', () => {
    const dist = harmony.distributeFee(100);
    assertApprox(dist.burn, 38.2, 0.1);
    assertApprox(dist.rewards, 38.2, 0.1);
    assertApprox(dist.treasury, 23.6, 0.1);
});

test('Fee distribution with zero = all zeros', () => {
    const dist = harmony.distributeFee(0);
    assertEqual(dist.burn, 0);
    assertEqual(dist.rewards, 0);
    assertEqual(dist.treasury, 0);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: TIERS
// ═══════════════════════════════════════════════════════════════

console.log('\n=== TIERS ===\n');

test('Tier at E-Score 0 = Observer', () => {
    const tier = harmony.getTier(0);
    assertEqual(tier.name, 'Observer');
});

test('Tier at E-Score 1 = Starter', () => {
    const tier = harmony.getTier(1);
    assertEqual(tier.name, 'Starter');
});

test('Tier at E-Score 100 = Legendary', () => {
    const tier = harmony.getTier(100);
    assertEqual(tier.name, 'Legendary');
});

test('Tier at E-Score 50 = Mythic', () => {
    const tier = harmony.getTier(50);
    assertEqual(tier.name, 'Mythic');
});

test('Progress to next tier works', () => {
    const progress = harmony.getProgressToNextTier(5);
    assertEqual(progress.currentTier, 'Common');
    assertEqual(progress.nextTier, 'Uncommon');
    assertTrue(progress.progress > 0 && progress.progress < 1);
});

test('No progress at max tier', () => {
    const progress = harmony.getProgressToNextTier(100);
    assertEqual(progress, null);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: BENEFITS
// ═══════════════════════════════════════════════════════════════

console.log('\n=== BENEFITS ===\n');

test('Benefits at E-Score 0', () => {
    const benefits = harmony.calculateBenefits(0);
    assertEqual(benefits.benefits.freeCalls, 10);
    assertEqual(benefits.benefits.rateLimit, 1);
    assertEqual(benefits.benefits.theoreticalDiscount, 0);
});

test('Benefits at E-Score 10', () => {
    const benefits = harmony.calculateBenefits(10);
    assertEqual(benefits.benefits.freeCalls, 210); // 10 + 10*20
    assertEqual(benefits.benefits.rateLimit, 51);  // min(1000, 1 + 10*5)
    assertApprox(benefits.benefits.theoreticalDiscount, 45, 5);
});

test('Benefits rate limit capped at 1000', () => {
    const benefits = harmony.calculateBenefits(1000);
    assertEqual(benefits.benefits.rateLimit, 1000);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: REWARD DISTRIBUTION
// ═══════════════════════════════════════════════════════════════

console.log('\n=== REWARD DISTRIBUTION ===\n');

test('Reward share calculation', () => {
    // Participant has 10 E-Score out of 100 total, pool is 1000
    const share = harmony.calculateRewardShare(10, 100, 1000);
    assertEqual(share, 100); // 10% of 1000
});

test('Reward share with zero total = 0', () => {
    const share = harmony.calculateRewardShare(10, 0, 1000);
    assertEqual(share, 0);
});

test('Reward pool calculation', () => {
    const pool = harmony.calculateRewardPool(1000);
    assertApprox(pool.rewards, 382, 1);
    assertApprox(pool.burn, 382, 1);
    assertApprox(pool.treasury, 236, 1);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: VALIDATION
// ═══════════════════════════════════════════════════════════════

console.log('\n=== VALIDATION ===\n');

test('Operation viability check - viable', () => {
    const result = harmony.validateOperationViability(100, 10);
    assertTrue(result.viable);
    assertEqual(result.issue, null);
});

test('Operation viability check - not viable', () => {
    const result = harmony.validateOperationViability(50, 50);
    assertTrue(!result.viable);
    assertTrue(result.issue !== null);
});

test('Batch validation', () => {
    const results = harmony.validateOperationCosts([
        { type: 'api_call', baseFee: 100, cost: 2 },
        { type: 'gasless_tx', baseFee: 500, cost: 17 },
        { type: 'bad_op', baseFee: 10, cost: 50 }
    ]);

    assertEqual(results.length, 3);
    assertTrue(results[0].viable);
    assertTrue(results[1].viable);
    assertTrue(!results[2].viable);
});

// ═══════════════════════════════════════════════════════════════
// TESTS: REAL-WORLD SCENARIOS
// ═══════════════════════════════════════════════════════════════

console.log('\n=== REAL-WORLD SCENARIOS ===\n');

test('Scenario: GASdf gasless TX with high E-Score user', () => {
    // User with E-Score 50, gasless TX costs 17 $ASDF, base fee 500
    const result = harmony.calculateFinalFee(50, 500, 17);

    // Theoretical: 86%, Max: 83%, Effective: 83%
    assertTrue(result.discounts.effective > 80);
    assertTrue(result.efficiency.isViable);
    assertTrue(result.efficiency.profit > 0);

    console.log(`    GASdf TX: ${result.finalFee} $ASDF (${result.discounts.effective}% off)`);
});

test('Scenario: HolDex API call with medium E-Score user', () => {
    // User with E-Score 10, API call costs 2 $ASDF, base fee 25
    const result = harmony.calculateFinalFee(10, 25, 2);

    // Should get decent discount
    assertTrue(result.discounts.effective > 30);
    assertTrue(result.efficiency.isViable);

    console.log(`    HolDex API: ${result.finalFee} $ASDF (${result.discounts.effective}% off)`);
});

test('Scenario: Free tier user (E-Score 0)', () => {
    const result = harmony.calculateFinalFee(0, 25, 2);

    // No discount
    assertEqual(result.discounts.effective, 0);
    assertEqual(result.finalFee, 25);
    assertTrue(result.efficiency.isViable);
});

test('Scenario: Whale vs Active User comparison', () => {
    // Passive whale: 1M tokens, 6 months
    const whale = harmony.calculateEScore({
        holdings: 1000000,
        daysActive: 180
    });

    // Active user: 10K tokens, burns, uses API, refers
    const activeUser = harmony.calculateEScore({
        holdings: 10000,
        burned: 5000,
        apiCalls30d: 10000,
        referralsActive: 10,
        daysActive: 90
    });

    console.log(`    Whale E-Score: ${whale.score} (${whale.activeDimensions} dimensions)`);
    console.log(`    Active User E-Score: ${activeUser.score} (${activeUser.activeDimensions} dimensions)`);

    // Active user should have higher score despite less capital
    assertTrue(activeUser.score > whale.score,
        'Active user should beat passive whale');
});

// ═══════════════════════════════════════════════════════════════
// SUMMARY
// ═══════════════════════════════════════════════════════════════

console.log('\n' + '='.repeat(60));
console.log(`\n  RESULTS: ${passed} passed, ${failed} failed\n`);

if (failed > 0) {
    console.log('  ❌ SOME TESTS FAILED\n');
    process.exit(1);
} else {
    console.log('  ✅ ALL TESTS PASSED\n');
    process.exit(0);
}
