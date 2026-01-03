/**
 * Generate visuals for K-Score X Article
 */

const { createCanvas } = require('canvas');
const fs = require('fs');
const path = require('path');

// Colors (same as cardGenerator)
const COLORS = {
    background: '#0d1117',
    backgroundGradient: '#161b22',
    surface: '#21262d',
    border: '#30363d',
    text: '#f0f6fc',
    textSecondary: '#8b949e',

    // Grade colors
    native: '#6366f1',
    diamond: '#b9f2ff',
    platinum: '#e5e4e2',
    gold: '#ffd700',
    silver: '#c0c0c0',
    bronze: '#cd7f32',
    copper: '#b87333',
    iron: '#434343',
    rust: '#8b4513',

    // Holder role colors
    accumulator: '#b9f2ff',
    holder: '#ffd700',
    reducer: '#c0c0c0',
    extractor: '#8b4513'
};

// Tier configurations (using symbols instead of emojis for canvas compatibility)
const TIERS = [
    { score: '90+', metal: 'Diamond', icon: '◆', grade: 'A1', label: 'Prime', color: COLORS.diamond, action: 'Ape with confidence' },
    { score: '80-89', metal: 'Platinum', icon: '◇', grade: 'A2', label: 'Excellent', color: COLORS.platinum, action: 'Strong bet' },
    { score: '70-79', metal: 'Gold', icon: '●', grade: 'A3', label: 'Good', color: COLORS.gold, action: 'Verify details' },
    { score: '60-69', metal: 'Silver', icon: '○', grade: 'B1', label: 'Fair', color: COLORS.silver, action: 'Caution advised' },
    { score: '50-59', metal: 'Bronze', icon: '◐', grade: 'B2', label: 'Speculative', color: COLORS.bronze, action: 'Small bag or skip' },
    { score: '40-49', metal: 'Copper', icon: '◑', grade: 'B3', label: 'Risky', color: COLORS.copper, action: 'High risk' },
    { score: '20-39', metal: 'Iron', icon: '■', grade: 'C', label: 'High Risk', color: COLORS.iron, action: 'Danger zone' },
    { score: '<20', metal: 'Rust', icon: '▣', grade: 'D', label: 'Junk', color: COLORS.rust, action: 'Run.' }
];

// Holder roles (using symbols instead of emojis)
const HOLDER_ROLES = [
    { role: 'Accumulator', metal: 'Diamond', icon: '◆', ratio: '≥ 2.0', behavior: '2x more buys than sells', impact: 'Bullish', color: COLORS.accumulator },
    { role: 'Holder', metal: 'Gold', icon: '●', ratio: '≥ 0.8', behavior: 'Roughly balanced', impact: 'Bullish', color: COLORS.holder },
    { role: 'Reducer', metal: 'Silver', icon: '○', ratio: '≥ 0.3', behavior: 'More sells than buys', impact: 'Neutral', color: COLORS.reducer },
    { role: 'Extractor', metal: 'Rust', icon: '▣', ratio: '< 0.3', behavior: 'Heavy selling', impact: 'Bearish', color: COLORS.extractor }
];

function roundRect(ctx, x, y, width, height, radius) {
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.lineTo(x + width - radius, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
    ctx.lineTo(x + width, y + height - radius);
    ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
    ctx.lineTo(x + radius, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
    ctx.lineTo(x, y + radius);
    ctx.quadraticCurveTo(x, y, x + radius, y);
    ctx.closePath();
}

/**
 * Generate Credit Rating Tiers Visual
 */
function generateTiersVisual() {
    const width = 1200;
    const height = 900;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');

    // Background
    const bgGradient = ctx.createLinearGradient(0, 0, width, height);
    bgGradient.addColorStop(0, COLORS.background);
    bgGradient.addColorStop(1, COLORS.backgroundGradient);
    ctx.fillStyle = bgGradient;
    ctx.fillRect(0, 0, width, height);

    // Title
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 48px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('K-Score Credit Rating System', width / 2, 70);

    // Subtitle
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '24px Arial';
    ctx.fillText('"Moody\'s for Memecoins"', width / 2, 110);

    // Tiers
    const startY = 160;
    const rowHeight = 85;
    const padding = 60;

    TIERS.forEach((tier, i) => {
        const y = startY + i * rowHeight;
        const rowWidth = width - padding * 2;

        // Row background
        ctx.fillStyle = tier.color + '15';
        roundRect(ctx, padding, y, rowWidth, 75, 12);
        ctx.fill();

        // Left border accent
        ctx.fillStyle = tier.color;
        roundRect(ctx, padding, y, 8, 75, 4);
        ctx.fill();

        // Score range
        ctx.fillStyle = COLORS.text;
        ctx.font = 'bold 28px Arial';
        ctx.textAlign = 'left';
        ctx.fillText(tier.score, padding + 30, y + 45);

        // Icon + Metal
        ctx.fillStyle = tier.color;
        ctx.font = '32px Arial';
        ctx.fillText(tier.icon, padding + 150, y + 47);

        ctx.fillStyle = COLORS.text;
        ctx.font = 'bold 26px Arial';
        ctx.fillText(tier.metal, padding + 195, y + 45);

        // Grade
        ctx.fillStyle = tier.color;
        ctx.font = 'bold 24px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(tier.grade, padding + 380, y + 45);

        // Label
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = '22px Arial';
        ctx.textAlign = 'left';
        ctx.fillText(tier.label, padding + 450, y + 45);

        // Action
        ctx.fillStyle = COLORS.text;
        ctx.font = '20px Arial';
        ctx.textAlign = 'right';
        ctx.fillText(tier.action, width - padding - 20, y + 45);
    });

    // Footer
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '20px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('holdex.io  •  Wallets Don\'t Lie', width / 2, height - 40);

    return canvas.toBuffer('image/png');
}

/**
 * Generate Holder Roles Visual
 */
function generateHolderRolesVisual() {
    const width = 1200;
    const height = 700;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');

    // Background
    const bgGradient = ctx.createLinearGradient(0, 0, width, height);
    bgGradient.addColorStop(0, COLORS.background);
    bgGradient.addColorStop(1, COLORS.backgroundGradient);
    ctx.fillStyle = bgGradient;
    ctx.fillRect(0, 0, width, height);

    // Title
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 48px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('Top 20 Holder Classification', width / 2, 70);

    // Subtitle
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '24px Arial';
    ctx.fillText('Buy/Sell Ratio Analysis', width / 2, 110);

    // Roles
    const startY = 160;
    const rowHeight = 120;
    const padding = 60;

    HOLDER_ROLES.forEach((role, i) => {
        const y = startY + i * rowHeight;
        const rowWidth = width - padding * 2;

        // Row background
        ctx.fillStyle = role.color + '20';
        roundRect(ctx, padding, y, rowWidth, 100, 16);
        ctx.fill();

        // Left accent
        ctx.fillStyle = role.color;
        roundRect(ctx, padding, y, 10, 100, 5);
        ctx.fill();

        // Icon
        ctx.font = '48px Arial';
        ctx.fillText(role.icon, padding + 40, y + 62);

        // Metal name
        ctx.fillStyle = role.color;
        ctx.font = 'bold 32px Arial';
        ctx.textAlign = 'left';
        ctx.fillText(role.metal, padding + 110, y + 40);

        // Role name
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = '22px Arial';
        ctx.fillText(role.role, padding + 110, y + 70);

        // Ratio
        ctx.fillStyle = COLORS.text;
        ctx.font = 'bold 28px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(`Ratio ${role.ratio}`, padding + 380, y + 55);

        // Behavior
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = '20px Arial';
        ctx.textAlign = 'left';
        ctx.fillText(role.behavior, padding + 500, y + 45);

        // Impact badge
        const impactColor = role.impact === 'Bullish' ? '#3fb950' :
                          role.impact === 'Bearish' ? '#f85149' : '#d29922';
        ctx.fillStyle = impactColor + '30';
        roundRect(ctx, width - padding - 140, y + 30, 120, 40, 20);
        ctx.fill();

        ctx.fillStyle = impactColor;
        ctx.font = 'bold 18px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(role.impact, width - padding - 80, y + 57);
    });

    // Formula note
    ctx.fillStyle = COLORS.surface;
    roundRect(ctx, padding, height - 100, width - padding * 2, 50, 10);
    ctx.fill();

    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '18px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('Diamond Hands % = (Diamond + Gold) / Top 20 × 100  •  Below 50% = Exit Liquidity', width / 2, height - 68);

    // Footer
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '20px Arial';
    ctx.fillText('holdex.io', width / 2, height - 25);

    return canvas.toBuffer('image/png');
}

/**
 * Generate 3 Pillars Visual
 */
function generatePillarsVisual() {
    const width = 1200;
    const height = 500;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');

    // Background
    const bgGradient = ctx.createLinearGradient(0, 0, width, height);
    bgGradient.addColorStop(0, COLORS.background);
    bgGradient.addColorStop(1, COLORS.backgroundGradient);
    ctx.fillStyle = bgGradient;
    ctx.fillRect(0, 0, width, height);

    // Title
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 42px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('K-Score Formula: 3 Pillars', width / 2, 60);

    // Formula
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '20px Arial';
    ctx.fillText('K = 100 × DiamondHands^0.50 × OrganicGrowth^0.35 × Longevity^0.15', width / 2, 100);

    const pillars = [
        {
            name: 'Diamond Hands',
            weight: '50%',
            icon: '◆',
            color: COLORS.diamond,
            desc: 'Top 20 holder conviction',
            detail: 'Accumulators + Holders ratio'
        },
        {
            name: 'Organic Growth',
            weight: '35%',
            icon: '↑',
            color: '#3fb950',
            desc: 'Real holders ($1+ balance)',
            detail: 'Distribution fairness'
        },
        {
            name: 'Longevity',
            weight: '15%',
            icon: '⧖',
            color: '#d29922',
            desc: 'Token age (τ=21 days)',
            detail: '~99% at 90 days'
        }
    ];

    const pillarWidth = 340;
    const pillarHeight = 280;
    const startX = (width - pillarWidth * 3 - 40) / 2;
    const startY = 140;

    pillars.forEach((pillar, i) => {
        const x = startX + i * (pillarWidth + 20);

        // Pillar background
        ctx.fillStyle = pillar.color + '15';
        roundRect(ctx, x, startY, pillarWidth, pillarHeight, 20);
        ctx.fill();

        // Top accent
        ctx.fillStyle = pillar.color;
        roundRect(ctx, x, startY, pillarWidth, 8, 4);
        ctx.fill();

        // Weight circle
        ctx.beginPath();
        ctx.arc(x + pillarWidth / 2, startY + 70, 45, 0, Math.PI * 2);
        ctx.fillStyle = pillar.color + '30';
        ctx.fill();
        ctx.strokeStyle = pillar.color;
        ctx.lineWidth = 3;
        ctx.stroke();

        // Weight text
        ctx.fillStyle = pillar.color;
        ctx.font = 'bold 28px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(pillar.weight, x + pillarWidth / 2, startY + 78);

        // Icon
        ctx.font = '36px Arial';
        ctx.fillText(pillar.icon, x + pillarWidth / 2, startY + 145);

        // Name
        ctx.fillStyle = COLORS.text;
        ctx.font = 'bold 24px Arial';
        ctx.fillText(pillar.name, x + pillarWidth / 2, startY + 190);

        // Description
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = '18px Arial';
        ctx.fillText(pillar.desc, x + pillarWidth / 2, startY + 225);
        ctx.fillText(pillar.detail, x + pillarWidth / 2, startY + 250);
    });

    // Footer
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '20px Arial';
    ctx.textAlign = 'center';
    ctx.fillText('holdex.io  •  Weighted Geometric Mean', width / 2, height - 30);

    return canvas.toBuffer('image/png');
}

/**
 * Generate Hero Card - fetch real token and create card
 */
async function generateHeroCard() {
    const { generateKScoreCard } = require('../src/services/cardGenerator');

    // Fetch a high-scoring verified token from the API
    const response = await fetch('https://holdex-api.onrender.com/api/tokens?filter=verified&sort=kscore&direction=desc&limit=1');
    const data = await response.json();

    if (!data.success || !data.tokens || data.tokens.length === 0) {
        console.log('   ⚠ No verified tokens found, skipping hero card');
        return null;
    }

    const topToken = data.tokens[0];
    console.log(`   Found: ${topToken.symbol} (K-Score: ${topToken.kScore})`);

    // Fetch full token details for conviction data
    const detailResponse = await fetch(`https://holdex-api.onrender.com/api/token/${topToken.mint}`);
    const detailData = await detailResponse.json();

    if (!detailData.success) {
        console.log('   ⚠ Could not fetch token details');
        return null;
    }

    const token = detailData.token;

    // Generate card
    const cardBuffer = await generateKScoreCard({
        name: token.name,
        symbol: token.symbol,
        mint: token.mint,
        k_score: token.kScore || token.k_score,
        image: token.image,
        conviction_accumulators: token.conviction?.accumulators || 0,
        conviction_holders: token.conviction?.holders || 0,
        conviction_reducers: token.conviction?.reducers || 0,
        conviction_extractors: token.conviction?.extractors || 0,
        holders: token.holders,
        marketCap: token.marketCap
    });

    return cardBuffer;
}

// Main execution
async function main() {
    const outputDir = path.join(__dirname, '../article_visuals');

    // Create output directory
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    console.log('Generating article visuals...\n');

    // Generate Hero Card (real token)
    console.log('1. Generating Hero Card (top verified token)...');
    try {
        const heroBuffer = await generateHeroCard();
        if (heroBuffer) {
            fs.writeFileSync(path.join(outputDir, 'hero_card.png'), heroBuffer);
            console.log('   ✓ hero_card.png');
        }
    } catch (e) {
        console.log('   ⚠ Hero card failed:', e.message);
    }

    // Generate Tiers Visual
    console.log('2. Generating Credit Rating Tiers...');
    const tiersBuffer = generateTiersVisual();
    fs.writeFileSync(path.join(outputDir, 'kscore_tiers.png'), tiersBuffer);
    console.log('   ✓ kscore_tiers.png');

    // Generate Holder Roles Visual
    console.log('3. Generating Holder Roles...');
    const rolesBuffer = generateHolderRolesVisual();
    fs.writeFileSync(path.join(outputDir, 'holder_roles.png'), rolesBuffer);
    console.log('   ✓ holder_roles.png');

    // Generate Pillars Visual
    console.log('4. Generating 3 Pillars...');
    const pillarsBuffer = generatePillarsVisual();
    fs.writeFileSync(path.join(outputDir, 'kscore_pillars.png'), pillarsBuffer);
    console.log('   ✓ kscore_pillars.png');

    console.log(`\n✅ All visuals saved to: ${outputDir}`);
}

main().catch(console.error);
