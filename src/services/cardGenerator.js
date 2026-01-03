/**
 * K-Score Card Image Generator
 * Generates shareable PNG images for Twitter/social media
 */

const { createCanvas, loadImage, registerFont } = require('canvas');
const path = require('path');
const axios = require('axios');

// Card dimensions (Twitter card optimal: 1200x628 or 1200x675)
const CARD_WIDTH = 1200;
const CARD_HEIGHT = 628;

// Colors
const COLORS = {
    background: '#0d1117',
    backgroundGradient: '#161b22',
    surface: '#21262d',
    border: '#30363d',
    text: '#f0f6fc',
    textSecondary: '#8b949e',
    accent: '#58a6ff',

    // Grade colors
    native: '#6366f1',   // Infrastructure (indigo)
    diamond: '#b9f2ff',
    platinum: '#e5e4e2',
    gold: '#ffd700',
    silver: '#c0c0c0',
    bronze: '#cd7f32',
    copper: '#b87333',
    iron: '#434343',
    rust: '#8b4513',

    // Conviction colors (holder role metals)
    accumulator: '#b9f2ff',  // Diamond
    holder: '#ffd700',       // Gold
    reducer: '#c0c0c0',      // Silver
    extractor: '#8b4513'     // Rust
};

// Holder Role configurations (conviction analysis)
const HOLDER_ROLES = {
    accumulator: { icon: '💎', metal: 'Diamond', label: 'Accumulators', color: COLORS.accumulator, impact: 'bullish' },
    holder: { icon: '🥇', metal: 'Gold', label: 'Holders', color: COLORS.holder, impact: 'bullish' },
    reducer: { icon: '🥈', metal: 'Silver', label: 'Reducers', color: COLORS.reducer, impact: 'neutral' },
    extractor: { icon: '🔩', metal: 'Rust', label: 'Extractors', color: COLORS.extractor, impact: 'bearish' }
};

// Grade configurations
const GRADES = {
    native: { min: null, icon: '🏛️', label: 'Native', credit: 'Infrastructure', color: COLORS.native },
    diamond: { min: 90, icon: '💎', label: 'Diamond', credit: 'A1 Prime', color: COLORS.diamond },
    platinum: { min: 80, icon: '💠', label: 'Platinum', credit: 'A2 Excellent', color: COLORS.platinum },
    gold: { min: 70, icon: '🥇', label: 'Gold', credit: 'A3 Good', color: COLORS.gold },
    silver: { min: 60, icon: '🥈', label: 'Silver', credit: 'B1 Fair', color: COLORS.silver },
    bronze: { min: 50, icon: '🥉', label: 'Bronze', credit: 'B2 Speculative', color: COLORS.bronze },
    copper: { min: 40, icon: '🟤', label: 'Copper', credit: 'B3 Risky', color: COLORS.copper },
    iron: { min: 20, icon: '⚫', label: 'Iron', credit: 'C High Risk', color: COLORS.iron },
    rust: { min: 0, icon: '🔩', label: 'Rust', credit: 'D Junk', color: COLORS.rust }
};

// Native tokens - Infrastructure, not rated
const NATIVE_TOKENS = new Set([
    'So11111111111111111111111111111111111111112',  // Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',  // mSOL (Marinade)
    'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn', // JitoSOL
    'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',  // bSOL (Blaze)
    'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',  // JupSOL
    '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj', // stSOL (Lido)
]);

function getGrade(score, mint = null) {
    // Native tokens are infrastructure - not rated
    if (mint && NATIVE_TOKENS.has(mint)) return GRADES.native;
    if (score >= 90) return GRADES.diamond;
    if (score >= 80) return GRADES.platinum;
    if (score >= 70) return GRADES.gold;
    if (score >= 60) return GRADES.silver;
    if (score >= 50) return GRADES.bronze;
    if (score >= 40) return GRADES.copper;
    if (score >= 20) return GRADES.iron;
    return GRADES.rust;
}

/**
 * Draw rounded rectangle helper
 */
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
 * Fetch token image from URL
 */
async function fetchTokenImage(imageUrl) {
    if (!imageUrl) return null;

    try {
        const response = await axios.get(imageUrl, {
            responseType: 'arraybuffer',
            timeout: 5000,
            headers: {
                'User-Agent': 'HolDex-CardGenerator/1.0'
            }
        });

        const buffer = Buffer.from(response.data);
        return await loadImage(buffer);
    } catch (err) {
        console.warn(`Failed to load token image: ${err.message}`);
        return null;
    }
}

/**
 * Draw conviction breakdown bar
 */
function drawConvictionBar(ctx, x, y, width, height, data) {
    const total = data.accumulators + data.holders + data.reducers + data.extractors;
    if (total === 0) return;

    const segments = [
        { count: data.accumulators, color: COLORS.accumulator, label: 'Acc' },
        { count: data.holders, color: COLORS.holder, label: 'Hold' },
        { count: data.reducers, color: COLORS.reducer, label: 'Red' },
        { count: data.extractors, color: COLORS.extractor, label: 'Ext' }
    ];

    let currentX = x;
    const radius = 6;

    // Draw background
    ctx.fillStyle = COLORS.surface;
    roundRect(ctx, x, y, width, height, radius);
    ctx.fill();

    // Draw segments
    segments.forEach((segment, index) => {
        if (segment.count === 0) return;

        const segmentWidth = (segment.count / total) * width;

        ctx.fillStyle = segment.color;

        if (index === 0) {
            // First segment - rounded left
            ctx.beginPath();
            ctx.moveTo(currentX + radius, y);
            ctx.lineTo(currentX + segmentWidth, y);
            ctx.lineTo(currentX + segmentWidth, y + height);
            ctx.lineTo(currentX + radius, y + height);
            ctx.quadraticCurveTo(currentX, y + height, currentX, y + height - radius);
            ctx.lineTo(currentX, y + radius);
            ctx.quadraticCurveTo(currentX, y, currentX + radius, y);
            ctx.fill();
        } else if (index === segments.length - 1 || segments.slice(index + 1).every(s => s.count === 0)) {
            // Last non-zero segment - rounded right
            ctx.beginPath();
            ctx.moveTo(currentX, y);
            ctx.lineTo(currentX + segmentWidth - radius, y);
            ctx.quadraticCurveTo(currentX + segmentWidth, y, currentX + segmentWidth, y + radius);
            ctx.lineTo(currentX + segmentWidth, y + height - radius);
            ctx.quadraticCurveTo(currentX + segmentWidth, y + height, currentX + segmentWidth - radius, y + height);
            ctx.lineTo(currentX, y + height);
            ctx.fill();
        } else {
            // Middle segment - no rounding
            ctx.fillRect(currentX, y, segmentWidth, height);
        }

        currentX += segmentWidth;
    });
}

/**
 * Generate K-Score card image
 *
 * @param {Object} token - Token data
 * @param {string} token.name - Token name
 * @param {string} token.symbol - Token symbol
 * @param {number} token.k_score - K-Score (0-100)
 * @param {string} token.image - Token image URL
 * @param {number} token.conviction_accumulators - Count of accumulator holders
 * @param {number} token.conviction_holders - Count of holder holders
 * @param {number} token.conviction_reducers - Count of reducer holders
 * @param {number} token.conviction_extractors - Count of extractor holders
 * @param {number} token.holders - Total holder count
 * @param {number} token.marketCap - Market cap in USD
 * @returns {Buffer} PNG image buffer
 */
async function generateKScoreCard(token) {
    const canvas = createCanvas(CARD_WIDTH, CARD_HEIGHT);
    const ctx = canvas.getContext('2d');

    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);

    // === BACKGROUND ===
    // Gradient background
    const bgGradient = ctx.createLinearGradient(0, 0, CARD_WIDTH, CARD_HEIGHT);
    bgGradient.addColorStop(0, COLORS.background);
    bgGradient.addColorStop(1, COLORS.backgroundGradient);
    ctx.fillStyle = bgGradient;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Subtle grid pattern
    ctx.strokeStyle = 'rgba(48, 54, 61, 0.3)';
    ctx.lineWidth = 1;
    for (let i = 0; i < CARD_WIDTH; i += 40) {
        ctx.beginPath();
        ctx.moveTo(i, 0);
        ctx.lineTo(i, CARD_HEIGHT);
        ctx.stroke();
    }
    for (let i = 0; i < CARD_HEIGHT; i += 40) {
        ctx.beginPath();
        ctx.moveTo(0, i);
        ctx.lineTo(CARD_WIDTH, i);
        ctx.stroke();
    }

    // === LEFT SECTION: Token Info ===
    const leftPadding = 60;
    const topPadding = 50;

    // Token image (circular)
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 120;
    const imgX = leftPadding;
    const imgY = topPadding;

    // Draw circular clip for token image
    ctx.save();
    ctx.beginPath();
    ctx.arc(imgX + imgSize/2, imgY + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.closePath();
    ctx.clip();

    if (tokenImg) {
        ctx.drawImage(tokenImg, imgX, imgY, imgSize, imgSize);
    } else {
        // Placeholder circle
        ctx.fillStyle = COLORS.surface;
        ctx.fillRect(imgX, imgY, imgSize, imgSize);
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = 'bold 48px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(token.symbol?.[0] || '?', imgX + imgSize/2, imgY + imgSize/2 + 16);
    }
    ctx.restore();

    // Token image border
    ctx.strokeStyle = grade.color;
    ctx.lineWidth = 4;
    ctx.beginPath();
    ctx.arc(imgX + imgSize/2, imgY + imgSize/2, imgSize/2 + 2, 0, Math.PI * 2);
    ctx.stroke();

    // Token name
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 42px Arial';
    ctx.textAlign = 'left';
    const displayName = (token.name || 'Unknown').slice(0, 20);
    ctx.fillText(displayName, imgX + imgSize + 30, imgY + 45);

    // Token symbol
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '28px Arial';
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, imgX + imgSize + 30, imgY + 85);

    // Market cap & holders
    ctx.font = '22px Arial';
    const mcap = token.marketCap ? `$${formatNumber(token.marketCap)}` : 'N/A';
    const holders = token.holders ? formatNumber(token.holders) : 'N/A';
    ctx.fillText(`MCap: ${mcap}  •  Holders: ${holders}`, imgX + imgSize + 30, imgY + 120);

    // === CENTER SECTION: K-Score ===
    const centerX = CARD_WIDTH / 2;
    const scoreY = 280;

    // Score circle background
    const circleRadius = 110;
    ctx.beginPath();
    ctx.arc(centerX, scoreY, circleRadius, 0, Math.PI * 2);
    ctx.fillStyle = COLORS.surface;
    ctx.fill();
    ctx.strokeStyle = grade.color;
    ctx.lineWidth = 8;
    ctx.stroke();

    // Score arc (progress indicator)
    const scoreAngle = (score / 100) * Math.PI * 2 - Math.PI / 2;
    ctx.beginPath();
    ctx.arc(centerX, scoreY, circleRadius - 15, -Math.PI / 2, scoreAngle);
    ctx.strokeStyle = grade.color;
    ctx.lineWidth = 12;
    ctx.lineCap = 'round';
    ctx.stroke();
    ctx.lineCap = 'butt';

    // Score number
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 72px Arial';
    ctx.textAlign = 'center';
    ctx.fillText(score.toString(), centerX, scoreY + 25);

    // "K-SCORE" label
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = 'bold 18px Arial';
    ctx.fillText('K-SCORE', centerX, scoreY - 50);

    // === GRADE BADGE ===
    const badgeY = scoreY + circleRadius + 40;
    const badgeWidth = 260;
    const badgeHeight = 50;

    // Badge background
    ctx.fillStyle = grade.color + '20'; // 20% opacity
    roundRect(ctx, centerX - badgeWidth/2, badgeY, badgeWidth, badgeHeight, 25);
    ctx.fill();
    ctx.strokeStyle = grade.color;
    ctx.lineWidth = 2;
    roundRect(ctx, centerX - badgeWidth/2, badgeY, badgeWidth, badgeHeight, 25);
    ctx.stroke();

    // Badge text
    ctx.fillStyle = grade.color;
    ctx.font = 'bold 24px Arial';
    ctx.textAlign = 'center';
    ctx.fillText(`${grade.icon} ${grade.label} • ${grade.credit}`, centerX, badgeY + 33);

    // === RIGHT SECTION: Conviction Breakdown ===
    const rightX = CARD_WIDTH - 380;
    const convictionY = topPadding + 30;

    // Section title
    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 24px Arial';
    ctx.textAlign = 'left';
    ctx.fillText('Top 20 Conviction', rightX, convictionY);

    // Conviction data
    const conviction = {
        accumulators: token.conviction_accumulators || 0,
        holders: token.conviction_holders || 0,
        reducers: token.conviction_reducers || 0,
        extractors: token.conviction_extractors || 0
    };

    // Draw conviction bar
    const barY = convictionY + 25;
    const barWidth = 300;
    const barHeight = 24;
    drawConvictionBar(ctx, rightX, barY, barWidth, barHeight, conviction);

    // Legend
    const legendY = barY + 50;
    const legendItems = [
        { label: `${HOLDER_ROLES.accumulator.icon} ${HOLDER_ROLES.accumulator.metal}`, count: conviction.accumulators, color: HOLDER_ROLES.accumulator.color },
        { label: `${HOLDER_ROLES.holder.icon} ${HOLDER_ROLES.holder.metal}`, count: conviction.holders, color: HOLDER_ROLES.holder.color },
        { label: `${HOLDER_ROLES.reducer.icon} ${HOLDER_ROLES.reducer.metal}`, count: conviction.reducers, color: HOLDER_ROLES.reducer.color },
        { label: `${HOLDER_ROLES.extractor.icon} ${HOLDER_ROLES.extractor.metal}`, count: conviction.extractors, color: HOLDER_ROLES.extractor.color }
    ];

    legendItems.forEach((item, i) => {
        const itemY = legendY + i * 35;

        // Color dot
        ctx.beginPath();
        ctx.arc(rightX + 10, itemY, 8, 0, Math.PI * 2);
        ctx.fillStyle = item.color;
        ctx.fill();

        // Label
        ctx.fillStyle = COLORS.textSecondary;
        ctx.font = '20px Arial';
        ctx.textAlign = 'left';
        ctx.fillText(item.label, rightX + 30, itemY + 6);

        // Count
        ctx.fillStyle = COLORS.text;
        ctx.font = 'bold 20px Arial';
        ctx.textAlign = 'right';
        ctx.fillText(item.count.toString(), rightX + barWidth, itemY + 6);
    });

    // Diamond Hands percentage
    const total = conviction.accumulators + conviction.holders + conviction.reducers + conviction.extractors;
    const diamondHands = total > 0
        ? Math.round(((conviction.accumulators + conviction.holders) / total) * 100)
        : 0;

    const dhY = legendY + 160;
    ctx.fillStyle = COLORS.surface;
    roundRect(ctx, rightX, dhY, barWidth, 50, 10);
    ctx.fill();

    ctx.fillStyle = COLORS.text;
    ctx.font = 'bold 20px Arial';
    ctx.textAlign = 'left';
    ctx.fillText('Diamond Hands', rightX + 15, dhY + 32);

    ctx.fillStyle = diamondHands >= 50 ? COLORS.accumulator : COLORS.extractor;
    ctx.font = 'bold 28px Arial';
    ctx.textAlign = 'right';
    ctx.fillText(`${diamondHands}%`, rightX + barWidth - 15, dhY + 34);

    // === FOOTER: Branding ===
    const footerY = CARD_HEIGHT - 45;

    // HolDex branding
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '22px Arial';
    ctx.textAlign = 'left';
    ctx.fillText('holdex.io', leftPadding, footerY);

    // Timestamp
    ctx.textAlign = 'right';
    ctx.font = '18px Arial';
    const now = new Date();
    ctx.fillText(now.toISOString().split('T')[0], CARD_WIDTH - leftPadding, footerY);

    // "Powered by K-Score" tagline
    ctx.textAlign = 'center';
    ctx.font = '18px Arial';
    ctx.fillText('On-chain conviction data', centerX, footerY);

    // Return PNG buffer
    return canvas.toBuffer('image/png');
}

/**
 * Format large numbers (1000 -> 1K, 1000000 -> 1M)
 */
function formatNumber(num) {
    if (!num || num === 0) return '0';
    if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    return num.toFixed(0);
}

/**
 * Get holder role info from conviction class
 * @param {string} convictionClass - 'accumulator', 'holder', 'reducer', 'extractor'
 * @returns {Object} { icon, metal, label, color, impact }
 */
function getHolderRole(convictionClass) {
    return HOLDER_ROLES[convictionClass] || HOLDER_ROLES.holder;
}

module.exports = {
    generateKScoreCard,
    getGrade,
    getHolderRole,
    HOLDER_ROLES,
    CARD_WIDTH,
    CARD_HEIGHT
};
