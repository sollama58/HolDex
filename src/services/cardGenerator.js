/**
 * K-Score Card Image Generator
 * 3 Styles: HolDex, ASDF, Minimal
 *
 * "Hold to enter. Cards unlock sharing."
 */

const { createCanvas, loadImage } = require('canvas');
const axios = require('axios');

// Card dimensions (Twitter card optimal: 1200x628)
const CARD_WIDTH = 1200;
const CARD_HEIGHT = 628;

// ═══════════════════════════════════════════════════════════════
// THEME CONFIGURATIONS
// ═══════════════════════════════════════════════════════════════

const THEMES = {
    // HolDex Style: Glassmorphism, dark, orange accent, "this is fine"
    holdex: {
        name: 'HolDex',
        background: '#0c0c14',
        backgroundAlt: '#14141f',
        surface: 'rgba(20, 20, 35, 0.8)',
        surfaceBorder: 'rgba(255, 255, 255, 0.08)',
        accent: '#f97316',
        accentGlow: 'rgba(249, 115, 22, 0.4)',
        text: '#f8fafc',
        textMuted: '#94a3b8',
        textDim: '#64748b',
        grades: {
            diamond: '#22d3ee',
            platinum: '#a855f7',
            gold: '#fbbf24',
            silver: '#94a3b8',
            bronze: '#f97316',
            copper: '#ea580c',
            iron: '#64748b',
            rust: '#ef4444',
            native: '#6366f1'
        },
        conviction: {
            accumulator: '#22d3ee',
            holder: '#fbbf24',
            reducer: '#94a3b8',
            extractor: '#ef4444'
        },
        tagline: 'this is fine.',
        font: 'Inter, Arial',
        borderRadius: 16,
        glassEffect: true
    },

    // ASDF Style: Burnt brown, fire theme, retro terminal, box-shadow
    asdf: {
        name: 'ASDForecast',
        background: '#451a03',
        backgroundAlt: '#7c2d12',
        surface: '#78350f',
        surfaceBorder: '#ea580c',
        accent: '#fb923c',
        accentGlow: 'rgba(251, 146, 60, 0.5)',
        text: '#ffedd5',
        textMuted: '#fdba74',
        textDim: '#fb923c',
        grades: {
            diamond: '#22d3ee',
            platinum: '#c4b5fd',
            gold: '#fde047',
            silver: '#d6d3d1',
            bronze: '#fb923c',
            copper: '#ea580c',
            iron: '#a8a29e',
            rust: '#dc2626',
            native: '#818cf8'
        },
        conviction: {
            accumulator: '#22d3ee',
            holder: '#fde047',
            reducer: '#d6d3d1',
            extractor: '#dc2626'
        },
        tagline: 'the room is on fire.',
        font: 'JetBrains Mono, Courier',
        borderRadius: 0,
        boxShadow: true,
        shadowOffset: 4
    },

    // Minimal Style: Clean white, Apple-like elegance
    minimal: {
        name: 'Minimal',
        background: '#fafafa',
        backgroundAlt: '#f5f5f5',
        surface: '#ffffff',
        surfaceBorder: '#e5e5e5',
        accent: '#171717',
        accentGlow: 'rgba(23, 23, 23, 0.1)',
        text: '#171717',
        textMuted: '#525252',
        textDim: '#a3a3a3',
        grades: {
            diamond: '#0ea5e9',
            platinum: '#8b5cf6',
            gold: '#eab308',
            silver: '#71717a',
            bronze: '#f97316',
            copper: '#ea580c',
            iron: '#52525b',
            rust: '#ef4444',
            native: '#6366f1'
        },
        conviction: {
            accumulator: '#0ea5e9',
            holder: '#eab308',
            reducer: '#71717a',
            extractor: '#ef4444'
        },
        tagline: 'on-chain conviction',
        font: 'SF Pro, Helvetica Neue, Arial',
        borderRadius: 12,
        clean: true
    }
};

// Grade thresholds
const GRADES = {
    native: { min: null, icon: 'N', label: 'Native', credit: 'Infrastructure' },
    diamond: { min: 90, icon: 'A+', label: 'Diamond', credit: 'Prime' },
    platinum: { min: 80, icon: 'A', label: 'Platinum', credit: 'Excellent' },
    gold: { min: 70, icon: 'B+', label: 'Gold', credit: 'Good' },
    silver: { min: 60, icon: 'B', label: 'Silver', credit: 'Fair' },
    bronze: { min: 50, icon: 'C+', label: 'Bronze', credit: 'Speculative' },
    copper: { min: 40, icon: 'C', label: 'Copper', credit: 'Risky' },
    iron: { min: 20, icon: 'D', label: 'Iron', credit: 'High Risk' },
    rust: { min: 0, icon: 'F', label: 'Rust', credit: 'Junk' }
};

// Native tokens (infrastructure)
const NATIVE_TOKENS = new Set([
    'So11111111111111111111111111111111111111112',
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
    'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',
    'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',
    'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',
    '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj',
]);

// ═══════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════

function getGrade(score, mint = null) {
    if (mint && NATIVE_TOKENS.has(mint)) return { key: 'native', ...GRADES.native };
    if (score >= 90) return { key: 'diamond', ...GRADES.diamond };
    if (score >= 80) return { key: 'platinum', ...GRADES.platinum };
    if (score >= 70) return { key: 'gold', ...GRADES.gold };
    if (score >= 60) return { key: 'silver', ...GRADES.silver };
    if (score >= 50) return { key: 'bronze', ...GRADES.bronze };
    if (score >= 40) return { key: 'copper', ...GRADES.copper };
    if (score >= 20) return { key: 'iron', ...GRADES.iron };
    return { key: 'rust', ...GRADES.rust };
}

function formatNumber(num) {
    if (!num || num === 0) return '0';
    if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    return num.toFixed(0);
}

function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
}

async function fetchTokenImage(imageUrl) {
    if (!imageUrl) return null;
    try {
        const response = await axios.get(imageUrl, {
            responseType: 'arraybuffer',
            timeout: 5000,
            headers: { 'User-Agent': 'HolDex-CardGenerator/2.0' }
        });
        return await loadImage(Buffer.from(response.data));
    } catch (err) {
        return null;
    }
}

// ═══════════════════════════════════════════════════════════════
// HOLDEX STYLE CARD
// ═══════════════════════════════════════════════════════════════

async function drawHoldexCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeColor = theme.grades[grade.key];

    // Background with gradient
    const bgGrad = ctx.createLinearGradient(0, 0, CARD_WIDTH, CARD_HEIGHT);
    bgGrad.addColorStop(0, theme.background);
    bgGrad.addColorStop(1, theme.backgroundAlt);
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Ambient glow (top-left orange, bottom-right purple)
    ctx.fillStyle = 'rgba(249, 115, 22, 0.08)';
    ctx.beginPath();
    ctx.arc(-100, -100, 400, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = 'rgba(168, 85, 247, 0.06)';
    ctx.beginPath();
    ctx.arc(CARD_WIDTH + 100, CARD_HEIGHT + 100, 500, 0, Math.PI * 2);
    ctx.fill();

    // === MAIN CONTENT CARD (glassmorphism) ===
    const margin = 40;
    const cardX = margin;
    const cardY = margin;
    const cardW = CARD_WIDTH - margin * 2;
    const cardH = CARD_HEIGHT - margin * 2;

    // Glass background
    ctx.fillStyle = theme.surface;
    roundRect(ctx, cardX, cardY, cardW, cardH, theme.borderRadius);
    ctx.fill();
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 1;
    roundRect(ctx, cardX, cardY, cardW, cardH, theme.borderRadius);
    ctx.stroke();

    // === LEFT: Token Info ===
    const leftX = cardX + 50;
    let y = cardY + 50;

    // Token image
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 80;

    ctx.save();
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, y + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.clip();
    if (tokenImg) {
        ctx.drawImage(tokenImg, leftX, y, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.backgroundAlt;
        ctx.fillRect(leftX, y, imgSize, imgSize);
        ctx.fillStyle = theme.textMuted;
        ctx.font = `bold 32px ${theme.font}`;
        ctx.textAlign = 'center';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, y + imgSize/2 + 12);
    }
    ctx.restore();

    // Token image glow border
    ctx.strokeStyle = gradeColor;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, y + imgSize/2, imgSize/2 + 2, 0, Math.PI * 2);
    ctx.stroke();

    // Token name
    ctx.fillStyle = theme.text;
    ctx.font = `bold 36px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText((token.name || 'Unknown').slice(0, 18), leftX + imgSize + 25, y + 35);

    // Symbol
    ctx.fillStyle = theme.textMuted;
    ctx.font = `500 22px ${theme.font}`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, leftX + imgSize + 25, y + 65);

    // === CENTER: K-Score ===
    const centerX = CARD_WIDTH / 2;
    const scoreY = cardY + cardH/2 - 30;

    // Score circle
    const radius = 95;
    ctx.beginPath();
    ctx.arc(centerX, scoreY, radius, 0, Math.PI * 2);
    ctx.fillStyle = theme.backgroundAlt;
    ctx.fill();
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 2;
    ctx.stroke();

    // Progress arc
    const progress = (score / 100) * Math.PI * 2;
    ctx.beginPath();
    ctx.arc(centerX, scoreY, radius - 8, -Math.PI/2, -Math.PI/2 + progress);
    ctx.strokeStyle = gradeColor;
    ctx.lineWidth = 8;
    ctx.lineCap = 'round';
    ctx.stroke();
    ctx.lineCap = 'butt';

    // Score number
    ctx.fillStyle = theme.text;
    ctx.font = `bold 64px ${theme.font}`;
    ctx.textAlign = 'center';
    ctx.fillText(score.toString(), centerX, scoreY + 22);

    // K-SCORE label
    ctx.fillStyle = theme.textDim;
    ctx.font = `600 14px ${theme.font}`;
    ctx.fillText('K-SCORE', centerX, scoreY - 45);

    // Grade badge
    const badgeY = scoreY + radius + 25;
    ctx.fillStyle = gradeColor + '20';
    roundRect(ctx, centerX - 90, badgeY, 180, 36, 18);
    ctx.fill();
    ctx.strokeStyle = gradeColor;
    ctx.lineWidth = 1.5;
    roundRect(ctx, centerX - 90, badgeY, 180, 36, 18);
    ctx.stroke();

    ctx.fillStyle = gradeColor;
    ctx.font = `bold 16px ${theme.font}`;
    ctx.fillText(`${grade.label} · ${grade.credit}`, centerX, badgeY + 24);

    // === RIGHT: Conviction ===
    const rightX = CARD_WIDTH - 320;
    const convY = cardY + 60;

    ctx.fillStyle = theme.text;
    ctx.font = `600 18px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('Top 20 Conviction', rightX, convY);

    // Conviction bar
    const conviction = {
        acc: token.conviction_accumulators || 0,
        hold: token.conviction_holders || 0,
        red: token.conviction_reducers || 0,
        ext: token.conviction_extractors || 0
    };
    const total = conviction.acc + conviction.hold + conviction.red + conviction.ext || 1;

    const barY = convY + 20;
    const barW = 250;
    const barH = 16;

    // Bar background
    ctx.fillStyle = theme.backgroundAlt;
    roundRect(ctx, rightX, barY, barW, barH, 8);
    ctx.fill();

    // Segments
    let currentX = rightX;
    const segments = [
        { val: conviction.acc, color: theme.conviction.accumulator },
        { val: conviction.hold, color: theme.conviction.holder },
        { val: conviction.red, color: theme.conviction.reducer },
        { val: conviction.ext, color: theme.conviction.extractor }
    ];

    segments.forEach((seg, i) => {
        if (seg.val === 0) return;
        const segW = (seg.val / total) * barW;
        ctx.fillStyle = seg.color;

        if (i === 0 && segW > 8) {
            // First segment - left rounded
            ctx.beginPath();
            ctx.moveTo(currentX + 8, barY);
            ctx.lineTo(currentX + segW, barY);
            ctx.lineTo(currentX + segW, barY + barH);
            ctx.lineTo(currentX + 8, barY + barH);
            ctx.arc(currentX + 8, barY + barH/2, 8, Math.PI/2, -Math.PI/2, true);
            ctx.fill();
        } else {
            ctx.fillRect(currentX, barY, segW, barH);
        }
        currentX += segW;
    });

    // Legend
    const legendY = barY + 35;
    const legendItems = [
        { label: 'Diamond', val: conviction.acc, color: theme.conviction.accumulator },
        { label: 'Gold', val: conviction.hold, color: theme.conviction.holder },
        { label: 'Silver', val: conviction.red, color: theme.conviction.reducer },
        { label: 'Rust', val: conviction.ext, color: theme.conviction.extractor }
    ];

    legendItems.forEach((item, i) => {
        const itemY = legendY + i * 28;

        ctx.beginPath();
        ctx.arc(rightX + 8, itemY, 6, 0, Math.PI * 2);
        ctx.fillStyle = item.color;
        ctx.fill();

        ctx.fillStyle = theme.textMuted;
        ctx.font = `500 14px ${theme.font}`;
        ctx.textAlign = 'left';
        ctx.fillText(item.label, rightX + 22, itemY + 5);

        ctx.fillStyle = theme.text;
        ctx.font = `600 14px ${theme.font}`;
        ctx.textAlign = 'right';
        ctx.fillText(item.val.toString(), rightX + barW, itemY + 5);
    });

    // Diamond Hands %
    const dhPct = Math.round(((conviction.acc + conviction.hold) / total) * 100);
    const dhY = legendY + 130;

    ctx.fillStyle = theme.backgroundAlt;
    roundRect(ctx, rightX, dhY, barW, 40, 8);
    ctx.fill();

    ctx.fillStyle = theme.textMuted;
    ctx.font = `500 14px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('Diamond Hands', rightX + 15, dhY + 26);

    ctx.fillStyle = dhPct >= 50 ? theme.conviction.accumulator : theme.conviction.extractor;
    ctx.font = `bold 20px ${theme.font}`;
    ctx.textAlign = 'right';
    ctx.fillText(`${dhPct}%`, rightX + barW - 15, dhY + 27);

    // === FOOTER ===
    const footerY = cardY + cardH - 25;

    ctx.fillStyle = theme.textDim;
    ctx.font = `500 14px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('holdex.io', leftX, footerY);

    ctx.textAlign = 'center';
    ctx.fillText(`${theme.tagline} 🐕‍🦺🔥`, centerX, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], CARD_WIDTH - margin - 50, footerY);
}

// ═══════════════════════════════════════════════════════════════
// ASDF STYLE CARD
// ═══════════════════════════════════════════════════════════════

async function drawAsdfCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeColor = theme.grades[grade.key];

    // Solid burnt background
    ctx.fillStyle = theme.background;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Fire gradient overlay
    const fireGrad = ctx.createLinearGradient(0, 0, 0, CARD_HEIGHT);
    fireGrad.addColorStop(0, 'rgba(251, 146, 60, 0.1)');
    fireGrad.addColorStop(1, 'rgba(124, 45, 18, 0.3)');
    ctx.fillStyle = fireGrad;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // === MAIN CARD (retro box shadow) ===
    const margin = 35;
    const cardX = margin;
    const cardY = margin;
    const cardW = CARD_WIDTH - margin * 2 - theme.shadowOffset;
    const cardH = CARD_HEIGHT - margin * 2 - theme.shadowOffset;

    // Shadow
    ctx.fillStyle = 'rgba(0, 0, 0, 0.4)';
    ctx.fillRect(cardX + theme.shadowOffset, cardY + theme.shadowOffset, cardW, cardH);

    // Card
    ctx.fillStyle = theme.surface;
    ctx.fillRect(cardX, cardY, cardW, cardH);
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 3;
    ctx.strokeRect(cardX, cardY, cardW, cardH);

    // === HEADER BAR ===
    const headerH = 50;
    ctx.fillStyle = theme.backgroundAlt;
    ctx.fillRect(cardX, cardY, cardW, headerH);
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(cardX, cardY + headerH);
    ctx.lineTo(cardX + cardW, cardY + headerH);
    ctx.stroke();

    ctx.fillStyle = theme.text;
    ctx.font = `bold 22px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('📊 K-SCORE TERMINAL', cardX + 20, cardY + 33);

    ctx.textAlign = 'right';
    ctx.font = `16px ${theme.font}`;
    ctx.fillText('🔥 ' + theme.tagline, cardX + cardW - 20, cardY + 33);

    // === LEFT: Token ===
    const leftX = cardX + 40;
    let y = cardY + headerH + 40;

    // Token box
    const tokenBoxW = 300;
    const tokenBoxH = 120;
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 2;
    ctx.strokeRect(leftX, y, tokenBoxW, tokenBoxH);

    // Token image
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 70;
    const imgX = leftX + 15;
    const imgY = y + 25;

    if (tokenImg) {
        ctx.drawImage(tokenImg, imgX, imgY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.backgroundAlt;
        ctx.fillRect(imgX, imgY, imgSize, imgSize);
        ctx.fillStyle = theme.accent;
        ctx.font = `bold 28px ${theme.font}`;
        ctx.textAlign = 'center';
        ctx.fillText(token.symbol?.[0] || '?', imgX + imgSize/2, imgY + imgSize/2 + 10);
    }

    ctx.strokeStyle = gradeColor;
    ctx.lineWidth = 2;
    ctx.strokeRect(imgX - 2, imgY - 2, imgSize + 4, imgSize + 4);

    // Token info
    ctx.fillStyle = theme.text;
    ctx.font = `bold 26px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText((token.name || 'Unknown').slice(0, 12), imgX + imgSize + 20, imgY + 30);

    ctx.fillStyle = theme.accent;
    ctx.font = `20px ${theme.font}`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, imgX + imgSize + 20, imgY + 60);

    // Stats
    y = y + tokenBoxH + 25;
    const stats = [
        ['MCap', token.marketCap ? `$${formatNumber(token.marketCap)}` : 'N/A'],
        ['Holders', token.holders ? formatNumber(token.holders) : 'N/A']
    ];

    stats.forEach((stat, i) => {
        ctx.fillStyle = theme.textDim;
        ctx.font = `14px ${theme.font}`;
        ctx.fillText(stat[0] + ':', leftX, y + i * 25);

        ctx.fillStyle = theme.text;
        ctx.font = `bold 14px ${theme.font}`;
        ctx.fillText(stat[1], leftX + 80, y + i * 25);
    });

    // === CENTER: Big Score ===
    const centerX = CARD_WIDTH / 2;
    const scoreBoxY = cardY + headerH + 40;
    const scoreBoxW = 250;
    const scoreBoxH = 280;

    // Score box with shadow
    ctx.fillStyle = 'rgba(0, 0, 0, 0.3)';
    ctx.fillRect(centerX - scoreBoxW/2 + 4, scoreBoxY + 4, scoreBoxW, scoreBoxH);
    ctx.fillStyle = theme.backgroundAlt;
    ctx.fillRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 3;
    ctx.strokeRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);

    // Score
    ctx.fillStyle = gradeColor;
    ctx.font = `bold 120px ${theme.font}`;
    ctx.textAlign = 'center';
    ctx.fillText(score.toString(), centerX, scoreBoxY + 140);

    // K-SCORE label
    ctx.fillStyle = theme.textDim;
    ctx.font = `bold 16px ${theme.font}`;
    ctx.fillText('K-SCORE', centerX, scoreBoxY + 30);

    // Grade
    ctx.fillStyle = theme.text;
    ctx.font = `bold 24px ${theme.font}`;
    ctx.fillText(`[${grade.icon}] ${grade.label.toUpperCase()}`, centerX, scoreBoxY + 190);

    ctx.fillStyle = theme.textDim;
    ctx.font = `16px ${theme.font}`;
    ctx.fillText(grade.credit, centerX, scoreBoxY + 220);

    // Progress bar
    const progY = scoreBoxY + scoreBoxH - 30;
    const progW = scoreBoxW - 40;
    const progH = 12;

    ctx.fillStyle = theme.background;
    ctx.fillRect(centerX - progW/2, progY, progW, progH);
    ctx.fillStyle = gradeColor;
    ctx.fillRect(centerX - progW/2, progY, (score / 100) * progW, progH);
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 1;
    ctx.strokeRect(centerX - progW/2, progY, progW, progH);

    // === RIGHT: Conviction ===
    const rightX = CARD_WIDTH - 350;
    const convY = cardY + headerH + 40;

    ctx.fillStyle = theme.text;
    ctx.font = `bold 18px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('> CONVICTION_SCAN', rightX, convY);

    const conviction = {
        acc: token.conviction_accumulators || 0,
        hold: token.conviction_holders || 0,
        red: token.conviction_reducers || 0,
        ext: token.conviction_extractors || 0
    };
    const total = conviction.acc + conviction.hold + conviction.red + conviction.ext || 1;

    const items = [
        { label: 'DIAMOND', val: conviction.acc, pct: Math.round((conviction.acc/total)*100), color: theme.conviction.accumulator },
        { label: 'GOLD   ', val: conviction.hold, pct: Math.round((conviction.hold/total)*100), color: theme.conviction.holder },
        { label: 'SILVER ', val: conviction.red, pct: Math.round((conviction.red/total)*100), color: theme.conviction.reducer },
        { label: 'RUST   ', val: conviction.ext, pct: Math.round((conviction.ext/total)*100), color: theme.conviction.extractor }
    ];

    items.forEach((item, i) => {
        const itemY = convY + 30 + i * 45;

        // Bar background
        ctx.fillStyle = theme.background;
        ctx.fillRect(rightX, itemY, 250, 20);

        // Bar fill
        ctx.fillStyle = item.color;
        ctx.fillRect(rightX, itemY, (item.pct / 100) * 250, 20);

        ctx.strokeStyle = theme.surfaceBorder;
        ctx.lineWidth = 1;
        ctx.strokeRect(rightX, itemY, 250, 20);

        // Label
        ctx.fillStyle = theme.text;
        ctx.font = `14px ${theme.font}`;
        ctx.textAlign = 'left';
        ctx.fillText(item.label, rightX, itemY + 38);

        ctx.textAlign = 'right';
        ctx.fillText(`${item.val} (${item.pct}%)`, rightX + 250, itemY + 38);
    });

    // Diamond hands
    const dhPct = Math.round(((conviction.acc + conviction.hold) / total) * 100);
    const dhY = convY + 230;

    ctx.fillStyle = theme.text;
    ctx.font = `bold 16px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('> DIAMOND_HANDS:', rightX, dhY);

    ctx.fillStyle = dhPct >= 50 ? '#22c55e' : '#ef4444';
    ctx.font = `bold 24px ${theme.font}`;
    ctx.fillText(`${dhPct}%`, rightX + 180, dhY);

    // === FOOTER ===
    const footerY = cardY + cardH - 20;
    ctx.fillStyle = theme.textDim;
    ctx.font = `14px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('holdex.io | powered by ASDForecast', cardX + 20, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(`[${new Date().toISOString().split('T')[0]}]`, cardX + cardW - 20, footerY);
}

// ═══════════════════════════════════════════════════════════════
// MINIMAL STYLE CARD
// ═══════════════════════════════════════════════════════════════

async function drawMinimalCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeColor = theme.grades[grade.key];

    // Clean white background
    ctx.fillStyle = theme.background;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Subtle border
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 1;
    ctx.strokeRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // === LEFT: Token + Score ===
    const leftX = 80;
    let y = 80;

    // Token image
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 64;

    ctx.save();
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, y + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.clip();
    if (tokenImg) {
        ctx.drawImage(tokenImg, leftX, y, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.surfaceBorder;
        ctx.fillRect(leftX, y, imgSize, imgSize);
        ctx.fillStyle = theme.textMuted;
        ctx.font = `600 24px ${theme.font}`;
        ctx.textAlign = 'center';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, y + imgSize/2 + 8);
    }
    ctx.restore();

    // Token name
    ctx.fillStyle = theme.text;
    ctx.font = `600 32px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText((token.name || 'Unknown').slice(0, 20), leftX + imgSize + 24, y + 28);

    // Symbol
    ctx.fillStyle = theme.textMuted;
    ctx.font = `500 18px ${theme.font}`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, leftX + imgSize + 24, y + 55);

    // === CENTER: Big Score ===
    const centerY = CARD_HEIGHT / 2;

    // Score
    ctx.fillStyle = theme.text;
    ctx.font = `200 180px ${theme.font}`;
    ctx.textAlign = 'center';
    ctx.fillText(score.toString(), CARD_WIDTH/2, centerY + 50);

    // Grade below
    ctx.fillStyle = gradeColor;
    ctx.font = `600 24px ${theme.font}`;
    ctx.fillText(`${grade.label} · ${grade.credit}`, CARD_WIDTH/2, centerY + 100);

    // K-SCORE label above
    ctx.fillStyle = theme.textDim;
    ctx.font = `500 14px ${theme.font}`;
    ctx.letterSpacing = '4px';
    ctx.fillText('K - S C O R E', CARD_WIDTH/2, centerY - 90);

    // === RIGHT: Minimal conviction ===
    const rightX = CARD_WIDTH - 280;
    const convY = 80;

    const conviction = {
        acc: token.conviction_accumulators || 0,
        hold: token.conviction_holders || 0,
        red: token.conviction_reducers || 0,
        ext: token.conviction_extractors || 0
    };
    const total = conviction.acc + conviction.hold + conviction.red + conviction.ext || 1;
    const dhPct = Math.round(((conviction.acc + conviction.hold) / total) * 100);

    ctx.fillStyle = theme.textDim;
    ctx.font = `500 12px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('CONVICTION', rightX, convY);

    // Simple bar
    const barY = convY + 15;
    const barW = 180;
    const barH = 8;

    ctx.fillStyle = theme.surfaceBorder;
    roundRect(ctx, rightX, barY, barW, barH, 4);
    ctx.fill();

    // Colored segments
    let currentX = rightX;
    const segments = [
        { val: conviction.acc, color: theme.conviction.accumulator },
        { val: conviction.hold, color: theme.conviction.holder },
        { val: conviction.red, color: theme.conviction.reducer },
        { val: conviction.ext, color: theme.conviction.extractor }
    ];

    segments.forEach(seg => {
        if (seg.val === 0) return;
        const segW = (seg.val / total) * barW;
        ctx.fillStyle = seg.color;
        ctx.fillRect(currentX, barY, segW, barH);
        currentX += segW;
    });

    // Diamond hands %
    ctx.fillStyle = theme.text;
    ctx.font = `600 28px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText(`${dhPct}%`, rightX, barY + 45);

    ctx.fillStyle = theme.textMuted;
    ctx.font = `400 14px ${theme.font}`;
    ctx.fillText('diamond hands', rightX + 65, barY + 45);

    // Stats
    const statsY = CARD_HEIGHT - 120;
    const stats = [
        { label: 'Market Cap', value: token.marketCap ? `$${formatNumber(token.marketCap)}` : '—' },
        { label: 'Holders', value: token.holders ? formatNumber(token.holders) : '—' }
    ];

    stats.forEach((stat, i) => {
        const statX = leftX + i * 180;
        ctx.fillStyle = theme.textDim;
        ctx.font = `500 12px ${theme.font}`;
        ctx.textAlign = 'left';
        ctx.fillText(stat.label.toUpperCase(), statX, statsY);

        ctx.fillStyle = theme.text;
        ctx.font = `600 20px ${theme.font}`;
        ctx.fillText(stat.value, statX, statsY + 28);
    });

    // === FOOTER ===
    const footerY = CARD_HEIGHT - 35;

    // Thin line
    ctx.strokeStyle = theme.surfaceBorder;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(leftX, footerY - 15);
    ctx.lineTo(CARD_WIDTH - leftX, footerY - 15);
    ctx.stroke();

    ctx.fillStyle = theme.textDim;
    ctx.font = `400 13px ${theme.font}`;
    ctx.textAlign = 'left';
    ctx.fillText('holdex.io', leftX, footerY);

    ctx.textAlign = 'center';
    ctx.fillText(theme.tagline, CARD_WIDTH/2, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], CARD_WIDTH - leftX, footerY);
}

// ═══════════════════════════════════════════════════════════════
// MAIN GENERATOR
// ═══════════════════════════════════════════════════════════════

/**
 * Generate K-Score card image
 * @param {Object} token - Token data
 * @param {string} style - 'holdex' | 'asdf' | 'minimal'
 * @returns {Buffer} PNG image buffer
 */
async function generateKScoreCard(token, style = 'holdex') {
    const canvas = createCanvas(CARD_WIDTH, CARD_HEIGHT);
    const ctx = canvas.getContext('2d');

    // Get theme
    const theme = THEMES[style] || THEMES.holdex;

    // Draw based on style
    switch (style) {
        case 'asdf':
            await drawAsdfCard(ctx, token, theme);
            break;
        case 'minimal':
            await drawMinimalCard(ctx, token, theme);
            break;
        case 'holdex':
        default:
            await drawHoldexCard(ctx, token, theme);
            break;
    }

    return canvas.toBuffer('image/png');
}

// Legacy support for modes
function styleFromMode(mode) {
    // Map old modes to new styles
    if (mode === 'simple' || mode === 'minimal') return 'minimal';
    if (mode === 'asdf' || mode === 'fire') return 'asdf';
    return 'holdex'; // 'full' or default
}

module.exports = {
    generateKScoreCard,
    getGrade,
    styleFromMode,
    CARD_WIDTH,
    CARD_HEIGHT,
    THEMES
};
