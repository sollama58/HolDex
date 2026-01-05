/**
 * K-Score Card Image Generator v3
 * Premium card designs for social sharing
 *
 * 3 Styles: HolDex, ASDF, Minimal
 */

const { createCanvas, loadImage } = require('canvas');
const axios = require('axios');

// Card dimensions (Twitter card optimal)
const CARD_WIDTH = 1200;
const CARD_HEIGHT = 628;

// Branding
const BRAND_URL = 'alonisthe.dev/holdex';

// ═══════════════════════════════════════════════════════════════
// THEME CONFIGURATIONS
// ═══════════════════════════════════════════════════════════════

const THEMES = {
    holdex: {
        name: 'HolDex',
        bg: {
            primary: '#0a0a0f',
            secondary: '#12121a',
            tertiary: '#1a1a24'
        },
        accent: {
            primary: '#f97316',
            secondary: '#fb923c',
            glow: 'rgba(249, 115, 22, 0.15)'
        },
        text: {
            primary: '#fafafa',
            secondary: '#a1a1aa',
            muted: '#71717a'
        },
        grades: {
            diamond: { color: '#22d3ee', glow: 'rgba(34, 211, 238, 0.3)' },
            platinum: { color: '#a78bfa', glow: 'rgba(167, 139, 250, 0.3)' },
            gold: { color: '#fbbf24', glow: 'rgba(251, 191, 36, 0.3)' },
            silver: { color: '#94a3b8', glow: 'rgba(148, 163, 184, 0.2)' },
            bronze: { color: '#f97316', glow: 'rgba(249, 115, 22, 0.3)' },
            copper: { color: '#ea580c', glow: 'rgba(234, 88, 12, 0.3)' },
            iron: { color: '#6b7280', glow: 'rgba(107, 114, 128, 0.2)' },
            rust: { color: '#ef4444', glow: 'rgba(239, 68, 68, 0.3)' },
            native: { color: '#818cf8', glow: 'rgba(129, 140, 248, 0.3)' }
        },
        conviction: {
            diamond: '#22d3ee',
            gold: '#fbbf24',
            silver: '#94a3b8',
            rust: '#ef4444'
        },
        tagline: 'this is fine.'
    },

    asdf: {
        name: 'ASDF',
        bg: {
            primary: '#1c0a00',
            secondary: '#2d1106',
            tertiary: '#451a03'
        },
        accent: {
            primary: '#fb923c',
            secondary: '#fdba74',
            glow: 'rgba(251, 146, 60, 0.2)'
        },
        text: {
            primary: '#fef3c7',
            secondary: '#fcd34d',
            muted: '#d97706'
        },
        grades: {
            diamond: { color: '#67e8f9', glow: 'rgba(103, 232, 249, 0.4)' },
            platinum: { color: '#c4b5fd', glow: 'rgba(196, 181, 253, 0.4)' },
            gold: { color: '#fde047', glow: 'rgba(253, 224, 71, 0.4)' },
            silver: { color: '#e5e7eb', glow: 'rgba(229, 231, 235, 0.3)' },
            bronze: { color: '#fb923c', glow: 'rgba(251, 146, 60, 0.4)' },
            copper: { color: '#ea580c', glow: 'rgba(234, 88, 12, 0.4)' },
            iron: { color: '#9ca3af', glow: 'rgba(156, 163, 175, 0.3)' },
            rust: { color: '#f87171', glow: 'rgba(248, 113, 113, 0.4)' },
            native: { color: '#a5b4fc', glow: 'rgba(165, 180, 252, 0.4)' }
        },
        conviction: {
            diamond: '#67e8f9',
            gold: '#fde047',
            silver: '#e5e7eb',
            rust: '#f87171'
        },
        tagline: 'the room is on fire.'
    },

    minimal: {
        name: 'Minimal',
        bg: {
            primary: '#ffffff',
            secondary: '#fafafa',
            tertiary: '#f4f4f5'
        },
        accent: {
            primary: '#18181b',
            secondary: '#3f3f46',
            glow: 'rgba(24, 24, 27, 0.05)'
        },
        text: {
            primary: '#09090b',
            secondary: '#52525b',
            muted: '#a1a1aa'
        },
        grades: {
            diamond: { color: '#0891b2', glow: 'rgba(8, 145, 178, 0.1)' },
            platinum: { color: '#7c3aed', glow: 'rgba(124, 58, 237, 0.1)' },
            gold: { color: '#ca8a04', glow: 'rgba(202, 138, 4, 0.1)' },
            silver: { color: '#52525b', glow: 'rgba(82, 82, 91, 0.1)' },
            bronze: { color: '#ea580c', glow: 'rgba(234, 88, 12, 0.1)' },
            copper: { color: '#c2410c', glow: 'rgba(194, 65, 12, 0.1)' },
            iron: { color: '#71717a', glow: 'rgba(113, 113, 122, 0.1)' },
            rust: { color: '#dc2626', glow: 'rgba(220, 38, 38, 0.1)' },
            native: { color: '#4f46e5', glow: 'rgba(79, 70, 229, 0.1)' }
        },
        conviction: {
            diamond: '#0891b2',
            gold: '#ca8a04',
            silver: '#71717a',
            rust: '#dc2626'
        },
        tagline: 'on-chain conviction data'
    }
};

// Grade thresholds
const GRADES = {
    native: { min: null, label: 'Native', credit: 'Infrastructure' },
    diamond: { min: 90, label: 'Diamond', credit: 'A1 Prime' },
    platinum: { min: 80, label: 'Platinum', credit: 'A2 Excellent' },
    gold: { min: 70, label: 'Gold', credit: 'A3 Good' },
    silver: { min: 60, label: 'Silver', credit: 'B1 Fair' },
    bronze: { min: 50, label: 'Bronze', credit: 'B2 Speculative' },
    copper: { min: 40, label: 'Copper', credit: 'B3 Risky' },
    iron: { min: 20, label: 'Iron', credit: 'C High Risk' },
    rust: { min: 0, label: 'Rust', credit: 'D Junk' }
};

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
    return Math.round(num).toString();
}

function roundRect(ctx, x, y, w, h, r) {
    if (r > h / 2) r = h / 2;
    if (r > w / 2) r = w / 2;
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
            headers: { 'User-Agent': 'HolDex-CardGen/3.0' }
        });
        return await loadImage(Buffer.from(response.data));
    } catch {
        return null;
    }
}

// ═══════════════════════════════════════════════════════════════
// HOLDEX STYLE - Premium glassmorphism
// ═══════════════════════════════════════════════════════════════

async function drawHoldexCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // === BACKGROUND ===
    // Deep dark gradient
    const bgGrad = ctx.createLinearGradient(0, 0, CARD_WIDTH, CARD_HEIGHT);
    bgGrad.addColorStop(0, theme.bg.primary);
    bgGrad.addColorStop(0.5, theme.bg.secondary);
    bgGrad.addColorStop(1, theme.bg.primary);
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Ambient glow - top left (orange)
    const glow1 = ctx.createRadialGradient(0, 0, 0, 0, 0, 500);
    glow1.addColorStop(0, 'rgba(249, 115, 22, 0.12)');
    glow1.addColorStop(0.5, 'rgba(249, 115, 22, 0.04)');
    glow1.addColorStop(1, 'transparent');
    ctx.fillStyle = glow1;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Ambient glow - bottom right (grade color)
    const glow2 = ctx.createRadialGradient(CARD_WIDTH, CARD_HEIGHT, 0, CARD_WIDTH, CARD_HEIGHT, 600);
    glow2.addColorStop(0, gradeStyle.glow);
    glow2.addColorStop(0.4, gradeStyle.glow.replace('0.3', '0.1'));
    glow2.addColorStop(1, 'transparent');
    ctx.fillStyle = glow2;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Subtle noise texture effect (grid pattern)
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.015)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= CARD_WIDTH; i += 60) {
        ctx.beginPath();
        ctx.moveTo(i, 0);
        ctx.lineTo(i, CARD_HEIGHT);
        ctx.stroke();
    }
    for (let i = 0; i <= CARD_HEIGHT; i += 60) {
        ctx.beginPath();
        ctx.moveTo(0, i);
        ctx.lineTo(CARD_WIDTH, i);
        ctx.stroke();
    }

    // === MAIN GLASS PANEL ===
    const panelMargin = 32;
    const panelX = panelMargin;
    const panelY = panelMargin;
    const panelW = CARD_WIDTH - panelMargin * 2;
    const panelH = CARD_HEIGHT - panelMargin * 2;

    // Panel shadow
    ctx.shadowColor = 'rgba(0, 0, 0, 0.4)';
    ctx.shadowBlur = 40;
    ctx.shadowOffsetY = 10;
    ctx.fillStyle = 'rgba(18, 18, 26, 0.85)';
    roundRect(ctx, panelX, panelY, panelW, panelH, 24);
    ctx.fill();
    ctx.shadowColor = 'transparent';

    // Panel border (subtle gradient)
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.08)';
    ctx.lineWidth = 1;
    roundRect(ctx, panelX, panelY, panelW, panelH, 24);
    ctx.stroke();

    // === LEFT SECTION: Token Info ===
    const leftX = panelX + 48;
    const topY = panelY + 48;

    // Token image with glow
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 88;

    // Image glow
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = 30;
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2 + 4, 0, Math.PI * 2);
    ctx.fillStyle = gradeStyle.color;
    ctx.fill();
    ctx.shadowColor = 'transparent';

    // Image circle
    ctx.save();
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.clip();
    if (tokenImg) {
        ctx.drawImage(tokenImg, leftX, topY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.bg.tertiary;
        ctx.fillRect(leftX, topY, imgSize, imgSize);
        ctx.fillStyle = theme.text.muted;
        ctx.font = 'bold 36px Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, topY + imgSize/2);
    }
    ctx.restore();

    // Image border
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2 + 1.5, 0, Math.PI * 2);
    ctx.stroke();

    // Token name
    const nameX = leftX + imgSize + 24;
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 32px Arial, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    const displayName = (token.name || 'Unknown').slice(0, 16);
    ctx.fillText(displayName, nameX, topY + 12);

    // Token symbol
    ctx.fillStyle = theme.text.secondary;
    ctx.font = '500 20px Arial, sans-serif';
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, nameX, topY + 52);

    // Stats row
    const statsY = topY + imgSize + 32;
    const stats = [
        { label: 'MCap', value: token.marketCap ? `$${formatNumber(token.marketCap)}` : '—' },
        { label: 'Holders', value: token.holders ? formatNumber(token.holders) : '—' }
    ];

    stats.forEach((stat, i) => {
        const statX = leftX + i * 140;
        ctx.fillStyle = theme.text.muted;
        ctx.font = '500 13px Arial, sans-serif';
        ctx.textBaseline = 'top';
        ctx.fillText(stat.label.toUpperCase(), statX, statsY);
        ctx.fillStyle = theme.text.primary;
        ctx.font = 'bold 18px Arial, sans-serif';
        ctx.fillText(stat.value, statX, statsY + 20);
    });

    // === CENTER: K-Score Circle ===
    const centerX = CARD_WIDTH / 2;
    const centerY = panelY + panelH / 2;
    const radius = 100;

    // Outer glow
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = 50;
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius + 8, 0, Math.PI * 2);
    ctx.fillStyle = theme.bg.tertiary;
    ctx.fill();
    ctx.shadowColor = 'transparent';

    // Circle background
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
    ctx.fillStyle = theme.bg.primary;
    ctx.fill();

    // Progress arc background
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius - 12, 0, Math.PI * 2);
    ctx.strokeStyle = theme.bg.tertiary;
    ctx.lineWidth = 10;
    ctx.stroke();

    // Progress arc
    const progress = (score / 100) * Math.PI * 2;
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius - 12, -Math.PI/2, -Math.PI/2 + progress);
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = 10;
    ctx.lineCap = 'round';
    ctx.stroke();
    ctx.lineCap = 'butt';

    // Score number
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 72px Arial, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(score.toString(), centerX, centerY - 5);

    // K-SCORE label
    ctx.fillStyle = theme.text.muted;
    ctx.font = '600 12px Arial, sans-serif';
    ctx.letterSpacing = '2px';
    ctx.fillText('K-SCORE', centerX, centerY - 50);

    // Grade badge
    const badgeY = centerY + radius + 24;
    const badgeW = 180;
    const badgeH = 36;

    // Badge background with glow
    ctx.fillStyle = gradeStyle.glow;
    roundRect(ctx, centerX - badgeW/2, badgeY, badgeW, badgeH, badgeH/2);
    ctx.fill();

    // Badge border
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = 1.5;
    roundRect(ctx, centerX - badgeW/2, badgeY, badgeW, badgeH, badgeH/2);
    ctx.stroke();

    // Badge text
    ctx.fillStyle = gradeStyle.color;
    ctx.font = 'bold 14px Arial, sans-serif';
    ctx.textBaseline = 'middle';
    ctx.fillText(`${grade.label} · ${grade.credit}`, centerX, badgeY + badgeH/2);

    // === RIGHT: Conviction ===
    const rightX = CARD_WIDTH - 340;
    const convY = topY;

    // Section title
    ctx.fillStyle = theme.text.primary;
    ctx.font = '600 16px Arial, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('Top 20 Conviction', rightX, convY);

    // Conviction data
    const conviction = {
        diamond: token.conviction_accumulators || 0,
        gold: token.conviction_holders || 0,
        silver: token.conviction_reducers || 0,
        rust: token.conviction_extractors || 0
    };
    const total = conviction.diamond + conviction.gold + conviction.silver + conviction.rust || 1;

    // Conviction bar
    const barY = convY + 30;
    const barW = 260;
    const barH = 14;

    // Bar background
    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, barY, barW, barH, barH/2);
    ctx.fill();

    // Bar segments
    let currentX = rightX;
    const segments = [
        { val: conviction.diamond, color: theme.conviction.diamond },
        { val: conviction.gold, color: theme.conviction.gold },
        { val: conviction.silver, color: theme.conviction.silver },
        { val: conviction.rust, color: theme.conviction.rust }
    ];

    ctx.save();
    roundRect(ctx, rightX, barY, barW, barH, barH/2);
    ctx.clip();
    segments.forEach(seg => {
        if (seg.val === 0) return;
        const segW = (seg.val / total) * barW;
        ctx.fillStyle = seg.color;
        ctx.fillRect(currentX, barY, segW + 1, barH);
        currentX += segW;
    });
    ctx.restore();

    // Legend
    const legendY = barY + 30;
    const legendItems = [
        { label: 'Diamond', val: conviction.diamond, color: theme.conviction.diamond },
        { label: 'Gold', val: conviction.gold, color: theme.conviction.gold },
        { label: 'Silver', val: conviction.silver, color: theme.conviction.silver },
        { label: 'Rust', val: conviction.rust, color: theme.conviction.rust }
    ];

    legendItems.forEach((item, i) => {
        const itemY = legendY + i * 30;

        // Dot
        ctx.beginPath();
        ctx.arc(rightX + 6, itemY + 8, 5, 0, Math.PI * 2);
        ctx.fillStyle = item.color;
        ctx.fill();

        // Label
        ctx.fillStyle = theme.text.secondary;
        ctx.font = '500 14px Arial, sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(item.label, rightX + 20, itemY + 8);

        // Value
        ctx.fillStyle = theme.text.primary;
        ctx.font = '600 14px Arial, sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText(item.val.toString(), rightX + barW, itemY + 8);
    });

    // Diamond Hands box
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);
    const dhY = legendY + 140;

    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, dhY, barW, 44, 10);
    ctx.fill();

    ctx.fillStyle = theme.text.secondary;
    ctx.font = '500 13px Arial, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText('Diamond Hands', rightX + 14, dhY + 22);

    ctx.fillStyle = dhPct >= 50 ? theme.conviction.diamond : theme.conviction.rust;
    ctx.font = 'bold 22px Arial, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(`${dhPct}%`, rightX + barW - 14, dhY + 22);

    // === FOOTER ===
    const footerY = panelY + panelH - 28;

    ctx.fillStyle = theme.text.muted;
    ctx.font = '500 13px Arial, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(BRAND_URL, leftX, footerY);

    ctx.textAlign = 'center';
    ctx.fillText(`${theme.tagline} 🐕‍🦺🔥`, centerX, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], CARD_WIDTH - panelMargin - 48, footerY);
}

// ═══════════════════════════════════════════════════════════════
// ASDF STYLE - Fire terminal
// ═══════════════════════════════════════════════════════════════

async function drawAsdfCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // === BACKGROUND ===
    // Deep burnt gradient
    const bgGrad = ctx.createLinearGradient(0, 0, 0, CARD_HEIGHT);
    bgGrad.addColorStop(0, theme.bg.secondary);
    bgGrad.addColorStop(0.5, theme.bg.primary);
    bgGrad.addColorStop(1, '#0f0503');
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Fire glow from bottom
    const fireGlow = ctx.createRadialGradient(CARD_WIDTH/2, CARD_HEIGHT + 100, 0, CARD_WIDTH/2, CARD_HEIGHT, 400);
    fireGlow.addColorStop(0, 'rgba(251, 146, 60, 0.25)');
    fireGlow.addColorStop(0.5, 'rgba(234, 88, 12, 0.1)');
    fireGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = fireGlow;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Scanlines effect
    ctx.fillStyle = 'rgba(0, 0, 0, 0.15)';
    for (let i = 0; i < CARD_HEIGHT; i += 4) {
        ctx.fillRect(0, i, CARD_WIDTH, 2);
    }

    // === MAIN PANEL ===
    const margin = 28;
    const panelX = margin;
    const panelY = margin;
    const panelW = CARD_WIDTH - margin * 2;
    const panelH = CARD_HEIGHT - margin * 2;

    // Panel shadow (offset)
    ctx.fillStyle = 'rgba(0, 0, 0, 0.5)';
    ctx.fillRect(panelX + 6, panelY + 6, panelW, panelH);

    // Panel background
    ctx.fillStyle = theme.bg.tertiary;
    ctx.fillRect(panelX, panelY, panelW, panelH);

    // Panel border
    ctx.strokeStyle = theme.accent.primary;
    ctx.lineWidth = 3;
    ctx.strokeRect(panelX, panelY, panelW, panelH);

    // === HEADER BAR ===
    const headerH = 48;
    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(panelX, panelY, panelW, headerH);

    // Header line
    ctx.strokeStyle = theme.accent.primary;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(panelX, panelY + headerH);
    ctx.lineTo(panelX + panelW, panelY + headerH);
    ctx.stroke();

    // Header text
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 20px Courier, monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText('> K-SCORE_TERMINAL v3.0', panelX + 20, panelY + headerH/2);

    ctx.textAlign = 'right';
    ctx.fillStyle = theme.accent.primary;
    ctx.font = '16px Courier, monospace';
    ctx.fillText(`🔥 ${theme.tagline}`, panelX + panelW - 20, panelY + headerH/2);

    // === LEFT: Token Info ===
    const leftX = panelX + 32;
    const contentY = panelY + headerH + 28;

    // Token box
    const tokenBoxW = 280;
    const tokenBoxH = 100;

    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(leftX, contentY, tokenBoxW, tokenBoxH);
    ctx.strokeStyle = theme.accent.primary;
    ctx.lineWidth = 2;
    ctx.strokeRect(leftX, contentY, tokenBoxW, tokenBoxH);

    // Token image
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 64;
    const imgX = leftX + 16;
    const imgY = contentY + 18;

    if (tokenImg) {
        ctx.drawImage(tokenImg, imgX, imgY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.bg.primary;
        ctx.fillRect(imgX, imgY, imgSize, imgSize);
        ctx.fillStyle = theme.accent.primary;
        ctx.font = 'bold 28px Courier, monospace';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', imgX + imgSize/2, imgY + imgSize/2);
    }

    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = 2;
    ctx.strokeRect(imgX - 2, imgY - 2, imgSize + 4, imgSize + 4);

    // Token info
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 22px Courier, monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText((token.name || 'Unknown').slice(0, 10), imgX + imgSize + 16, imgY + 8);

    ctx.fillStyle = theme.accent.secondary;
    ctx.font = '18px Courier, monospace';
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, imgX + imgSize + 16, imgY + 38);

    // Stats
    const statsY = contentY + tokenBoxH + 20;
    ctx.fillStyle = theme.text.muted;
    ctx.font = '14px Courier, monospace';
    ctx.fillText('MCAP:', leftX, statsY);
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 14px Courier, monospace';
    ctx.fillText(token.marketCap ? `$${formatNumber(token.marketCap)}` : 'N/A', leftX + 60, statsY);

    ctx.fillStyle = theme.text.muted;
    ctx.font = '14px Courier, monospace';
    ctx.fillText('HOLDERS:', leftX, statsY + 22);
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 14px Courier, monospace';
    ctx.fillText(token.holders ? formatNumber(token.holders) : 'N/A', leftX + 90, statsY + 22);

    // === CENTER: Score Display ===
    const centerX = CARD_WIDTH / 2;
    const scoreBoxY = contentY;
    const scoreBoxW = 220;
    const scoreBoxH = 260;

    // Score box shadow
    ctx.fillStyle = 'rgba(0, 0, 0, 0.4)';
    ctx.fillRect(centerX - scoreBoxW/2 + 5, scoreBoxY + 5, scoreBoxW, scoreBoxH);

    // Score box
    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);
    ctx.strokeStyle = theme.accent.primary;
    ctx.lineWidth = 3;
    ctx.strokeRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);

    // K-SCORE label
    ctx.fillStyle = theme.text.muted;
    ctx.font = 'bold 14px Courier, monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText('[ K-SCORE ]', centerX, scoreBoxY + 16);

    // Big score with glow
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = 30;
    ctx.fillStyle = gradeStyle.color;
    ctx.font = 'bold 100px Courier, monospace';
    ctx.textBaseline = 'middle';
    ctx.fillText(score.toString(), centerX, scoreBoxY + 110);
    ctx.shadowColor = 'transparent';

    // Grade
    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 20px Courier, monospace';
    ctx.fillText(`[${grade.label.toUpperCase()}]`, centerX, scoreBoxY + 175);

    ctx.fillStyle = theme.text.muted;
    ctx.font = '14px Courier, monospace';
    ctx.fillText(grade.credit, centerX, scoreBoxY + 200);

    // Progress bar
    const progY = scoreBoxY + scoreBoxH - 28;
    const progW = scoreBoxW - 32;
    const progH = 10;

    ctx.fillStyle = theme.bg.primary;
    ctx.fillRect(centerX - progW/2, progY, progW, progH);
    ctx.fillStyle = gradeStyle.color;
    ctx.fillRect(centerX - progW/2, progY, (score / 100) * progW, progH);
    ctx.strokeStyle = theme.accent.primary;
    ctx.lineWidth = 1;
    ctx.strokeRect(centerX - progW/2, progY, progW, progH);

    // === RIGHT: Conviction ===
    const rightX = CARD_WIDTH - 340;
    const convY = contentY;

    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 16px Courier, monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('> CONVICTION_SCAN:', rightX, convY);

    const conviction = {
        diamond: token.conviction_accumulators || 0,
        gold: token.conviction_holders || 0,
        silver: token.conviction_reducers || 0,
        rust: token.conviction_extractors || 0
    };
    const total = conviction.diamond + conviction.gold + conviction.silver + conviction.rust || 1;

    const items = [
        { label: 'DIAMOND', val: conviction.diamond, pct: Math.round((conviction.diamond/total)*100), color: theme.conviction.diamond },
        { label: 'GOLD', val: conviction.gold, pct: Math.round((conviction.gold/total)*100), color: theme.conviction.gold },
        { label: 'SILVER', val: conviction.silver, pct: Math.round((conviction.silver/total)*100), color: theme.conviction.silver },
        { label: 'RUST', val: conviction.rust, pct: Math.round((conviction.rust/total)*100), color: theme.conviction.rust }
    ];

    items.forEach((item, i) => {
        const itemY = convY + 30 + i * 50;

        // Bar background
        ctx.fillStyle = theme.bg.primary;
        ctx.fillRect(rightX, itemY, 240, 18);

        // Bar fill
        ctx.fillStyle = item.color;
        ctx.fillRect(rightX, itemY, (item.pct / 100) * 240, 18);

        ctx.strokeStyle = theme.accent.primary;
        ctx.lineWidth = 1;
        ctx.strokeRect(rightX, itemY, 240, 18);

        // Label
        ctx.fillStyle = theme.text.primary;
        ctx.font = '13px Courier, monospace';
        ctx.textBaseline = 'top';
        ctx.fillText(`${item.label}: ${item.val} (${item.pct}%)`, rightX, itemY + 22);
    });

    // Diamond hands
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);
    const dhY = convY + 240;

    ctx.fillStyle = theme.text.primary;
    ctx.font = 'bold 14px Courier, monospace';
    ctx.fillText('> DIAMOND_HANDS:', rightX, dhY);

    ctx.fillStyle = dhPct >= 50 ? '#22c55e' : '#ef4444';
    ctx.font = 'bold 22px Courier, monospace';
    ctx.fillText(`${dhPct}%`, rightX + 180, dhY);

    // === FOOTER ===
    const footerY = panelY + panelH - 22;
    ctx.fillStyle = theme.text.muted;
    ctx.font = '13px Courier, monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(`[${BRAND_URL}]`, panelX + 20, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(`[${new Date().toISOString().split('T')[0]}]`, panelX + panelW - 20, footerY);
}

// ═══════════════════════════════════════════════════════════════
// MINIMAL STYLE - Clean elegance
// ═══════════════════════════════════════════════════════════════

async function drawMinimalCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // === BACKGROUND ===
    ctx.fillStyle = theme.bg.primary;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // Subtle gradient overlay
    const subtleGrad = ctx.createLinearGradient(0, 0, 0, CARD_HEIGHT);
    subtleGrad.addColorStop(0, 'rgba(0, 0, 0, 0)');
    subtleGrad.addColorStop(1, 'rgba(0, 0, 0, 0.02)');
    ctx.fillStyle = subtleGrad;
    ctx.fillRect(0, 0, CARD_WIDTH, CARD_HEIGHT);

    // === LEFT: Token ===
    const leftX = 72;
    const topY = 72;

    // Token image
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = 56;

    ctx.save();
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.clip();
    if (tokenImg) {
        ctx.drawImage(tokenImg, leftX, topY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.bg.tertiary;
        ctx.fillRect(leftX, topY, imgSize, imgSize);
        ctx.fillStyle = theme.text.muted;
        ctx.font = '600 22px -apple-system, BlinkMacSystemFont, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, topY + imgSize/2);
    }
    ctx.restore();

    // Token name
    ctx.fillStyle = theme.text.primary;
    ctx.font = '600 28px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText((token.name || 'Unknown').slice(0, 18), leftX + imgSize + 20, topY + 6);

    // Symbol
    ctx.fillStyle = theme.text.muted;
    ctx.font = '500 16px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, leftX + imgSize + 20, topY + 38);

    // === CENTER: Score ===
    const centerX = CARD_WIDTH / 2;
    const centerY = CARD_HEIGHT / 2;

    // Big score
    ctx.fillStyle = theme.text.primary;
    ctx.font = '100 160px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(score.toString(), centerX, centerY - 10);

    // K-SCORE label
    ctx.fillStyle = theme.text.muted;
    ctx.font = '500 13px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.letterSpacing = '3px';
    ctx.fillText('K - S C O R E', centerX, centerY - 100);

    // Grade
    ctx.fillStyle = gradeStyle.color;
    ctx.font = '600 20px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.fillText(`${grade.label} · ${grade.credit}`, centerX, centerY + 90);

    // === RIGHT: Conviction ===
    const rightX = CARD_WIDTH - 260;
    const convY = 72;

    // Label
    ctx.fillStyle = theme.text.muted;
    ctx.font = '500 11px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('CONVICTION', rightX, convY);

    // Simple bar
    const conviction = {
        diamond: token.conviction_accumulators || 0,
        gold: token.conviction_holders || 0,
        silver: token.conviction_reducers || 0,
        rust: token.conviction_extractors || 0
    };
    const total = conviction.diamond + conviction.gold + conviction.silver + conviction.rust || 1;
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);

    const barY = convY + 20;
    const barW = 160;
    const barH = 6;

    // Bar background
    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, barY, barW, barH, 3);
    ctx.fill();

    // Segments
    ctx.save();
    roundRect(ctx, rightX, barY, barW, barH, 3);
    ctx.clip();
    let currentX = rightX;
    [
        { val: conviction.diamond, color: theme.conviction.diamond },
        { val: conviction.gold, color: theme.conviction.gold },
        { val: conviction.silver, color: theme.conviction.silver },
        { val: conviction.rust, color: theme.conviction.rust }
    ].forEach(seg => {
        if (seg.val === 0) return;
        const segW = (seg.val / total) * barW;
        ctx.fillStyle = seg.color;
        ctx.fillRect(currentX, barY, segW + 1, barH);
        currentX += segW;
    });
    ctx.restore();

    // Diamond hands
    ctx.fillStyle = theme.text.primary;
    ctx.font = '700 26px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textBaseline = 'top';
    ctx.fillText(`${dhPct}%`, rightX, barY + 18);

    ctx.fillStyle = theme.text.muted;
    ctx.font = '400 13px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.fillText('diamond hands', rightX + 60, barY + 24);

    // === BOTTOM STATS ===
    const statsY = CARD_HEIGHT - 130;
    const stats = [
        { label: 'MARKET CAP', value: token.marketCap ? `$${formatNumber(token.marketCap)}` : '—' },
        { label: 'HOLDERS', value: token.holders ? formatNumber(token.holders) : '—' }
    ];

    stats.forEach((stat, i) => {
        const statX = leftX + i * 160;
        ctx.fillStyle = theme.text.muted;
        ctx.font = '500 11px -apple-system, BlinkMacSystemFont, sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        ctx.fillText(stat.label, statX, statsY);

        ctx.fillStyle = theme.text.primary;
        ctx.font = '600 18px -apple-system, BlinkMacSystemFont, sans-serif';
        ctx.fillText(stat.value, statX, statsY + 20);
    });

    // === FOOTER ===
    // Thin line
    ctx.strokeStyle = theme.bg.tertiary;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(leftX, CARD_HEIGHT - 52);
    ctx.lineTo(CARD_WIDTH - leftX, CARD_HEIGHT - 52);
    ctx.stroke();

    ctx.fillStyle = theme.text.muted;
    ctx.font = '400 12px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(BRAND_URL, leftX, CARD_HEIGHT - 32);

    ctx.textAlign = 'center';
    ctx.fillText(theme.tagline, centerX, CARD_HEIGHT - 32);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], CARD_WIDTH - leftX, CARD_HEIGHT - 32);
}

// ═══════════════════════════════════════════════════════════════
// MAIN GENERATOR
// ═══════════════════════════════════════════════════════════════

async function generateKScoreCard(token, style = 'holdex') {
    const canvas = createCanvas(CARD_WIDTH, CARD_HEIGHT);
    const ctx = canvas.getContext('2d');

    const theme = THEMES[style] || THEMES.holdex;

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

function styleFromMode(mode) {
    if (mode === 'simple' || mode === 'minimal') return 'minimal';
    if (mode === 'asdf' || mode === 'fire') return 'asdf';
    return 'holdex';
}

module.exports = {
    generateKScoreCard,
    getGrade,
    styleFromMode,
    CARD_WIDTH,
    CARD_HEIGHT,
    THEMES
};
