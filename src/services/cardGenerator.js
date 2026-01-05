/**
 * K-Score Card Image Generator v4
 * Premium card designs for social sharing
 *
 * 3 Styles: HolDex, ASDF, Minimal (dark)
 * Rendered at 2x for crisp output
 */

const { createCanvas, loadImage } = require('canvas');
const axios = require('axios');

// Card dimensions (Twitter optimal) - render at 2x for sharpness
const SCALE = 2;
const CARD_WIDTH = 1200;
const CARD_HEIGHT = 628;
const RENDER_WIDTH = CARD_WIDTH * SCALE;
const RENDER_HEIGHT = CARD_HEIGHT * SCALE;

// Branding
const BRAND_URL = 'alonisthe.dev/holdex';

// ═══════════════════════════════════════════════════════════════
// THEME CONFIGURATIONS
// ═══════════════════════════════════════════════════════════════

const THEMES = {
    // HolDex: Premium glassmorphism, dark blue-black
    holdex: {
        name: 'HolDex',
        bg: {
            primary: '#08080c',
            secondary: '#0e0e14',
            tertiary: '#16161e',
            card: 'rgba(14, 14, 20, 0.9)'
        },
        accent: '#f97316',
        text: {
            primary: '#fafafa',
            secondary: '#a1a1aa',
            muted: '#52525b'
        },
        grades: {
            diamond: { color: '#22d3ee', glow: 'rgba(34, 211, 238, 0.35)' },
            platinum: { color: '#a78bfa', glow: 'rgba(167, 139, 250, 0.35)' },
            gold: { color: '#fbbf24', glow: 'rgba(251, 191, 36, 0.35)' },
            silver: { color: '#94a3b8', glow: 'rgba(148, 163, 184, 0.25)' },
            bronze: { color: '#fb923c', glow: 'rgba(251, 146, 60, 0.35)' },
            copper: { color: '#ea580c', glow: 'rgba(234, 88, 12, 0.35)' },
            iron: { color: '#6b7280', glow: 'rgba(107, 114, 128, 0.25)' },
            rust: { color: '#ef4444', glow: 'rgba(239, 68, 68, 0.35)' },
            native: { color: '#818cf8', glow: 'rgba(129, 140, 248, 0.35)' }
        },
        conviction: {
            diamond: '#22d3ee',
            gold: '#fbbf24',
            silver: '#71717a',
            rust: '#ef4444'
        },
        tagline: 'this is fine.'
    },

    // ASDF: Fire terminal, burnt tones
    asdf: {
        name: 'ASDF',
        bg: {
            primary: '#0c0402',
            secondary: '#1a0a04',
            tertiary: '#2d1408',
            card: '#1f0d05'
        },
        accent: '#fb923c',
        text: {
            primary: '#fef3c7',
            secondary: '#fcd34d',
            muted: '#b45309'
        },
        grades: {
            diamond: { color: '#67e8f9', glow: 'rgba(103, 232, 249, 0.45)' },
            platinum: { color: '#c4b5fd', glow: 'rgba(196, 181, 253, 0.45)' },
            gold: { color: '#fde047', glow: 'rgba(253, 224, 71, 0.45)' },
            silver: { color: '#e5e7eb', glow: 'rgba(229, 231, 235, 0.35)' },
            bronze: { color: '#fb923c', glow: 'rgba(251, 146, 60, 0.45)' },
            copper: { color: '#ea580c', glow: 'rgba(234, 88, 12, 0.45)' },
            iron: { color: '#9ca3af', glow: 'rgba(156, 163, 175, 0.35)' },
            rust: { color: '#f87171', glow: 'rgba(248, 113, 113, 0.45)' },
            native: { color: '#a5b4fc', glow: 'rgba(165, 180, 252, 0.45)' }
        },
        conviction: {
            diamond: '#67e8f9',
            gold: '#fde047',
            silver: '#d6d3d1',
            rust: '#f87171'
        },
        tagline: 'the room is on fire.'
    },

    // Minimal: Dark, clean, institutional - high contrast for readability
    // Philosophy: Swiss design meets trading terminal
    minimal: {
        name: 'Minimal',
        bg: {
            primary: '#09090b',      // Near black with warmth
            secondary: '#0c0c0e',
            tertiary: '#18181b',     // Zinc-900
            card: '#131316'          // Lifted panel
        },
        accent: '#e4e4e7',           // Zinc-200
        text: {
            primary: '#fafafa',      // Zinc-50 - max contrast
            secondary: '#a1a1aa',    // Zinc-400 - readable
            muted: '#71717a'         // Zinc-500 - still visible (5.3:1 ratio)
        },
        grades: {
            diamond: { color: '#22d3ee', glow: 'rgba(34, 211, 238, 0.25)' },
            platinum: { color: '#a78bfa', glow: 'rgba(167, 139, 250, 0.25)' },
            gold: { color: '#fbbf24', glow: 'rgba(251, 191, 36, 0.25)' },
            silver: { color: '#a1a1aa', glow: 'rgba(161, 161, 170, 0.2)' },
            bronze: { color: '#fb923c', glow: 'rgba(251, 146, 60, 0.25)' },
            copper: { color: '#f97316', glow: 'rgba(249, 115, 22, 0.25)' },
            iron: { color: '#71717a', glow: 'rgba(113, 113, 122, 0.2)' },
            rust: { color: '#f87171', glow: 'rgba(248, 113, 113, 0.25)' },
            native: { color: '#818cf8', glow: 'rgba(129, 140, 248, 0.25)' }
        },
        conviction: {
            diamond: '#22d3ee',
            gold: '#fbbf24',
            silver: '#a1a1aa',       // Visible silver
            rust: '#f87171'
        },
        tagline: 'on-chain conviction'
    }
};

// Grade thresholds
const GRADES = {
    native: { label: 'Native', credit: 'Infrastructure' },
    diamond: { label: 'Diamond', credit: 'A1 Prime' },
    platinum: { label: 'Platinum', credit: 'A2 Excellent' },
    gold: { label: 'Gold', credit: 'A3 Good' },
    silver: { label: 'Silver', credit: 'B1 Fair' },
    bronze: { label: 'Bronze', credit: 'B2 Speculative' },
    copper: { label: 'Copper', credit: 'B3 Risky' },
    iron: { label: 'Iron', credit: 'C High Risk' },
    rust: { label: 'Rust', credit: 'D Junk' }
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

function s(val) { return val * SCALE; } // Scale helper

function roundRect(ctx, x, y, w, h, r) {
    r = Math.min(r, h / 2, w / 2);
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
            headers: { 'User-Agent': 'HolDex-CardGen/4.0' }
        });
        return await loadImage(Buffer.from(response.data));
    } catch {
        return null;
    }
}

// ═══════════════════════════════════════════════════════════════
// HOLDEX STYLE
// ═══════════════════════════════════════════════════════════════

async function drawHoldexCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // Background gradient
    const bgGrad = ctx.createLinearGradient(0, 0, RENDER_WIDTH, RENDER_HEIGHT);
    bgGrad.addColorStop(0, theme.bg.primary);
    bgGrad.addColorStop(0.5, theme.bg.secondary);
    bgGrad.addColorStop(1, theme.bg.primary);
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Ambient glow - top left orange
    const glow1 = ctx.createRadialGradient(0, 0, 0, 0, 0, s(450));
    glow1.addColorStop(0, 'rgba(249, 115, 22, 0.15)');
    glow1.addColorStop(0.6, 'rgba(249, 115, 22, 0.03)');
    glow1.addColorStop(1, 'transparent');
    ctx.fillStyle = glow1;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Ambient glow - bottom right grade color
    const glow2 = ctx.createRadialGradient(RENDER_WIDTH, RENDER_HEIGHT, 0, RENDER_WIDTH, RENDER_HEIGHT, s(500));
    glow2.addColorStop(0, gradeStyle.glow);
    glow2.addColorStop(0.5, gradeStyle.glow.replace(/[\d.]+\)$/, '0.08)'));
    glow2.addColorStop(1, 'transparent');
    ctx.fillStyle = glow2;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Subtle grid
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.02)';
    ctx.lineWidth = s(1);
    for (let i = 0; i <= RENDER_WIDTH; i += s(50)) {
        ctx.beginPath();
        ctx.moveTo(i, 0);
        ctx.lineTo(i, RENDER_HEIGHT);
        ctx.stroke();
    }
    for (let i = 0; i <= RENDER_HEIGHT; i += s(50)) {
        ctx.beginPath();
        ctx.moveTo(0, i);
        ctx.lineTo(RENDER_WIDTH, i);
        ctx.stroke();
    }

    // Main panel
    const m = s(28);
    const panelX = m, panelY = m;
    const panelW = RENDER_WIDTH - m * 2, panelH = RENDER_HEIGHT - m * 2;

    ctx.shadowColor = 'rgba(0, 0, 0, 0.5)';
    ctx.shadowBlur = s(40);
    ctx.shadowOffsetY = s(8);
    ctx.fillStyle = theme.bg.card;
    roundRect(ctx, panelX, panelY, panelW, panelH, s(20));
    ctx.fill();
    ctx.shadowColor = 'transparent';

    ctx.strokeStyle = 'rgba(255, 255, 255, 0.06)';
    ctx.lineWidth = s(1);
    roundRect(ctx, panelX, panelY, panelW, panelH, s(20));
    ctx.stroke();

    // LEFT: Token
    const leftX = panelX + s(44);
    const topY = panelY + s(44);

    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = s(80);

    // Token glow
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = s(25);
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2 + s(3), 0, Math.PI * 2);
    ctx.fillStyle = gradeStyle.color;
    ctx.fill();
    ctx.shadowColor = 'transparent';

    // Token image
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
        ctx.font = `bold ${s(32)}px Arial, sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, topY + imgSize/2);
    }
    ctx.restore();

    // Token border
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = s(2.5);
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2 + s(1), 0, Math.PI * 2);
    ctx.stroke();

    // Token name
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(30)}px Arial, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText((token.name || 'Unknown').slice(0, 16), leftX + imgSize + s(20), topY + s(10));

    // Symbol
    ctx.fillStyle = theme.text.secondary;
    ctx.font = `500 ${s(18)}px Arial, sans-serif`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, leftX + imgSize + s(20), topY + s(48));

    // Stats
    const statsY = topY + imgSize + s(28);
    const stats = [
        { label: 'MCAP', value: token.marketCap ? `$${formatNumber(token.marketCap)}` : '—' },
        { label: 'HOLDERS', value: token.holders ? formatNumber(token.holders) : '—' }
    ];
    stats.forEach((stat, i) => {
        const statX = leftX + i * s(130);
        ctx.fillStyle = theme.text.muted;
        ctx.font = `500 ${s(11)}px Arial, sans-serif`;
        ctx.fillText(stat.label, statX, statsY);
        ctx.fillStyle = theme.text.primary;
        ctx.font = `600 ${s(16)}px Arial, sans-serif`;
        ctx.fillText(stat.value, statX, statsY + s(18));
    });

    // CENTER: K-Score
    const centerX = RENDER_WIDTH / 2;
    const centerY = panelY + panelH / 2;
    const radius = s(90);

    // Circle glow
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = s(45);
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius + s(6), 0, Math.PI * 2);
    ctx.fillStyle = theme.bg.tertiary;
    ctx.fill();
    ctx.shadowColor = 'transparent';

    // Circle bg
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
    ctx.fillStyle = theme.bg.primary;
    ctx.fill();

    // Progress track
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius - s(10), 0, Math.PI * 2);
    ctx.strokeStyle = theme.bg.tertiary;
    ctx.lineWidth = s(8);
    ctx.stroke();

    // Progress arc
    const progress = (score / 100) * Math.PI * 2;
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius - s(10), -Math.PI/2, -Math.PI/2 + progress);
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = s(8);
    ctx.lineCap = 'round';
    ctx.stroke();
    ctx.lineCap = 'butt';

    // Score
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(64)}px Arial, sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(score.toString(), centerX, centerY - s(2));

    // K-SCORE label
    ctx.fillStyle = theme.text.muted;
    ctx.font = `600 ${s(11)}px Arial, sans-serif`;
    ctx.fillText('K-SCORE', centerX, centerY - s(44));

    // Grade badge
    const badgeY = centerY + radius + s(20);
    const badgeW = s(170), badgeH = s(32);

    ctx.fillStyle = gradeStyle.glow;
    roundRect(ctx, centerX - badgeW/2, badgeY, badgeW, badgeH, badgeH/2);
    ctx.fill();
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = s(1.5);
    roundRect(ctx, centerX - badgeW/2, badgeY, badgeW, badgeH, badgeH/2);
    ctx.stroke();

    ctx.fillStyle = gradeStyle.color;
    ctx.font = `bold ${s(12)}px Arial, sans-serif`;
    ctx.textBaseline = 'middle';
    ctx.fillText(`${grade.label} · ${grade.credit}`, centerX, badgeY + badgeH/2);

    // RIGHT: Conviction
    const rightX = RENDER_WIDTH - s(320);
    const convY = topY;

    ctx.fillStyle = theme.text.primary;
    ctx.font = `600 ${s(14)}px Arial, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('Top 20 Conviction', rightX, convY);

    const conviction = {
        diamond: token.conviction_accumulators || 0,
        gold: token.conviction_holders || 0,
        silver: token.conviction_reducers || 0,
        rust: token.conviction_extractors || 0
    };
    const total = conviction.diamond + conviction.gold + conviction.silver + conviction.rust || 1;

    // Bar
    const barY = convY + s(26);
    const barW = s(240), barH = s(12);

    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, barY, barW, barH, barH/2);
    ctx.fill();

    ctx.save();
    roundRect(ctx, rightX, barY, barW, barH, barH/2);
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

    // Legend
    const legendY = barY + s(28);
    const legendItems = [
        { label: 'Diamond', val: conviction.diamond, color: theme.conviction.diamond },
        { label: 'Gold', val: conviction.gold, color: theme.conviction.gold },
        { label: 'Silver', val: conviction.silver, color: theme.conviction.silver },
        { label: 'Rust', val: conviction.rust, color: theme.conviction.rust }
    ];

    legendItems.forEach((item, i) => {
        const itemY = legendY + i * s(26);
        ctx.beginPath();
        ctx.arc(rightX + s(5), itemY + s(6), s(4), 0, Math.PI * 2);
        ctx.fillStyle = item.color;
        ctx.fill();

        ctx.fillStyle = theme.text.secondary;
        ctx.font = `500 ${s(12)}px Arial, sans-serif`;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(item.label, rightX + s(16), itemY + s(6));

        ctx.fillStyle = theme.text.primary;
        ctx.font = `600 ${s(12)}px Arial, sans-serif`;
        ctx.textAlign = 'right';
        ctx.fillText(item.val.toString(), rightX + barW, itemY + s(6));
    });

    // Diamond Hands
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);
    const dhY = legendY + s(120);

    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, dhY, barW, s(40), s(8));
    ctx.fill();

    ctx.fillStyle = theme.text.secondary;
    ctx.font = `500 ${s(12)}px Arial, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText('Diamond Hands', rightX + s(12), dhY + s(20));

    ctx.fillStyle = dhPct >= 50 ? theme.conviction.diamond : theme.conviction.rust;
    ctx.font = `bold ${s(20)}px Arial, sans-serif`;
    ctx.textAlign = 'right';
    ctx.fillText(`${dhPct}%`, rightX + barW - s(12), dhY + s(20));

    // Footer
    const footerY = panelY + panelH - s(24);
    ctx.fillStyle = theme.text.muted;
    ctx.font = `500 ${s(12)}px Arial, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(BRAND_URL, leftX, footerY);

    ctx.textAlign = 'center';
    ctx.fillText(`${theme.tagline} 🐕‍🦺🔥`, centerX, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], RENDER_WIDTH - m - s(44), footerY);
}

// ═══════════════════════════════════════════════════════════════
// ASDF STYLE
// ═══════════════════════════════════════════════════════════════

async function drawAsdfCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // Background
    const bgGrad = ctx.createLinearGradient(0, 0, 0, RENDER_HEIGHT);
    bgGrad.addColorStop(0, theme.bg.secondary);
    bgGrad.addColorStop(0.6, theme.bg.primary);
    bgGrad.addColorStop(1, '#050201');
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Fire glow bottom
    const fireGlow = ctx.createRadialGradient(RENDER_WIDTH/2, RENDER_HEIGHT + s(80), 0, RENDER_WIDTH/2, RENDER_HEIGHT, s(350));
    fireGlow.addColorStop(0, 'rgba(251, 146, 60, 0.3)');
    fireGlow.addColorStop(0.4, 'rgba(234, 88, 12, 0.12)');
    fireGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = fireGlow;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Scanlines
    ctx.fillStyle = 'rgba(0, 0, 0, 0.12)';
    for (let i = 0; i < RENDER_HEIGHT; i += s(3)) {
        ctx.fillRect(0, i, RENDER_WIDTH, s(1.5));
    }

    // Panel
    const m = s(24);
    const panelX = m, panelY = m;
    const panelW = RENDER_WIDTH - m * 2, panelH = RENDER_HEIGHT - m * 2;

    // Shadow offset
    ctx.fillStyle = 'rgba(0, 0, 0, 0.55)';
    ctx.fillRect(panelX + s(5), panelY + s(5), panelW, panelH);

    ctx.fillStyle = theme.bg.tertiary;
    ctx.fillRect(panelX, panelY, panelW, panelH);
    ctx.strokeStyle = theme.accent;
    ctx.lineWidth = s(2.5);
    ctx.strokeRect(panelX, panelY, panelW, panelH);

    // Header
    const headerH = s(44);
    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(panelX, panelY, panelW, headerH);
    ctx.strokeStyle = theme.accent;
    ctx.lineWidth = s(1.5);
    ctx.beginPath();
    ctx.moveTo(panelX, panelY + headerH);
    ctx.lineTo(panelX + panelW, panelY + headerH);
    ctx.stroke();

    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(18)}px Courier, monospace`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText('> K-SCORE_TERMINAL', panelX + s(16), panelY + headerH/2);

    ctx.textAlign = 'right';
    ctx.fillStyle = theme.accent;
    ctx.font = `${s(14)}px Courier, monospace`;
    ctx.fillText(`🔥 ${theme.tagline}`, panelX + panelW - s(16), panelY + headerH/2);

    // LEFT: Token
    const leftX = panelX + s(28);
    const contentY = panelY + headerH + s(24);

    const tokenBoxW = s(260), tokenBoxH = s(90);
    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(leftX, contentY, tokenBoxW, tokenBoxH);
    ctx.strokeStyle = theme.accent;
    ctx.lineWidth = s(1.5);
    ctx.strokeRect(leftX, contentY, tokenBoxW, tokenBoxH);

    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = s(58);
    const imgX = leftX + s(14);
    const imgY = contentY + s(16);

    if (tokenImg) {
        ctx.drawImage(tokenImg, imgX, imgY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.bg.primary;
        ctx.fillRect(imgX, imgY, imgSize, imgSize);
        ctx.fillStyle = theme.accent;
        ctx.font = `bold ${s(24)}px Courier, monospace`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', imgX + imgSize/2, imgY + imgSize/2);
    }

    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = s(2);
    ctx.strokeRect(imgX - s(2), imgY - s(2), imgSize + s(4), imgSize + s(4));

    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(20)}px Courier, monospace`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText((token.name || 'Unknown').slice(0, 10), imgX + imgSize + s(14), imgY + s(6));

    ctx.fillStyle = theme.text.secondary;
    ctx.font = `${s(16)}px Courier, monospace`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase()}`, imgX + imgSize + s(14), imgY + s(32));

    // Stats
    const statsY = contentY + tokenBoxH + s(18);
    ctx.fillStyle = theme.text.muted;
    ctx.font = `${s(12)}px Courier, monospace`;
    ctx.fillText('MCAP:', leftX, statsY);
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(12)}px Courier, monospace`;
    ctx.fillText(token.marketCap ? `$${formatNumber(token.marketCap)}` : 'N/A', leftX + s(55), statsY);

    ctx.fillStyle = theme.text.muted;
    ctx.font = `${s(12)}px Courier, monospace`;
    ctx.fillText('HOLDERS:', leftX, statsY + s(20));
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(12)}px Courier, monospace`;
    ctx.fillText(token.holders ? formatNumber(token.holders) : 'N/A', leftX + s(80), statsY + s(20));

    // CENTER: Score
    const centerX = RENDER_WIDTH / 2;
    const scoreBoxY = contentY;
    const scoreBoxW = s(200), scoreBoxH = s(240);

    ctx.fillStyle = 'rgba(0, 0, 0, 0.4)';
    ctx.fillRect(centerX - scoreBoxW/2 + s(4), scoreBoxY + s(4), scoreBoxW, scoreBoxH);

    ctx.fillStyle = theme.bg.secondary;
    ctx.fillRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);
    ctx.strokeStyle = theme.accent;
    ctx.lineWidth = s(2.5);
    ctx.strokeRect(centerX - scoreBoxW/2, scoreBoxY, scoreBoxW, scoreBoxH);

    ctx.fillStyle = theme.text.muted;
    ctx.font = `bold ${s(12)}px Courier, monospace`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText('[ K-SCORE ]', centerX, scoreBoxY + s(14));

    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = s(25);
    ctx.fillStyle = gradeStyle.color;
    ctx.font = `bold ${s(88)}px Courier, monospace`;
    ctx.textBaseline = 'middle';
    ctx.fillText(score.toString(), centerX, scoreBoxY + s(100));
    ctx.shadowColor = 'transparent';

    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(18)}px Courier, monospace`;
    ctx.fillText(`[${grade.label.toUpperCase()}]`, centerX, scoreBoxY + s(160));

    ctx.fillStyle = theme.text.muted;
    ctx.font = `${s(12)}px Courier, monospace`;
    ctx.fillText(grade.credit, centerX, scoreBoxY + s(185));

    // Progress bar
    const progY = scoreBoxY + scoreBoxH - s(24);
    const progW = scoreBoxW - s(28), progH = s(8);

    ctx.fillStyle = theme.bg.primary;
    ctx.fillRect(centerX - progW/2, progY, progW, progH);
    ctx.fillStyle = gradeStyle.color;
    ctx.fillRect(centerX - progW/2, progY, (score / 100) * progW, progH);
    ctx.strokeStyle = theme.accent;
    ctx.lineWidth = s(1);
    ctx.strokeRect(centerX - progW/2, progY, progW, progH);

    // RIGHT: Conviction
    const rightX = RENDER_WIDTH - s(320);
    const convY = contentY;

    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(14)}px Courier, monospace`;
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
        const itemY = convY + s(26) + i * s(44);

        ctx.fillStyle = theme.bg.primary;
        ctx.fillRect(rightX, itemY, s(220), s(16));
        ctx.fillStyle = item.color;
        ctx.fillRect(rightX, itemY, (item.pct / 100) * s(220), s(16));
        ctx.strokeStyle = theme.accent;
        ctx.lineWidth = s(1);
        ctx.strokeRect(rightX, itemY, s(220), s(16));

        ctx.fillStyle = theme.text.primary;
        ctx.font = `${s(11)}px Courier, monospace`;
        ctx.textBaseline = 'top';
        ctx.fillText(`${item.label}: ${item.val} (${item.pct}%)`, rightX, itemY + s(20));
    });

    // Diamond hands
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);
    const dhY = convY + s(210);

    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(12)}px Courier, monospace`;
    ctx.fillText('> DIAMOND_HANDS:', rightX, dhY);

    ctx.fillStyle = dhPct >= 50 ? '#22c55e' : '#ef4444';
    ctx.font = `bold ${s(20)}px Courier, monospace`;
    ctx.fillText(`${dhPct}%`, rightX + s(165), dhY);

    // Footer
    const footerY = panelY + panelH - s(18);
    ctx.fillStyle = theme.text.muted;
    ctx.font = `${s(11)}px Courier, monospace`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(`[${BRAND_URL}]`, panelX + s(16), footerY);

    ctx.textAlign = 'right';
    ctx.fillText(`[${new Date().toISOString().split('T')[0]}]`, panelX + panelW - s(16), footerY);
}

// ═══════════════════════════════════════════════════════════════
// MINIMAL STYLE (Dark, clean, institutional)
// Golden ratio spacing, WCAG AA contrast, trader-optimized hierarchy
// ═══════════════════════════════════════════════════════════════

async function drawMinimalCard(ctx, token, theme) {
    const score = Math.round(token.k_score || 0);
    const grade = getGrade(score, token.mint);
    const gradeStyle = theme.grades[grade.key];

    // φ-based spacing constants
    const PHI = 1.618;
    const MARGIN = s(36);

    // Background with subtle warmth
    const bgGrad = ctx.createLinearGradient(0, 0, 0, RENDER_HEIGHT);
    bgGrad.addColorStop(0, theme.bg.primary);
    bgGrad.addColorStop(1, theme.bg.secondary);
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Subtle corner glow in grade color (psychological anchor)
    const cornerGlow = ctx.createRadialGradient(
        RENDER_WIDTH, 0, 0,
        RENDER_WIDTH, 0, s(400)
    );
    cornerGlow.addColorStop(0, gradeStyle.glow);
    cornerGlow.addColorStop(0.5, gradeStyle.glow.replace(/[\d.]+\)$/, '0.05)'));
    cornerGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = cornerGlow;
    ctx.fillRect(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

    // Main panel
    const panelX = MARGIN, panelY = MARGIN;
    const panelW = RENDER_WIDTH - MARGIN * 2, panelH = RENDER_HEIGHT - MARGIN * 2;

    ctx.fillStyle = theme.bg.card;
    roundRect(ctx, panelX, panelY, panelW, panelH, s(16));
    ctx.fill();

    // Panel border with grade accent on top
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
    ctx.lineWidth = s(1);
    roundRect(ctx, panelX, panelY, panelW, panelH, s(16));
    ctx.stroke();

    // Grade accent line (top) - instant visual signal
    ctx.fillStyle = gradeStyle.color;
    roundRect(ctx, panelX, panelY, panelW, s(3), s(16));
    ctx.save();
    ctx.clip();
    ctx.fillRect(panelX, panelY, panelW, s(3));
    ctx.restore();

    // ═══════════════════════════════════════════════════════════════
    // MINIMAL LAYOUT - Clean, institutional, readable
    // Refined sizing for harmony
    // ═══════════════════════════════════════════════════════════════
    void PHI;
    const leftX = panelX + s(40);
    const topY = panelY + s(40);
    const centerY = panelY + panelH / 2;

    // Zone boundaries
    const RIGHT_ZONE_X = RENDER_WIDTH - s(340);

    // ═══════════════════════════════════════════════════════════════
    // LEFT ZONE: Token Identity
    // ═══════════════════════════════════════════════════════════════
    const tokenImg = await fetchTokenImage(token.image);
    const imgSize = s(72);

    // Token image with grade ring
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = s(20);
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2 + s(3), 0, Math.PI * 2);
    ctx.strokeStyle = gradeStyle.color;
    ctx.lineWidth = s(2.5);
    ctx.stroke();
    ctx.shadowColor = 'transparent';

    ctx.save();
    ctx.beginPath();
    ctx.arc(leftX + imgSize/2, topY + imgSize/2, imgSize/2, 0, Math.PI * 2);
    ctx.clip();
    if (tokenImg) {
        ctx.drawImage(tokenImg, leftX, topY, imgSize, imgSize);
    } else {
        ctx.fillStyle = theme.bg.tertiary;
        ctx.fillRect(leftX, topY, imgSize, imgSize);
        ctx.fillStyle = theme.text.secondary;
        ctx.font = `600 ${s(32)}px Arial, Helvetica, sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(token.symbol?.[0] || '?', leftX + imgSize/2, topY + imgSize/2);
    }
    ctx.restore();

    // Token name + symbol (next to image)
    const nameX = leftX + imgSize + s(16);
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(36)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText((token.name || 'Unknown').slice(0, 12), nameX, topY + s(12));

    ctx.fillStyle = theme.text.secondary;
    ctx.font = `600 ${s(22)}px Arial, Helvetica, sans-serif`;
    ctx.fillText(`$${(token.symbol || '???').toUpperCase().slice(0, 8)}`, nameX, topY + s(48));

    // Stats below token
    const statsY = topY + imgSize + s(24);

    // MCAP
    ctx.fillStyle = theme.text.muted;
    ctx.font = `600 ${s(14)}px Arial, Helvetica, sans-serif`;
    ctx.textBaseline = 'top';
    ctx.fillText('MCAP', leftX, statsY);
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(28)}px Arial, Helvetica, sans-serif`;
    ctx.fillText(token.marketCap ? `$${formatNumber(token.marketCap)}` : '—', leftX, statsY + s(18));

    // HOLDERS
    ctx.fillStyle = theme.text.muted;
    ctx.font = `600 ${s(14)}px Arial, Helvetica, sans-serif`;
    ctx.fillText('HOLDERS', leftX, statsY + s(56));
    ctx.fillStyle = theme.text.primary;
    ctx.font = `bold ${s(28)}px Arial, Helvetica, sans-serif`;
    ctx.fillText(token.holders ? formatNumber(token.holders) : '—', leftX, statsY + s(74));

    // ═══════════════════════════════════════════════════════════════
    // CENTER: Grade (hero but refined)
    // ═══════════════════════════════════════════════════════════════
    const gradeX = RENDER_WIDTH / 2;

    // GRADE
    ctx.shadowColor = gradeStyle.glow;
    ctx.shadowBlur = s(25);
    ctx.fillStyle = gradeStyle.color;
    ctx.font = `bold ${s(88)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(grade.label.toUpperCase(), gradeX, centerY - s(16));
    ctx.shadowColor = 'transparent';

    // Credit rating
    ctx.fillStyle = theme.text.secondary;
    ctx.font = `600 ${s(24)}px Arial, Helvetica, sans-serif`;
    ctx.fillText(grade.credit, gradeX, centerY + s(36));

    // K-Score
    ctx.fillStyle = theme.text.muted;
    ctx.font = `600 ${s(18)}px Arial, Helvetica, sans-serif`;
    ctx.fillText(`K-Score: ${score}`, gradeX, centerY + s(64));

    // ═══════════════════════════════════════════════════════════════
    // RIGHT ZONE: Conviction
    // ═══════════════════════════════════════════════════════════════
    const rightX = RIGHT_ZONE_X;
    const convY = topY;
    const barW = s(280);

    // Header
    ctx.fillStyle = theme.text.secondary;
    ctx.font = `600 ${s(16)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('TOP 20 CONVICTION', rightX, convY);

    const conviction = {
        diamond: token.conviction_accumulators || 0,
        gold: token.conviction_holders || 0,
        silver: token.conviction_reducers || 0,
        rust: token.conviction_extractors || 0
    };
    const total = conviction.diamond + conviction.gold + conviction.silver + conviction.rust || 1;
    const dhPct = Math.round(((conviction.diamond + conviction.gold) / total) * 100);

    // Conviction bar
    const barY = convY + s(28);
    const barH = s(12);

    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, barY, barW, barH, s(6));
    ctx.fill();

    ctx.save();
    roundRect(ctx, rightX, barY, barW, barH, s(6));
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

    // Legend - 2x2 grid
    const legendY = barY + s(24);
    const legendItems = [
        { label: 'Diamond', val: conviction.diamond, color: theme.conviction.diamond },
        { label: 'Gold', val: conviction.gold, color: theme.conviction.gold },
        { label: 'Silver', val: conviction.silver, color: theme.conviction.silver },
        { label: 'Rust', val: conviction.rust, color: theme.conviction.rust }
    ];

    legendItems.forEach((item, i) => {
        const col = i % 2;
        const row = Math.floor(i / 2);
        const itemX = rightX + col * s(145);
        const itemY = legendY + row * s(36);

        // Dot
        ctx.beginPath();
        ctx.arc(itemX + s(6), itemY + s(10), s(5), 0, Math.PI * 2);
        ctx.fillStyle = item.color;
        ctx.fill();

        // Label
        ctx.fillStyle = theme.text.secondary;
        ctx.font = `500 ${s(16)}px Arial, Helvetica, sans-serif`;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(item.label, itemX + s(16), itemY + s(10));

        // Value
        ctx.fillStyle = theme.text.primary;
        ctx.font = `600 ${s(16)}px Arial, Helvetica, sans-serif`;
        ctx.textAlign = 'right';
        ctx.fillText(item.val.toString(), itemX + s(135), itemY + s(10));
    });

    // Diamond Hands box
    const dhY = legendY + s(90);
    const dhBoxW = barW, dhBoxH = s(52);

    ctx.fillStyle = theme.bg.tertiary;
    roundRect(ctx, rightX, dhY, dhBoxW, dhBoxH, s(10));
    ctx.fill();

    // DH label
    ctx.fillStyle = theme.text.secondary;
    ctx.font = `600 ${s(14)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText('Diamond Hands', rightX + s(12), dhY + dhBoxH/2);

    // DH percentage
    const dhColor = dhPct >= 60 ? theme.conviction.diamond
                  : dhPct >= 40 ? theme.conviction.gold
                  : dhPct >= 20 ? theme.conviction.silver
                  : theme.conviction.rust;
    ctx.fillStyle = dhColor;
    ctx.font = `bold ${s(32)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'right';
    ctx.fillText(`${dhPct}%`, rightX + dhBoxW - s(12), dhY + dhBoxH/2);

    // ═══════════════════════════════════════════════════════════════
    // FOOTER
    // ═══════════════════════════════════════════════════════════════
    const footerY = panelY + panelH - s(32);

    // Separator line
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.1)';
    ctx.lineWidth = s(1);
    ctx.beginPath();
    ctx.moveTo(panelX + s(44), footerY - s(18));
    ctx.lineTo(panelX + panelW - s(44), footerY - s(18));
    ctx.stroke();

    // Footer text
    ctx.fillStyle = theme.text.muted;
    ctx.font = `bold ${s(36)}px Arial, Helvetica, sans-serif`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(BRAND_URL, panelX + s(44), footerY);

    ctx.textAlign = 'center';
    ctx.fillText(theme.tagline, RENDER_WIDTH / 2, footerY);

    ctx.textAlign = 'right';
    ctx.fillText(new Date().toISOString().split('T')[0], panelX + panelW - s(44), footerY);
}

// ═══════════════════════════════════════════════════════════════
// MAIN GENERATOR
// ═══════════════════════════════════════════════════════════════

async function generateKScoreCard(token, style = 'holdex') {
    // Create high-res canvas
    const canvas = createCanvas(RENDER_WIDTH, RENDER_HEIGHT);
    const ctx = canvas.getContext('2d');

    // Enable high quality rendering
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = 'high';

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

    // Downscale to target size for sharper result
    const outputCanvas = createCanvas(CARD_WIDTH, CARD_HEIGHT);
    const outputCtx = outputCanvas.getContext('2d');
    outputCtx.imageSmoothingEnabled = true;
    outputCtx.imageSmoothingQuality = 'high';
    outputCtx.drawImage(canvas, 0, 0, CARD_WIDTH, CARD_HEIGHT);

    return outputCanvas.toBuffer('image/png');
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
