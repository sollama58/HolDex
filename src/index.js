const express = require('express');
const http = require('http');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const { Server } = require('socket.io');
const config = require('./config/env');
const { initDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');

// NOTE: Background tasks (metadataUpdater, etc.) are NO LONGER imported here.
// They run in the separate 'worker' service defined in docker-compose.

const app = express();
const server = http.createServer(app);

// --- WEBSOCKET SERVER ---
const io = new Server(server, {
    cors: {
        origin: process.env.CORS_ORIGIN || '*',
        methods: ['GET', 'POST']
    },
    pingTimeout: 60000,
    pingInterval: 25000
});

app.set('trust proxy', 1);

// --- SECURITY & MIDDLEWARE ---
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: [
                "'self'",
                "'unsafe-inline'",
                "'unsafe-eval'",
                "https://cdn.socket.io",
                "https://unpkg.com",
                "https://bundle.run",
                "https://cdn.tailwindcss.com"
            ],
            scriptSrcAttr: ["'unsafe-inline'"],  // Allow onclick handlers
            styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'", "wss:", "https:"],
            fontSrc: ["'self'", "https://fonts.gstatic.com", "https://fonts.googleapis.com", "data:"]
        }
    },
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginOpenerPolicy: { policy: "same-origin" }
}));
app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    credentials: false  // Security: credentials only with specific origins
}));
app.use(express.json());

// --- STATIC FILES (Frontend) ---
app.use(express.static(path.join(__dirname, '..')));

// Serve homepage.html for root
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'homepage.html'));
});

// --- HEALTH CHECK ---
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        service: 'holdex-api',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

// --- REQUEST LOGGING ---
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
});

// --- RATE LIMITING ---

// 1. Global Limiter: 3000 requests per 15 mins (Increased for scale)
const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, 
    max: 3000, 
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: "Too many requests, please try again later." }
});
app.use(globalLimiter);

// 2. Strict Limiter: For Search & Updates
const strictLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 150, // Slight increase to handle bursts
    message: { success: false, error: "Rate limit exceeded. Please slow down." }
});

// --- WEBSOCKET HANDLERS ---

// Track connected clients and subscriptions
const subscriptions = {
    tokens: new Set(),      // Clients subscribed to all token updates
    prices: new Map(),      // Map<mint, Set<socketId>> for price updates
    kscore: new Map()       // Map<mint, Set<socketId>> for K-Score updates
};

io.on('connection', (socket) => {
    console.log(`[WS] Client connected: ${socket.id}`);

    // --- LEGACY: Frontend uses 'subscribe' with mint ---
    socket.on('subscribe', (mint) => {
        if (!mint || typeof mint !== 'string') return;
        socket.join(`token:${mint}`);
        console.log(`[WS] ${socket.id} subscribed to token:${mint.slice(0,8)}`);
    });

    // Subscribe to all token updates (new tokens, price changes)
    socket.on('subscribe:tokens', () => {
        subscriptions.tokens.add(socket.id);
        socket.join('tokens');
        socket.emit('subscribed', { channel: 'tokens' });
        console.log(`[WS] ${socket.id} subscribed to tokens`);
    });

    // Subscribe to specific token price updates
    socket.on('subscribe:price', (mint) => {
        if (!mint || typeof mint !== 'string') return;
        if (!subscriptions.prices.has(mint)) {
            subscriptions.prices.set(mint, new Set());
        }
        subscriptions.prices.get(mint).add(socket.id);
        socket.join(`price:${mint}`);
        socket.emit('subscribed', { channel: 'price', mint });
        console.log(`[WS] ${socket.id} subscribed to price:${mint.slice(0,8)}`);
    });

    // Subscribe to K-Score updates for a token
    socket.on('subscribe:kscore', (mint) => {
        if (!mint || typeof mint !== 'string') return;
        if (!subscriptions.kscore.has(mint)) {
            subscriptions.kscore.set(mint, new Set());
        }
        subscriptions.kscore.get(mint).add(socket.id);
        socket.join(`kscore:${mint}`);
        socket.emit('subscribed', { channel: 'kscore', mint });
        console.log(`[WS] ${socket.id} subscribed to kscore:${mint.slice(0,8)}`);
    });

    // Unsubscribe handlers
    socket.on('unsubscribe:tokens', () => {
        subscriptions.tokens.delete(socket.id);
        socket.leave('tokens');
    });

    socket.on('unsubscribe:price', (mint) => {
        if (subscriptions.prices.has(mint)) {
            subscriptions.prices.get(mint).delete(socket.id);
        }
        socket.leave(`price:${mint}`);
    });

    socket.on('unsubscribe:kscore', (mint) => {
        if (subscriptions.kscore.has(mint)) {
            subscriptions.kscore.get(mint).delete(socket.id);
        }
        socket.leave(`kscore:${mint}`);
    });

    // Cleanup on disconnect
    socket.on('disconnect', () => {
        subscriptions.tokens.delete(socket.id);
        // Clean up from all price/kscore subscriptions
        for (const [_mint, sockets] of subscriptions.prices) {
            sockets.delete(socket.id);
        }
        for (const [_mint, sockets] of subscriptions.kscore) {
            sockets.delete(socket.id);
        }
        console.log(`[WS] Client disconnected: ${socket.id}`);
    });
});

// --- BROADCAST HELPERS (exported for workers) ---

// Broadcast new token to all subscribers
function broadcastNewToken(token) {
    io.to('tokens').emit('token:new', {
        mint: token.mint,
        name: token.name,
        symbol: token.symbol,
        timestamp: Date.now()
    });
}

// Broadcast price update for a token
function broadcastPriceUpdate(mint, data) {
    io.to('tokens').emit('token:price', { mint, ...data });
    io.to(`price:${mint}`).emit('price:update', { mint, ...data });
    // Legacy: frontend expects 'update' event
    io.to(`token:${mint}`).emit('update', { mint, ...data });
}

// Broadcast K-Score update for a token
function broadcastKScoreUpdate(mint, data) {
    io.to('tokens').emit('token:kscore', { mint, ...data });
    io.to(`kscore:${mint}`).emit('kscore:update', { mint, ...data });
    // Legacy: frontend expects 'update' event
    io.to(`token:${mint}`).emit('update', { mint, ...data });
}

// Export broadcast functions for use in workers/tasks
module.exports.broadcast = {
    newToken: broadcastNewToken,
    priceUpdate: broadcastPriceUpdate,
    kscoreUpdate: broadcastKScoreUpdate,
    io
};

// --- INITIALIZATION ---

async function startServer() {
    try {
        // 1. Init Data Layer
        const db = await initDB();
        const redis = await initRedis();

        // 2. Dependencies (include io for routes that need it)
        const deps = { db, redis, io, broadcast: module.exports.broadcast };

        // 3. Init Routes
        // Search and Write operations get strict limits
        app.use('/api/request-update', strictLimiter);
        app.use('/api/tokens', strictLimiter);

        app.use('/api', tokenRoutes.init(deps));

        // SPA fallback - serve homepage.html for all non-API routes
        // This enables client-side routing for /about, /token/:mint, etc.
        app.get('*', (req, res) => {
            res.sendFile(path.join(__dirname, '..', 'homepage.html'));
        });

        // 4. Start HTTP + WebSocket Server
        server.listen(config.PORT, () => {
            console.log(`🔥 HolDex API Node Online on port ${config.PORT}`);
            console.log(`🔌 WebSocket Server Ready`);
            console.log(`🛡️  Mode: API Only (Workers decoupled)`);
        });
    } catch (err) {
        console.error("Fatal Server Startup Error:", err);
        process.exit(1);
    }
}

startServer();
