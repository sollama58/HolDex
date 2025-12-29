const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const config = require('./config/env');
const { initDB } = require('./services/database');
const { initRedis } = require('./services/redis');
const tokenRoutes = require('./routes/tokens');

// NOTE: Background tasks (metadataUpdater, etc.) are NO LONGER imported here.
// They run in the separate 'worker' service defined in docker-compose.

const app = express();

app.set('trust proxy', 1);

// --- SECURITY & MIDDLEWARE ---
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'", "'unsafe-inline'", "https://cdn.socket.io"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            imgSrc: ["'self'", "data:", "https:"],
            connectSrc: ["'self'", "wss:", "https:"],
            fontSrc: ["'self'", "https:", "data:"]
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

// --- INITIALIZATION ---

async function startServer() {
    try {
        // 1. Init Data Layer
        const db = await initDB();
        const redis = await initRedis();

        // 2. Dependencies
        const deps = { db, redis };
        
        // 3. Init Routes
        // Search and Write operations get strict limits
        app.use('/api/request-update', strictLimiter);
        app.use('/api/tokens', strictLimiter); 
        
        app.use('/api', tokenRoutes.init(deps));

        // 4. Start Listener
        app.listen(config.PORT, () => {
            console.log(`🔥 HolDex API Node Online on port ${config.PORT}`);
            console.log(`🛡️  Mode: API Only (Workers decoupled)`);
        });
    } catch (err) {
        console.error("Fatal Server Startup Error:", err);
        process.exit(1);
    }
}

startServer();
