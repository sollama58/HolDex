require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const http = require('http');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
// Optional tasks - wrapped in try/catch to prevent startup failure if files missing
let startSnapshotter, startNewTokenListener;
try {
    ({ startSnapshotter } = require('./indexer/tasks/snapshotter'));
    ({ startNewTokenListener } = require('./tasks/newTokenListener'));
} catch (e) { logger.warn('⚠️  Indexer tasks not found, skipping...'); }

const { initSocket } = require('./services/socket');
const tokensRoutes = require('./routes/tokens');
const path = require('path');

const app = express();
const server = http.createServer(app);

// SECURITY HEADERS
app.use(helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: false,
}));

// CORS CONFIGURATION (Relaxed for debugging)
const allowedOrigins = config.CORS_ORIGINS || '*';

const corsOptions = {
    origin: true, // Allow all origins reflected (safest for unknown frontend domains)
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-auth', 'x-requested-with', 'Accept']
};

app.options('*', cors(corsOptions));
app.use(cors(corsOptions));
app.use(express.json({ limit: '100kb' }));

// RATE LIMITING
app.set('trust proxy', 1);
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 1000, // Increased limit to prevent false positives
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req, res) => req.headers['x-forwarded-for'] || req.ip
});
app.use(limiter);

// HEALTH CHECK (Always JSON)
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: Date.now(), service: 'HolDEX API' });
});

async function startServer() {
    try {
        logger.info('🚀 System: Initializing HolDEX API...');
        
        // Initialize Core Services
        await initDB();
        await connectRedis();

        // Initialize Socket
        try {
            initSocket(server, allowedOrigins);
        } catch (e) { logger.error(`Socket Init Failed: ${e.message}`); }

        // Start Background Tasks
        if (startSnapshotter) startSnapshotter();
        if (startNewTokenListener) {
            startNewTokenListener().catch(e => logger.error(`Listener Start Error: ${e.message}`));
        }

        // --- ROUTING FIXES ---
        // Initialize routes with DB dependency
        const apiRouter = tokensRoutes.init({ db: getDB() });

        // Mount at BOTH /api and / (root) to fix frontend mismatch
        app.use('/api', apiRouter);
        app.use('/', apiRouter);

        // Serve Homepage for root if no API route matched
        app.get('/', (req, res) => {
            if (req.accepts('html')) {
                res.sendFile(path.join(__dirname, '../homepage.html'));
            } else {
                res.json({ message: "HolDEX API Running" });
            }
        });

        // 404 Handler - FORCE JSON for API-like requests
        app.use((req, res, next) => {
            if (req.path.startsWith('/api') || req.path.startsWith('/token') || req.headers.accept?.includes('json')) {
                return res.status(404).json({ success: false, error: 'Endpoint not found' });
            }
            next(); // Fallback to default HTML 404 for browser
        });

        // Global Error Handler - FORCE JSON
        app.use((err, req, res, next) => {
            logger.error(`🔥 Unhandled Server Error: ${err.message}`);
            logger.error(err.stack);
            
            if (res.headersSent) return next(err);

            // Always return JSON for errors to prevent "DOCTYPE" issues
            res.status(500).json({ 
                success: false, 
                error: 'Internal Server Error',
                message: process.env.NODE_ENV === 'development' ? err.message : undefined
            });
        });

        const PORT = process.env.PORT || 3000;
        server.listen(PORT, () => {
            logger.info(`✅ API & Socket: Listening on port ${PORT}`);
        });

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

startServer();
