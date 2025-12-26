require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const http = require('http'); // NEW: Required for Socket.io
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
const { startSnapshotter } = require('./indexer/tasks/snapshotter'); 
const { startNewTokenListener } = require('./tasks/newTokenListener'); 
const { initSocket } = require('./services/socket'); // NEW
const tokensRoutes = require('./routes/tokens');

const app = express();
const server = http.createServer(app); // Wrap Express

// SECURITY HEADERS
app.use(helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: false,
}));

// CORS CONFIGURATION
const allowedOrigins = config.CORS_ORIGINS;

const corsOptions = {
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);
        const isConfigAllowed = allowedOrigins === '*' || (Array.isArray(allowedOrigins) && allowedOrigins.includes(origin));
        const isDomainAllowed = origin.includes('alonisthe.dev') || origin.includes('localhost'); 

        if (isConfigAllowed || isDomainAllowed) {
            return callback(null, true);
        } else {
            logger.warn(`⚠️ CORS Blocked Origin: ${origin}`);
            return callback(new Error(`Not allowed by CORS (Origin: ${origin})`));
        }
    },
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
    max: 500, 
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req, res) => req.headers['x-forwarded-for'] || req.ip
});
app.use(limiter);

app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: Date.now(), service: 'HolDEX API' });
});

async function startServer() {
    try {
        logger.info('🚀 System: Initializing HolDEX API...');
        
        await initDB();
        await connectRedis();

        // Start WebSocket Server
        initSocket(server, allowedOrigins);

        startSnapshotter();
        startNewTokenListener().catch(e => logger.error(`Listener Start Error: ${e.message}`));

        app.use('/api', tokensRoutes.init({ db: getDB() }));

        app.use((err, req, res, next) => {
            logger.error(`🔥 Unhandled Server Error: ${err.message}`);
            logger.error(err.stack);
            if (!res.headersSent) {
                res.status(500).json({ success: false, error: 'Internal Server Error' });
            }
        });

        const PORT = process.env.PORT || 3000;
        // Use 'server.listen' instead of 'app.listen'
        server.listen(PORT, () => {
            logger.info(`✅ API & Socket: Listening on port ${PORT}`);
        });

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

startServer();
