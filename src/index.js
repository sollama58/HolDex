require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
const { startSnapshotter } = require('./indexer/tasks/snapshotter'); // Import Snapshotter
const tokensRoutes = require('./routes/tokens');

const app = express();

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
        if (allowedOrigins === '*' || (Array.isArray(allowedOrigins) && allowedOrigins.includes(origin))) {
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
        const originLog = Array.isArray(allowedOrigins) ? allowedOrigins.join(', ') : allowedOrigins;
        logger.info(`🛡️  CORS Configured for: ${originLog}`);

        // 1. Initialize Database
        await initDB();
        
        // 2. Initialize Redis
        await connectRedis();

        // 3. Start Background Tasks (The Snapshotter)
        startSnapshotter();

        // 4. Initialize Routes
        app.use('/api', tokensRoutes.init({ db: getDB() }));

        // 5. Global Error Handler
        app.use((err, req, res, next) => {
            logger.error(`🔥 Unhandled Server Error: ${err.message}`);
            logger.error(err.stack);
            if (!res.headersSent) {
                res.status(500).json({ success: false, error: 'Internal Server Error' });
            }
        });

        const PORT = process.env.PORT || 3000;
        app.listen(PORT, () => {
            logger.info(`✅ API: Listening on port ${PORT}`);
        });

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

startServer();
