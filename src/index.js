require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');

const tokensRoutes = require('./routes/tokens');

const app = express();

// SECURITY HEADERS
// IMPORTANT: We must relax specific policies to allow Public API access from other domains
app.use(helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" }, // Allows images/data to be loaded by other sites
    contentSecurityPolicy: false, // Disable default CSP which can block API calls
}));

// CORS CONFIGURATION
const allowedOrigins = config.CORS_ORIGINS;

const corsOptions = {
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps, curl, or server-to-server)
        if (!origin) return callback(null, true);
        
        // Check if origin is allowed
        if (allowedOrigins === '*' || (Array.isArray(allowedOrigins) && allowedOrigins.includes(origin))) {
            return callback(null, true);
        } else {
            // Log the blocked origin for debugging
            logger.warn(`⚠️ CORS Blocked Origin: ${origin}`);
            // Return an error to block the request
            return callback(new Error(`Not allowed by CORS (Origin: ${origin})`));
        }
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-auth', 'x-requested-with', 'Accept']
};

// 1. Handle Pre-flight requests explicitly
// This is critical. Browsers send an OPTIONS request before the actual POST/GET.
// If this is not handled, the browser assumes CORS is failed.
app.options('*', cors(corsOptions));

// 2. Apply CORS to all requests
app.use(cors(corsOptions));

// Body Parser with increased limit
app.use(express.json({ limit: '100kb' }));

// RATE LIMITING
// Trust proxy is required if you are behind a load balancer (like Render, Heroku, AWS)
app.set('trust proxy', 1);

const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 500, // limit each IP to 500 requests per windowMs
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req, res) => {
        return req.headers['x-forwarded-for'] || req.ip; 
    }
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

        // A. Initialize Database
        await initDB();
        
        // B. Initialize Routes
        app.use('/api', tokensRoutes.init({ db: getDB() }));

        // C. Start API Server
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
