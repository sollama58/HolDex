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
app.use(helmet({
    crossOriginResourcePolicy: false,
}));

// CORS CONFIGURATION
const allowedOrigins = config.CORS_ORIGINS;

app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);
        
        if (allowedOrigins === '*' || allowedOrigins.indexOf(origin) !== -1) {
            callback(null, true);
        } else {
            logger.warn(`CORS Blocked Origin: ${origin}`);
            callback(new Error(`Not allowed by CORS (Origin: ${origin})`));
        }
    },
    credentials: true
}));

app.use(express.json());

// RATE LIMITING
app.set('trust proxy', 1);

const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 500, 
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
        logger.info(`🛡️  CORS Configuration: ${Array.isArray(allowedOrigins) ? allowedOrigins.join(', ') : 'ALLOW ALL (*)'}`);

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
