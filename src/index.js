require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');

// --- ROUTES ---
const tokensRoutes = require('./routes/tokens');
const { startSnapshotter } = require('./indexer/tasks/snapshotter');

// Note: pumpfun listener removed to prevent auto-indexing of all new tokens.
// const { startPumpListener } = require('./indexer/listeners/pumpfun'); 

const app = express();

// 1. Security & Middleware
app.use(helmet());
app.use(cors({ origin: '*' })); // Allow all for public API
app.use(express.json());

// Global Rate Limiting
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 500, 
    standardHeaders: true,
    legacyHeaders: false,
});
app.use(limiter);

// 2. Health Check
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        timestamp: Date.now(),
        service: 'HolDEX API' 
    });
});

async function startServer() {
    try {
        logger.info('🚀 System: Initializing HolDEX API...');

        // A. Initialize Database
        await initDB();
        
        // B. Initialize Routes
        app.use('/api', tokensRoutes.init({ db: getDB() }));

        // C. Start API Server
        const PORT = process.env.PORT || 3000;
        app.listen(PORT, () => {
            logger.info(`✅ API: Listening on port ${PORT}`);
        });

        // D. Start Background Services
        // We only run the snapshotter now. New tokens are added via Search/Update only.
        logger.info('🔄 System: Starting Background Snapshotter...');
        startSnapshotter();

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

startServer();
