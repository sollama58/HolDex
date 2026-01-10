require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const http = require('http'); 
const rateLimit = require('express-rate-limit');
const compression = require('compression'); 
const config = require('./config/env');
const logger = require('./services/logger');
const { initDB, getDB } = require('./services/database');
const { connectRedis } = require('./services/redis');
// MOVED TO CALCULATOR: const { startSnapshotter } = require('./indexer/tasks/snapshotter');
const _kScoreUpdater = require('./tasks/kScoreUpdater');
const integrityWatchdog = require('./tasks/integrityWatchdog');
// MOVED TO CALCULATOR: const { startPriceWorker } = require('./tasks/priceWorker');
const alerting = require('./services/alerting');
// REMOVED: const { startNewTokenListener } = require('./services/new_token_listener'); 
const { initSocket } = require('./services/socket'); 
const tokensRoutes = require('./routes/tokens');
const webhooksRoutes = require('./routes/webhooks');
const oracleRoutes = require('./routes/oracle');
const spaceRoutes = require('./routes/space');
const nodesRoutes = require('./routes/nodes');
const nodeService = require('./services/nodeService');
const { getOrCreateMasterWebhook } = require('./services/heliusWebhook');
const { correlationIdMiddleware, requestTimingMiddleware } = require('./middleware/correlationId');
const fs = require('fs');
const path = require('path');

const app = express();

// SECURITY: HTML escape function to prevent XSS in dynamic meta tags
function escapeHtml(str) {
    if (!str || typeof str !== 'string') return '';
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
const server = http.createServer(app); 

// SECURITY HEADERS
// CSP allows external CDNs used by frontend (Tailwind, Socket.io, Solana, Charts)
app.use(helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: [
                "'self'",
                "'unsafe-inline'",
                "'unsafe-eval'", // Required for Tailwind JIT
                "https://bundle.run",
                "https://unpkg.com",
                "https://cdn.socket.io",
                "https://cdn.tailwindcss.com",
            ],
            scriptSrcAttr: ["'unsafe-inline'"], // Allow onclick="" handlers
            styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: [
                "'self'",
                "wss:",
                "ws:",
                "https://api.helius.xyz",
                "https://*.helius-rpc.com",
                "https://*.solana.com",
                "https://lite-api.jup.ag",
                "https://api.jup.ag",
                "https://api-v3.raydium.io",
                "https://api.coingecko.com",
            ],
            fontSrc: ["'self'", "https://fonts.gstatic.com", "https://fonts.googleapis.com", "data:"],
            objectSrc: ["'none'"],
            mediaSrc: ["'self'"],
            frameSrc: ["'none'"],
            baseUri: ["'self'"],
            formAction: ["'self'"],
            frameAncestors: ["'none'"],
            upgradeInsecureRequests: [],
        },
    },
    // Additional security headers
    hsts: {
        maxAge: 31536000, // 1 year
        includeSubDomains: true,
        preload: true,
    },
    noSniff: true,
    xssFilter: true,
    referrerPolicy: { policy: "strict-origin-when-cross-origin" },
}));

app.use(compression());

// OBSERVABILITY: Correlation ID and request timing middleware
// Must be early in the middleware chain for accurate timing
app.use(correlationIdMiddleware);
app.use(requestTimingMiddleware);

// PRE-LOAD TEMPLATES INTO MEMORY
// This optimization prevents disk I/O on every page load
const HOMEPAGE_PATH = path.join(__dirname, '../homepage.html');
const TRACKRECORD_PATH = path.join(__dirname, '../track-record.html');
const THEMEPREVIEW_PATH = path.join(__dirname, '../theme-preview.html');
const ADMIN_PATH = path.join(__dirname, '../admin.html');
const SPACE_PATH = path.join(__dirname, '../space.html');
let HOMEPAGE_TEMPLATE = '';
let TRACKRECORD_TEMPLATE = '';
let THEMEPREVIEW_TEMPLATE = '';
let ADMIN_TEMPLATE = '';
let SPACE_TEMPLATE = '';

try {
    HOMEPAGE_TEMPLATE = fs.readFileSync(HOMEPAGE_PATH, 'utf8');
    logger.info('✅ Template: homepage.html loaded into memory');
} catch (e) {
    logger.error(`❌ Template Load Error: ${e.message}`);
    process.exit(1); // Fail fast if template is missing
}

try {
    TRACKRECORD_TEMPLATE = fs.readFileSync(TRACKRECORD_PATH, 'utf8');
    logger.info('✅ Template: track-record.html loaded into memory');
} catch (e) {
    logger.warn(`⚠️ Track record template not found: ${e.message}`);
    TRACKRECORD_TEMPLATE = ''; // Optional page, don't fail
}

try {
    THEMEPREVIEW_TEMPLATE = fs.readFileSync(THEMEPREVIEW_PATH, 'utf8');
    logger.info('✅ Template: theme-preview.html loaded into memory');
} catch (e) {
    logger.warn(`⚠️ Theme preview template not found: ${e.message}`);
    THEMEPREVIEW_TEMPLATE = ''; // Optional page, don't fail
}

try {
    ADMIN_TEMPLATE = fs.readFileSync(ADMIN_PATH, 'utf8');
    logger.info('✅ Template: admin.html loaded into memory');
} catch (e) {
    logger.warn(`⚠️ Admin template not found: ${e.message}`);
    ADMIN_TEMPLATE = ''; // Optional page, don't fail
}

try {
    SPACE_TEMPLATE = fs.readFileSync(SPACE_PATH, 'utf8');
    logger.info('✅ Template: space.html loaded into memory');
} catch (e) {
    logger.warn(`⚠️ Space template not found: ${e.message}`);
    SPACE_TEMPLATE = ''; // Optional page, don't fail
}

// CORS CONFIGURATION
const allowedOrigins = config.CORS_ORIGINS;

// SECURITY: Strict domain validation patterns (prevents subdomain spoofing)
// Pattern explanation:
// - ^https?:// = must start with http:// or https://
// - (www\.)? = optional www prefix
// - domain\.tld$ = exact domain match at end
const ALLOWED_DOMAIN_PATTERNS = [
    /^https?:\/\/(www\.)?alonisthe\.dev$/,           // alonisthe.dev (exact)
    /^https?:\/\/[a-z0-9-]+\.alonisthe\.dev$/,       // *.alonisthe.dev subdomains
    /^https?:\/\/localhost(:\d+)?$/,                  // localhost with optional port
    /^https?:\/\/127\.0\.0\.1(:\d+)?$/,              // 127.0.0.1 with optional port
    /^https?:\/\/[a-z0-9-]+\.squarespace\.com$/,     // *.squarespace.com
    /^https?:\/\/[a-z0-9-]+\.github\.dev$/,          // GitHub Codespaces
    /^https?:\/\/[a-z0-9-]+\.app\.github\.dev$/,     // GitHub Codespaces (port forwarding)
    /^https?:\/\/[a-z0-9-]+\.onrender\.com$/,        // Render deployments
];

function isOriginAllowed(origin) {
    if (!origin) return true; // Allow requests with no origin (server-to-server)

    // Check explicit config first
    if (allowedOrigins === '*') return true;
    if (Array.isArray(allowedOrigins) && allowedOrigins.includes(origin)) return true;

    // Check against strict patterns
    return ALLOWED_DOMAIN_PATTERNS.some(pattern => pattern.test(origin));
}

const corsOptions = {
    origin: function (origin, callback) {
        if (isOriginAllowed(origin)) {
            return callback(null, true);
        } else {
            logger.warn(`⚠️ CORS Blocked Origin: ${origin}`);
            return callback(new Error('CORS not allowed'), false);
        }
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-auth', 'x-session-token', 'x-requested-with', 'Accept', 'x-api-key']
};

app.options('*', cors(corsOptions));
app.use(cors(corsOptions));

// SECURITY: Capture raw body for webhook signature verification
// Must be placed BEFORE express.json() to access the original request body
app.use('/webhook', express.json({
    limit: '100kb',
    verify: (req, _res, buf) => {
        req.rawBody = buf.toString('utf8');
    }
}));

// Standard JSON parsing for all other routes
app.use(express.json({ limit: '100kb' }));

// RATE LIMITING
app.set('trust proxy', 1);
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 500, 
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req, _res) => req.headers['x-forwarded-for'] || req.ip
});
app.use(limiter);

// --- DYNAMIC OG META TAGS ---
// Intercept /token/:mint requests to inject social metadata
app.get('/token/:mint', async (req, res, next) => {
    // If Accept header requests JSON (API call), skip this HTML handler
    if (req.headers.accept && (req.headers.accept.includes('application/json') || req.xhr)) {
        return next();
    }

    const { mint } = req.params;
    const db = getDB();

    try {
        const token = await db.get('SELECT name, symbol, image, k_score, hasCommunityUpdate FROM tokens WHERE mint = $1', [mint]);
        
        // Default Metadata
        let title = 'HolDex | The Terminal Fire Explorer';
        let desc = 'Real-time Solana token analytics, K-Score ratings, and direct RPC syncing.';
        let image = 'https://holdex.io/logo.png'; // Placeholder if no token image

        if (token) {
            const isVerified = (token.hasCommunityUpdate || token.hascommunityupdate) ? '✅' : '';
            const score = token.k_score ? `(K-Score: ${token.k_score})` : '';
            
            title = `${token.name} ($${token.symbol}) ${score} | HolDex`;
            desc = `Analyze ${token.name} on HolDex. ${isVerified} Community Verified. Real-time charts, holder analysis, and conviction scoring.`;
            if (token.image) image = token.image;
        }

        // Use In-Memory Template
        let html = HOMEPAGE_TEMPLATE;

        // SECURITY: Escape all user-controlled values to prevent XSS
        const safeTitle = escapeHtml(title);
        const safeDesc = escapeHtml(desc);
        const safeImage = escapeHtml(image);
        const safeMint = escapeHtml(mint);

        // Prepare Meta Tags (with escaped values)
        const metaTags = `
            <meta property="og:title" content="${safeTitle}" />
            <meta property="og:description" content="${safeDesc}" />
            <meta property="og:image" content="${safeImage}" />
            <meta property="og:url" content="https://holdex.io/token/${safeMint}" />
            <meta property="og:type" content="website" />
            <meta name="twitter:card" content="summary_large_image" />
            <meta name="twitter:title" content="${safeTitle}" />
            <meta name="twitter:description" content="${safeDesc}" />
            <meta name="twitter:image" content="${safeImage}" />
        `;

        // Inject into <head>
        html = html.replace('<head>', `<head>${metaTags}`);

        // Inject Client-Side Redirect Script (To enable SPA hash routing)
        // This converts /token/:mint -> /#token/:mint so the JS app loads the view
        // SECURITY: Use JSON.stringify for JavaScript context (escapes all special chars)
        const jsSafeMint = JSON.stringify(mint).slice(1, -1); // Remove quotes from stringify
        const redirectScript = `
            <script>
                if (!window.location.hash) {
                    history.replaceState(null, null, '/#token/${jsSafeMint}');
                }
            </script>
        `;
        html = html.replace('</body>', `${redirectScript}</body>`);

        res.send(html);

    } catch (e) {
        logger.error(`OG Generation Error: ${e.message}`);
        // Fallback to serving raw template
        res.send(HOMEPAGE_TEMPLATE);
    }
});

// --- SHORT URL FOR K-SCORE SHARING ---
// /k/:mint - Optimized for Twitter/social sharing with K-Score card image
// Supports: ?mode=simple (compact) or ?mode=full (default, with conviction breakdown)
app.get('/k/:mint', async (req, res) => {
    const { mint } = req.params;
    const mode = req.query.mode || 'full';
    const db = getDB();

    // Validate mint format
    if (!mint || mint.length < 32 || mint.length > 44) {
        return res.status(400).send('Invalid token address');
    }

    try {
        const token = await db.get(`
            SELECT name, symbol, image, k_score, hasCommunityUpdate,
                   conviction_accumulators, conviction_holders, conviction_reducers, conviction_extractors,
                   holders, marketcap
            FROM tokens WHERE mint = $1
        `, [mint]);

        const isVerified = token && (token.hasCommunityUpdate || token.hascommunityupdate);

        // Build OG metadata
        let title, desc, cardUrl;
        const baseUrl = config.API_URL || 'https://holdex-api.onrender.com';

        if (!token) {
            title = 'Token Not Found | HolDex K-Score';
            desc = 'This token has not been indexed yet. Submit it for community review at holdex.io';
            cardUrl = `${baseUrl}/api/token/${mint}/card.png?mode=${mode}`;
        } else if (!isVerified) {
            title = `${token.name} ($${token.symbol}) | Pending Verification`;
            desc = `K-Score not yet calculated. Submit ${token.symbol} for community review to unlock conviction analysis.`;
            cardUrl = `${baseUrl}/api/token/${mint}/card.png?mode=${mode}`;
        } else {
            const score = Math.round(token.k_score || 0);
            const tier = score >= 90 ? 'Diamond 💎' : score >= 80 ? 'Platinum 💠' : score >= 70 ? 'Gold 🥇' :
                        score >= 60 ? 'Silver 🥈' : score >= 50 ? 'Bronze 🥉' : score >= 40 ? 'Copper 🟤' :
                        score >= 20 ? 'Iron ⚫' : 'Rust 🔩';

            title = `${token.name} ($${token.symbol}) K-Score: ${score} ${tier}`;

            const total = (token.conviction_accumulators || 0) + (token.conviction_holders || 0) +
                         (token.conviction_reducers || 0) + (token.conviction_extractors || 0);
            const diamondHands = total > 0 ? Math.round(((token.conviction_accumulators || 0) + (token.conviction_holders || 0)) / total * 100) : 0;

            desc = `${tier} tier with ${diamondHands}% diamond hands. On-chain conviction analysis powered by HolDex.`;
            cardUrl = `${baseUrl}/api/token/${mint}/card.png?mode=${mode}`;
        }

        // SECURITY: Escape all values
        const safeTitle = escapeHtml(title);
        const safeDesc = escapeHtml(desc);
        const safeCardUrl = escapeHtml(cardUrl);
        const safeMint = escapeHtml(mint);

        let html = HOMEPAGE_TEMPLATE;

        const metaTags = `
            <meta property="og:title" content="${safeTitle}" />
            <meta property="og:description" content="${safeDesc}" />
            <meta property="og:image" content="${safeCardUrl}" />
            <meta property="og:image:width" content="1200" />
            <meta property="og:image:height" content="628" />
            <meta property="og:url" content="${baseUrl}/k/${safeMint}" />
            <meta property="og:type" content="website" />
            <meta property="og:site_name" content="HolDex K-Score" />
            <meta name="twitter:card" content="summary_large_image" />
            <meta name="twitter:title" content="${safeTitle}" />
            <meta name="twitter:description" content="${safeDesc}" />
            <meta name="twitter:image" content="${safeCardUrl}" />
            <meta name="twitter:image:alt" content="K-Score card for ${escapeHtml(token?.symbol || 'token')}" />
        `;

        html = html.replace('<head>', `<head>${metaTags}`);

        // Redirect to token page in SPA
        const jsSafeMint = JSON.stringify(mint).slice(1, -1);
        const redirectScript = `
            <script>
                if (!window.location.hash) {
                    history.replaceState(null, null, '/#token/${jsSafeMint}');
                }
            </script>
        `;
        html = html.replace('</body>', `${redirectScript}</body>`);

        res.send(html);

    } catch (e) {
        logger.error(`K-Score Share Error: ${e.message}`);
        res.send(HOMEPAGE_TEMPLATE);
    }
});

// Serve Root - Crucial for handling /#token/... links
app.get('/', (req, res) => {
    res.send(HOMEPAGE_TEMPLATE);
});

// Serve Track Record page
app.get('/track-record.html', (req, res) => {
    if (TRACKRECORD_TEMPLATE) {
        res.send(TRACKRECORD_TEMPLATE);
    } else {
        res.status(404).send('Track record page not available');
    }
});

// Serve Theme Preview page (for cult vote)
app.get('/theme-preview.html', (req, res) => {
    if (THEMEPREVIEW_TEMPLATE) {
        res.send(THEMEPREVIEW_TEMPLATE);
    } else {
        res.status(404).send('Theme preview page not available');
    }
});

// Serve Admin Dashboard - "this is fine" style 🐕‍🦺🔥
app.get('/admin', (req, res) => {
    if (ADMIN_TEMPLATE) {
        res.send(ADMIN_TEMPLATE);
    } else {
        res.status(404).send('Admin page not available');
    }
});

// Serve Cult Member Space - "Hold to enter. Grants unlock features."
app.get('/cult-space', (req, res) => {
    if (SPACE_TEMPLATE) {
        res.send(SPACE_TEMPLATE);
    } else {
        res.status(404).send('Space page not available');
    }
});

// ============================================
// HEALTH CHECK ENDPOINTS (Kubernetes-compatible)
// ============================================

/**
 * Liveness Probe: /live
 * Returns 200 if the process is running.
 * Used by K8s to determine if the container should be restarted.
 */
app.get('/live', (_req, res) => {
    res.status(200).json({
        status: 'alive',
        timestamp: new Date().toISOString()
    });
});

/**
 * Readiness Probe: /ready
 * Returns 200 if the service is ready to accept traffic.
 * Checks database and Redis connectivity.
 */
app.get('/ready', async (_req, res) => {
    const { isHealthy: isRedisHealthy, getCircuitBreakerState: getRedisCircuitBreaker } = require('./services/redis');
    const { getAllCircuitBreakerStates } = require('./utils/circuitBreaker');

    const checks = {
        database: false,
        redis: false,
        circuitBreakers: {}
    };

    // Check database
    try {
        const db = getDB();
        if (db && db.isHealthy && db.isHealthy()) {
            await db.get('SELECT 1');
            checks.database = true;
        } else if (db) {
            await db.get('SELECT 1');
            checks.database = true;
        }
    } catch (_e) {
        checks.database = false;
    }

    // Check Redis
    checks.redis = isRedisHealthy();

    // Get all circuit breaker states
    checks.circuitBreakers = getAllCircuitBreakerStates();

    // Determine overall readiness
    // Service is ready if database is available (Redis is optional)
    const isReady = checks.database;

    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'ready' : 'not_ready',
        timestamp: new Date().toISOString(),
        checks,
        degraded: !checks.redis || Object.values(checks.circuitBreakers).some(cb => cb?.state === 'open')
    });
});

/**
 * Comprehensive Health Check: /health
 * Returns detailed system status for monitoring dashboards.
 */
app.get('/health', async (_req, res) => {
    const { isHealthy: isRedisHealthy, getCircuitBreakerState: getRedisCircuitBreaker } = require('./services/redis');
    const { getAllCircuitBreakerStates, isSystemDegraded } = require('./utils/circuitBreaker');

    const startTime = Date.now();
    const health = {
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        checks: {
            database: { status: 'unknown', latency: null },
            redis: { status: 'unknown', latency: null }
        },
        circuitBreakers: getAllCircuitBreakerStates(),
        degraded: false
    };

    // Database health check with latency
    try {
        const dbStart = Date.now();
        const db = getDB();
        await db.get('SELECT 1');
        health.checks.database = {
            status: 'healthy',
            latency: Date.now() - dbStart,
            circuitBreaker: db.getCircuitBreakerStates ? db.getCircuitBreakerStates() : null
        };
    } catch (e) {
        health.checks.database = {
            status: 'unhealthy',
            error: e.message,
            latency: null
        };
        health.status = 'unhealthy';
    }

    // Redis health check with latency
    try {
        const redisStart = Date.now();
        const { getClient } = require('./services/redis');
        const redis = getClient();
        if (redis) {
            await redis.ping();
            health.checks.redis = {
                status: 'healthy',
                latency: Date.now() - redisStart,
                circuitBreaker: getRedisCircuitBreaker()
            };
        } else {
            health.checks.redis = {
                status: 'unavailable',
                latency: null
            };
            health.degraded = true;
        }
    } catch (e) {
        health.checks.redis = {
            status: 'unhealthy',
            error: e.message,
            latency: null
        };
        health.degraded = true;
    }

    // Check if system is degraded due to circuit breakers
    if (isSystemDegraded()) {
        health.degraded = true;
    }

    // Overall status
    if (health.checks.database.status !== 'healthy') {
        health.status = 'unhealthy';
    } else if (health.degraded) {
        health.status = 'degraded';
    }

    health.responseTime = Date.now() - startTime;

    res.status(health.status === 'unhealthy' ? 503 : 200).json(health);
});

// Handle /about and /update for direct linking (with redirect to hash)
app.get(['/about', '/update'], (req, res) => {
    try {
        let html = HOMEPAGE_TEMPLATE;
        const pathName = req.path.replace('/', '');
        const redirectScript = `<script>if(!window.location.hash) history.replaceState(null, null, '/#${pathName}');</script>`;
        html = html.replace('</body>', `${redirectScript}</body>`);
        res.send(html);
    } catch (_e) {
        res.send(HOMEPAGE_TEMPLATE);
    }
});

async function startServer() {
    try {
        logger.info('🚀 System: Initializing HolDEX API...');
        
        await initDB();
        await connectRedis();

        // Start WebSocket Server
        initSocket(server, allowedOrigins);

        // Initialize Helius Webhooks (if configured)
        if (config.USE_WEBHOOKS) {
            const webhookUrl = config.WEBHOOK_URL || `${config.API_URL}/webhook/transfers`;
            logger.info(`[Webhook] Initializing master webhook → ${webhookUrl}`);
            try {
                const webhookId = await getOrCreateMasterWebhook(getDB(), webhookUrl);
                if (webhookId) {
                    logger.info(`[Webhook] Master webhook active: ${webhookId}`);
                }
            } catch (err) {
                logger.error(`[Webhook] Failed to initialize: ${err.message}`);
                // Don't crash - webhooks are optional, fall back to polling
            }
        } else {
            logger.warn('[Webhook] Webhooks disabled (set API_URL or WEBHOOK_URL to enable)');
        }

        // BACKGROUND TASKS MOVED TO CALCULATOR WORKER
        // Snapshotter and PriceWorker now run on Calculator service to improve API responsiveness.
        // The Calculator handles all heavy computation (K-Score, Metadata, Snapshotter, PriceWorker).
        // This keeps the API server lightweight and focused on request handling.

        // ARCHITECTURE FIX: kScoreUpdater.start() REMOVED from API
        // K-Score periodic calculations run ONLY on Calculator service to prevent race conditions.
        // API still has access to updateSingleToken() for on-demand recalcs (admin, approvals, etc.)
        // Calculator = sole authority for K-Score calculations
        // API = verification only (integrityWatchdog)

        // DISABLED: Integrity watchdog is too aggressive for single-backend architecture
        // It flags legitimate data updates (bug fixes, indexer updates) as "tampering"
        // The system is designed for multi-node Byzantine fault tolerance, but we have a trusted backend
        // Signatures are still generated and available via /api/token/:mint/verify endpoint
        // integrityWatchdog.start({ db: getDB() });
        // integrityWatchdog.startNodeWatchdog({ db: getDB() });

        // Initialize Routes
        app.use('/api', tokensRoutes.init({ db: getDB() }));
        app.use('/webhook', webhooksRoutes.init({ db: getDB() }));
        app.use('/oracle', oracleRoutes.init({ db: getDB(), logger }));
        app.use('/space', spaceRoutes.init({ db: getDB(), logger }));

        // Node Network Routes
        app.set('db', getDB()); // Make db available to routes
        app.use('/api/nodes', nodesRoutes);
        app.use('/internal', nodesRoutes.internalRouter);

        // Initialize this node in the network
        await nodeService.initializeNode(getDB());

        // Start heartbeat interval (every 60 seconds)
        heartbeatInterval = setInterval(async () => {
            await nodeService.sendHeartbeat(getDB());
            await nodeService.updateNodeStatuses(getDB());
        }, 60 * 1000);

        // Send initial heartbeat
        await nodeService.sendHeartbeat(getDB());

        app.use((err, req, res, _next) => {
            logger.error(`🔥 Unhandled Server Error: ${err.message}`);
            logger.error(err.stack);
            if (!res.headersSent) {
                res.status(500).json({ success: false, error: 'Internal Server Error' });
            }
        });

        const PORT = process.env.PORT || 3000;
        server.listen(PORT, () => {
            logger.info(`✅ API & Socket: Listening on port ${PORT}`);
            // Send startup alert (fire-and-forget)
            alerting.sendAlert({
                title: 'HolDex API Started',
                message: `Server listening on port ${PORT}`,
                severity: 'INFO',
                type: 'info',
                key: 'startup'
            }).catch(() => {});
        });

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

// ============================================
// GRACEFUL SHUTDOWN HANDLERS
// ============================================
let isShuttingDown = false;
let activeConnections = new Set();
let heartbeatInterval = null;
// MOVED TO CALCULATOR: let priceWorkerRef = null;

async function gracefulShutdown(signal) {
    if (isShuttingDown) {
        logger.warn(`⚠️ Shutdown: Already in progress, ignoring ${signal}`);
        return;
    }

    isShuttingDown = true;
    logger.info(`🛑 Shutdown: Received ${signal}, starting graceful shutdown...`);

    // 1. Stop accepting new connections
    server.close((err) => {
        if (err) {
            logger.error(`❌ Shutdown: Server close error: ${err.message}`);
        } else {
            logger.info('✅ Shutdown: Server stopped accepting connections');
        }
    });

    // 2. Clear intervals
    if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        logger.info('✅ Shutdown: Heartbeat interval cleared');
    }

    // 3. Price worker now runs on Calculator service (moved for better API responsiveness)

    // 4. Close active WebSocket connections gracefully
    const { getIO } = require('./services/socket');
    const io = getIO();
    if (io) {
        io.close(() => {
            logger.info('✅ Shutdown: WebSocket server closed');
        });
    }

    // 5. Send shutdown alert
    try {
        await alerting.sendAlert({
            title: 'HolDex API Shutting Down',
            message: `Server received ${signal}, gracefully shutting down`,
            severity: 'WARN',
            type: 'warning',
            key: 'shutdown'
        });
    } catch (_e) {
        // Ignore alert errors during shutdown
    }

    // 6. Wait for existing requests to complete (max 10 seconds)
    logger.info('⏳ Shutdown: Waiting up to 10s for requests to complete...');
    await new Promise(resolve => setTimeout(resolve, 2000));

    // 7. Final cleanup
    logger.info('✅ Shutdown: Graceful shutdown complete');
    process.exit(0);
}

// Register shutdown handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
    logger.error(`🔥 Uncaught Exception: ${err.message}`);
    logger.error(err.stack);
    gracefulShutdown('uncaughtException');
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    logger.error(`🔥 Unhandled Rejection at: ${promise}, reason: ${reason}`);
    // Don't exit on unhandled rejections, but log them
});

startServer();
