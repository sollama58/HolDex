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
const { startSnapshotter } = require('./indexer/tasks/snapshotter');
const kScoreUpdater = require('./tasks/kScoreUpdater'); 
// REMOVED: const { startNewTokenListener } = require('./services/new_token_listener'); 
const { initSocket } = require('./services/socket'); 
const tokensRoutes = require('./routes/tokens');
const webhooksRoutes = require('./routes/webhooks');
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
app.use(helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: false,
}));

app.use(compression());

// PRE-LOAD HOMEPAGE TEMPLATE INTO MEMORY
// This optimization prevents disk I/O on every page load
const HOMEPAGE_PATH = path.join(__dirname, '../homepage.html');
let HOMEPAGE_TEMPLATE = '';

try {
    HOMEPAGE_TEMPLATE = fs.readFileSync(HOMEPAGE_PATH, 'utf8');
    logger.info('✅ Template: homepage.html loaded into memory');
} catch (e) {
    logger.error(`❌ Template Load Error: ${e.message}`);
    process.exit(1); // Fail fast if template is missing
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
    allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-auth', 'x-requested-with', 'Accept', 'x-api-key']
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
        const redirectScript = `
            <script>
                if (!window.location.hash) {
                    history.replaceState(null, null, '/#token/${safeMint}');
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

// Serve Root - Crucial for handling /#token/... links
app.get('/', (req, res) => {
    res.send(HOMEPAGE_TEMPLATE);
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

        // Start Background Tasks
        startSnapshotter();
        kScoreUpdater.start({ db: getDB() });
        
        // Initialize Routes
        app.use('/api', tokensRoutes.init({ db: getDB() }));
        app.use('/webhook', webhooksRoutes.init({ db: getDB() }));

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
        });

    } catch (error) {
        logger.error('❌ System Fatal Error:', error);
        process.exit(1);
    }
}

startServer();
