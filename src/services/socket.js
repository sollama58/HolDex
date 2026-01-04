const { Server } = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const { createClient } = require('redis');
const config = require('../config/env');
const logger = require('./logger');

let io;
let pubClient = null;
let subClient = null;

async function initSocket(server) {
    // 1. Initialize Socket.io
    // SECURITY: Never fallback to wildcard - use explicit origins only
    const allowedOrigins = config.CORS_ORIGINS === '*' ? '*' : (config.CORS_ORIGINS || [
        'https://www.alonisthe.dev',
        'https://alonisthe.dev',
        'http://localhost:3000',
        'http://localhost:5173'
    ]);

    io = new Server(server, {
        cors: {
            origin: allowedOrigins,
            methods: ["GET", "POST"]
        },
        transports: ['websocket', 'polling'],
        path: '/socket.io'
    });

    // 2. Setup Redis Adapter for Scaling (Cluster Mode)
    try {
        pubClient = createClient({ url: config.REDIS_URL || 'redis://redis:6379' });
        subClient = pubClient.duplicate();

        // Handle Redis errors to prevent crashes
        pubClient.on('error', (err) => logger.error('Redis Pub Client Error', err));
        subClient.on('error', (err) => logger.error('Redis Sub Client Error', err));

        await Promise.all([pubClient.connect(), subClient.connect()]);

        io.adapter(createAdapter(pubClient, subClient));
        logger.info("🔌 Socket.io: Redis Adapter Connected (Cluster Mode Ready)");
    } catch (e) {
        logger.error("❌ Socket.io: Failed to connect to Redis adapter", e);
        // We continue without Redis (Memory Mode) if it fails, to keep the server alive
    }

    // 3. Handle Connections
    io.on('connection', (socket) => {
        // Track rooms per socket to prevent flooding
        let roomCount = 0;
        const MAX_ROOMS_PER_SOCKET = 50;

        // Room Management for Token Subscriptions
        socket.on('subscribe', (mint) => {
            // SECURITY: Validate mint format (base58, 32-44 chars)
            if (!mint || typeof mint !== 'string') return;
            if (mint.length < 32 || mint.length > 44) return;
            if (!/^[1-9A-HJ-NP-Za-km-z]+$/.test(mint)) return; // base58 chars only
            if (roomCount >= MAX_ROOMS_PER_SOCKET) return; // prevent room flooding

            socket.join(`token:${mint}`);
            roomCount++;
        });

        socket.on('unsubscribe', (mint) => {
            if (mint && typeof mint === 'string') {
                socket.leave(`token:${mint}`);
                if (roomCount > 0) roomCount--;
            }
        });
    });

    return io;
}

function getIO() {
    if (!io) {
        // Return a dummy object if IO isn't ready yet to prevent crashes in race conditions
        return { to: () => ({ emit: () => {} }) };
    }
    return io;
}

// RESTORED: Original function name and event signature
function broadcastTokenUpdate(mint, data) {
    if (!io) return;
    
    // Must match the room name in 'subscribe' and event name in homepage.html ('update')
    io.to(`token:${mint}`).emit('update', data);
}

/**
 * Cleanup function for graceful shutdown
 * Call this when the server is shutting down
 */
async function cleanupSocket() {
    if (io) {
        io.close();
        io = null;
    }
    if (pubClient) {
        try { await pubClient.quit(); } catch (_e) { /* ignore */ }
        pubClient = null;
    }
    if (subClient) {
        try { await subClient.quit(); } catch (_e) { /* ignore */ }
        subClient = null;
    }
    logger.info('🔌 Socket.io: Cleaned up');
}

module.exports = { initSocket, getIO, broadcastTokenUpdate, cleanupSocket };
