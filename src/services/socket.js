const { Server } = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const { createClient } = require('redis');
const config = require('../config/env');
const logger = require('./logger');

let io;

async function initSocket(server) {
    // 1. Initialize Socket.io
    io = new Server(server, {
        cors: {
            origin: config.CORS_ORIGINS || "*", 
            methods: ["GET", "POST"]
        },
        transports: ['websocket', 'polling'],
        path: '/socket.io'
    });

    // 2. Setup Redis Adapter for Scaling (Cluster Mode)
    try {
        const pubClient = createClient({ url: config.REDIS_URL || 'redis://redis:6379' });
        const subClient = pubClient.duplicate();

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
        // Room Management for Token Subscriptions
        socket.on('subscribe', (mint) => {
            if (mint && typeof mint === 'string') {
                // Must match the room name used in broadcastTokenUpdate
                socket.join(`token:${mint}`);
            }
        });

        socket.on('unsubscribe', (mint) => {
            if (mint) socket.leave(`token:${mint}`);
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

module.exports = { initSocket, getIO, broadcastTokenUpdate };
