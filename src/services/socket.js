const { Server } = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const { createClient } = require('redis');
const config = require('../config/env');
const logger = require('./logger');

let io;

async function initSocket(server) {
    io = new Server(server, {
        cors: {
            origin: "*", 
            methods: ["GET", "POST"]
        },
        transports: ['websocket', 'polling'] 
    });

    // --- REDIS ADAPTER SETUP ---
    // This allows multiple API instances to talk to each other
    try {
        const pubClient = createClient({ url: config.REDIS_URL || 'redis://redis:6379' });
        const subClient = pubClient.duplicate();

        await pubClient.connect();
        await subClient.connect();

        io.adapter(createAdapter(pubClient, subClient));
        logger.info("🔌 Socket.io: Redis Adapter Connected");
    } catch (e) {
        logger.error("❌ Socket.io: Failed to connect to Redis adapter", e);
        // Fallback to memory adapter if Redis fails
    }

    io.on('connection', (socket) => {
        // logger.info(`Socket Connected: ${socket.id}`);
        
        // Allow clients to subscribe to specific tokens (rooms)
        socket.on('subscribe', (mint) => {
            if(mint) socket.join(mint);
        });

        socket.on('unsubscribe', (mint) => {
            if(mint) socket.leave(mint);
        });
    });

    return io;
}

function getIO() {
    if (!io) {
        throw new Error("Socket.io not initialized!");
    }
    return io;
}

// Helper to broadcast updates
// With the Redis adapter, this works even if the user is connected to a different server instance
function broadcastPriceUpdate(mint, data) {
    if (io) {
        io.to(mint).emit('price-update', data);
    }
}

module.exports = { initSocket, getIO, broadcastPriceUpdate };
