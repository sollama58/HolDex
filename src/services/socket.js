const { Server } = require('socket.io');
const logger = require('./logger');

let io = null;

function initSocket(httpServer, corsOrigins) {
    if (io) return io;

    const allowedOrigins = corsOrigins;
    // Helper to match logic in main app
    const isAllowed = (origin) => {
        if (!origin) return true;
        if (allowedOrigins === '*' || (Array.isArray(allowedOrigins) && allowedOrigins.includes(origin))) return true;
        if (origin.includes('localhost') || origin.includes('alonisthe.dev')) return true;
        return false;
    };

    io = new Server(httpServer, {
        cors: {
            origin: (origin, callback) => {
                if (isAllowed(origin)) callback(null, true);
                else callback(new Error('Not allowed by CORS'));
            },
            methods: ["GET", "POST"]
        },
        path: '/socket.io'
    });

    io.on('connection', (socket) => {
        // logger.info(`🔌 Socket Connected: ${socket.id}`);

        // Room Management for Token Subscriptions
        socket.on('subscribe', (mint) => {
            if (mint && typeof mint === 'string') {
                socket.join(`token:${mint}`);
                // logger.info(`Socket ${socket.id} joined token:${mint}`);
            }
        });

        socket.on('unsubscribe', (mint) => {
            if (mint) socket.leave(`token:${mint}`);
        });

        socket.on('disconnect', () => {
            // logger.info(`Socket Disconnected: ${socket.id}`);
        });
    });

    logger.info("📡 WebSocket Server Initialized");
    return io;
}

function getIO() {
    return io;
}

// Helper to broadcast updates
function broadcastTokenUpdate(mint, data) {
    if (!io) return;
    io.to(`token:${mint}`).emit('update', data);
}

module.exports = { initSocket, getIO, broadcastTokenUpdate };
