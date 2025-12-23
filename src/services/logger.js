/**
 * Simple Logger Service
 * standardized logging with timestamps
 */

const formatMessage = (level, message, meta) => {
    const timestamp = new Date().toISOString();
    const metaStr = meta && Object.keys(meta).length ? JSON.stringify(meta) : '';
    return `[${timestamp}] [${level}] ${message} ${metaStr}`;
};

const logger = {
    info: (message, meta = {}) => {
        console.log(formatMessage('INFO', message, meta));
    },
    warn: (message, meta = {}) => {
        console.warn(formatMessage('WARN', message, meta));
    },
    error: (message, meta = {}) => {
        console.error(formatMessage('ERROR', message, meta));
    },
    debug: (message, meta = {}) => {
        if (process.env.NODE_ENV === 'development') {
            console.debug(formatMessage('DEBUG', message, meta));
        }
    }
};

module.exports = logger;
