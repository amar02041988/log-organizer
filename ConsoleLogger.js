// This class is created as we cannot use the audit logging utils package to avoid any cyclic redundancy issues

class ConsoleLogger {
    constructor() {
        const level = (process.env.LOG_LEVEL || 'info').toLowerCase();
        this.levels = ['error', 'warn', 'info', 'debug'];
        this.currentLevelIndex = this.levels.indexOf(level);
        if (this.currentLevelIndex === -1) {
            this.currentLevelIndex = this.levels.indexOf('info'); // default to info
        }
    }

    log(level, ...args) {
        const levelIndex = this.levels.indexOf(level);
        if (levelIndex <= this.currentLevelIndex) {
            const timestamp = new Date().toISOString();
            console.log(`[${timestamp}] [${level.toUpperCase()}]`, ...args);
        }
    }

    
    error(...args) {
        this.log('error', ...args);
    }

    warn(...args) {
        this.log('warn', ...args);
    }

    info(...args) {
        this.log('info', ...args);
    }

    debug(...args) {
        this.log('debug', ...args);
    }
}

module.exports = ConsoleLogger;
