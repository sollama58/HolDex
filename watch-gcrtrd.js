const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const CHECK_INTERVAL = 30000; // 30 seconds
let lastStatus = 'offline';

async function checkNode() {
    try {
        const result = await pool.query(`
            SELECT node_id, status, last_heartbeat, credits
            FROM nodes
            WHERE node_id = 'gcrtrd'
        `);

        if (result.rows.length === 0) {
            console.log(`[${new Date().toISOString()}] gcrtrd: not in database`);
            return;
        }

        const node = result.rows[0];
        const age = Date.now() - node.last_heartbeat;
        const ageSeconds = Math.round(age / 1000);

        // Consider online if heartbeat within last 60 seconds
        const isOnline = age < 60000 && node.status === 'active';

        if (isOnline && lastStatus !== 'online') {
            console.log('\n' + '='.repeat(60));
            console.log('🎉 gcrtrd NODE IS NOW ONLINE!');
            console.log('='.repeat(60));
            console.log(`Status: ${node.status}`);
            console.log(`Last heartbeat: ${ageSeconds}s ago`);
            console.log(`Credits: ${node.credits}`);
            console.log('='.repeat(60) + '\n');
            lastStatus = 'online';

            // Check network status
            const network = await pool.query(`
                SELECT COUNT(*) as active FROM nodes
                WHERE status = 'active' AND is_genesis = TRUE
            `);
            console.log(`Genesis nodes active: ${network.rows[0].active}/2`);

            if (network.rows[0].active >= 2) {
                console.log('✅ CONSENSUS NOW POSSIBLE - Both genesis nodes online!');
            }
        } else if (!isOnline) {
            console.log(`[${new Date().toISOString()}] gcrtrd: ${node.status} (heartbeat ${ageSeconds}s ago)`);
            lastStatus = 'offline';
        }
    } catch (err) {
        console.error('Error:', err.message);
    }
}

console.log('👀 Watching for gcrtrd node to come online...');
console.log(`   Checking every ${CHECK_INTERVAL/1000}s`);
console.log('   Press Ctrl+C to stop\n');

checkNode();
setInterval(checkNode, CHECK_INTERVAL);
