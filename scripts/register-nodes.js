#!/usr/bin/env node
/**
 * Register Initial Nodes
 *
 * Registers the 2 founding HolDex node operators:
 * - zeyxx (primary maintainer)
 * - gcrtrd/sollama58 (secondary node)
 *
 * Run: NODE_ENV=production node scripts/register-nodes.js
 */

require('dotenv').config();
const { Pool } = require('pg');

const NODES = [
    {
        node_id: 'node-zeyxx-001',
        name: 'zeyxx-primary',
        operator: 'zeyxx', // Replace with actual wallet address
        api_url: 'https://holdex-api.onrender.com',
        region: 'us-oregon'
    },
    {
        node_id: 'node-gcrtrd-001',
        name: 'gcrtrd-secondary',
        operator: 'sollama58', // Replace with actual wallet address
        api_url: null, // Set when known
        region: 'us-oregon'
    }
];

async function main() {
    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
    });

    try {
        console.log('🌐 Registering founding nodes...\n');

        for (const node of NODES) {
            const result = await pool.query(`
                INSERT INTO nodes (node_id, name, operator, api_url, region, status, joined_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, 'pending', $6, $6)
                ON CONFLICT (node_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    operator = EXCLUDED.operator,
                    api_url = COALESCE(EXCLUDED.api_url, nodes.api_url),
                    region = EXCLUDED.region,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
            `, [node.node_id, node.name, node.operator, node.api_url, node.region, Date.now()]);

            console.log(`✅ Registered: ${node.node_id}`);
            console.log(`   Name: ${node.name}`);
            console.log(`   Operator: ${node.operator}`);
            console.log(`   Region: ${node.region}`);
            console.log(`   Status: ${result.rows[0].status}\n`);
        }

        // Show all nodes
        const allNodes = await pool.query('SELECT * FROM nodes ORDER BY joined_at');
        console.log('📊 All registered nodes:');
        console.table(allNodes.rows.map(n => ({
            node_id: n.node_id,
            name: n.name,
            operator: n.operator,
            status: n.status,
            region: n.region
        })));

    } catch (e) {
        console.error('❌ Error:', e.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

main();
