const { Client } = require('pg');
const sql = require('mssql');
const fs = require('fs');

// PostgreSQL connection config for Product API
const pgConfig = {
    user: process.env.PG_USER || 'your_pg_username',
    host: process.env.PG_HOST || 'localhost',
    database: process.env.PG_DATABASE || 'product_api',
    password: process.env.PG_PASSWORD || 'your_pg_password',
    port: process.env.PG_PORT || 5432,
};

// SQL Server connection config for Publisher
const sqlConfig = {
    user: process.env.SQL_USER || 'your_sql_username',
    password: process.env.SQL_PASSWORD || 'your_sql_password',
    server: process.env.SQL_SERVER || 'localhost',
    database: process.env.SQL_DATABASE || 'publisher_db',
    authentication: {
        type: 'default'
    },
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

// Fetch published product count from PostgreSQL
async function fetchPublishedProductCount() {
    const client = new Client(pgConfig);
    try {
        await client.connect();
        const res = await client.query('SELECT COUNT(*) FROM products WHERE status = \\'published\\'');
        const count = parseInt(res.rows[0].count, 10);
        console.log(`PostgreSQL (Product API): ${count} published products`);
        return count;
    } catch (err) {
        console.error('PostgreSQL error:', err.message);
        throw err;
    } finally {
        await client.end();
    }
}

// Fetch synced product count from SQL Server
async function fetchSyncedProductCount() {
    try {
        await sql.connect(sqlConfig);
        const result = await sql.query('SELECT COUNT(*) AS count FROM synced_products');
        const count = result.recordset[0].count;
        console.log(`SQL Server (Publisher): ${count} synced products`);
        return count;
    } catch (err) {
        console.error('SQL Server error:', err.message);
        throw err;
    } finally {
        await sql.close();
    }
}

// Compare product counts and generate report
async function compareProductCounts() {
    try {
        const publishedCount = await fetchPublishedProductCount();
        const syncedCount = await fetchSyncedProductCount();
        const discrepancy = publishedCount - syncedCount;
        const timestamp = new Date().toISOString();
        let reportLine;
        if (publishedCount === syncedCount) {
            reportLine = `[${timestamp}] SUCCESS - Counts match: ${publishedCount} products`;
        } else {
            reportLine = `[${timestamp}] DISCREPANCY - Published: ${publishedCount}, Synced: ${syncedCount}, Difference: ${discrepancy}`;
        }
        console.log(reportLine);
        // Log result to file
        fs.appendFileSync('productCountTally.log', reportLine + '\n');
    } catch (err) {
        const errorLog = `[${new Date().toISOString()}] ERROR - ${err.message}`;
        console.error(errorLog);
        fs.appendFileSync('productCountTally.log', errorLog + '\n');
    }
}

// Run the comparison
compareProductCounts();