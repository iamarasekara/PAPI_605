'use strict';

/**
 * validateProducts.js
 *
 * Product (Trip) Data Integrity & Consistency Verification – Production
 *
 * Covers all mind-map branches:
 *   PREPARATION   → DB connections, scope definition
 *   DATA DOMAINS  → Products, Itineraries, Policies, Departures, Related Products, Prices
 *   VERIFICATION  → Count checks, field-level comparison, referential integrity, sync status
 *   EXECUTION     → Cross-check Starship/Elements (SQL Server) vs Product API (PostgreSQL)
 *   FINDINGS      → Discrepancy report written to JSON + log
 *   ESCALATION    → Exit code 1 when gaps found so CI/alerting can trigger re-sync request
 */

const { Client } = require('pg');
const sql = require('mssql');
const fs = require('fs');
const path = require('path');

// ─────────────────────────────────────────────────────────────────────────────
// FAIL FAST – require all connection environment variables at startup
// ─────────────────────────────────────────────────────────────────────────────

const REQUIRED_ENV = [
    'PG_USER', 'PG_HOST', 'PG_DATABASE', 'PG_PASSWORD',
    'SQL_USER', 'SQL_PASSWORD', 'SQL_SERVER', 'SQL_DATABASE',
];
const missingEnv = REQUIRED_ENV.filter(k => !process.env[k]);
if (missingEnv.length > 0) {
    console.error(`Missing required environment variables: ${missingEnv.join(', ')}`);
    process.exit(2);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTION CONFIGS  (values injected via environment variables)
// ─────────────────────────────────────────────────────────────────────────────

const pgConfig = {
    user:     process.env.PG_USER,
    host:     process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port:     parseInt(process.env.PG_PORT || '5432', 10),
};

const sqlConfig = {
    user:     process.env.SQL_USER,
    password: process.env.SQL_PASSWORD,
    server:   process.env.SQL_SERVER,
    database: process.env.SQL_DATABASE,
    authentication: { type: 'default' },
    options: {
        encrypt: true,
        // Set SQL_TRUST_SERVER_CERT=true only in non-production environments
        trustServerCertificate: process.env.SQL_TRUST_SERVER_CERT === 'true',
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUT FILES
// ─────────────────────────────────────────────────────────────────────────────

const LOG_FILE    = path.join(__dirname, 'validateProducts.log');
const REPORT_FILE = path.join(__dirname, 'validateProducts_report.json');

function log(message) {
    const line = `[${new Date().toISOString()}] ${message}`;
    console.log(line);
    fs.appendFileSync(LOG_FILE, line + '\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER – run a single PostgreSQL query and return rows
// ─────────────────────────────────────────────────────────────────────────────

async function pgQuery(client, queryText, params = []) {
    const res = await client.query(queryText, params);
    return res.rows;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 – COUNT CHECKS
//   Compare record totals for each data domain between the two systems.
//   Table names come from a hardcoded whitelist to prevent SQL injection.
// ─────────────────────────────────────────────────────────────────────────────

// Whitelisted table mappings – never accept user/external input for table names
const DOMAIN_TABLE_MAP = [
    { label: 'Products',         pgTable: 'products',         sqlTable: 'synced_products'         },
    { label: 'Itineraries',      pgTable: 'itineraries',      sqlTable: 'synced_itineraries'       },
    { label: 'Policies',         pgTable: 'policies',         sqlTable: 'synced_policies'          },
    { label: 'Departures',       pgTable: 'departures',       sqlTable: 'synced_departures'        },
    { label: 'Related Products', pgTable: 'related_products', sqlTable: 'synced_related_products'  },
    { label: 'Prices',           pgTable: 'prices',           sqlTable: 'synced_prices'            },
];

const ALLOWED_PG_TABLES  = new Set(DOMAIN_TABLE_MAP.map(d => d.pgTable));
const ALLOWED_SQL_TABLES = new Set(DOMAIN_TABLE_MAP.map(d => d.sqlTable));

function assertAllowedPgTable(name) {
    if (!ALLOWED_PG_TABLES.has(name)) {
        throw new Error(`Table name '${name}' is not in the allowed PostgreSQL table list`);
    }
}

function assertAllowedSqlTable(name) {
    if (!ALLOWED_SQL_TABLES.has(name)) {
        throw new Error(`Table name '${name}' is not in the allowed SQL Server table list`);
    }
}

async function runCountChecks(pgClient) {
    log('--- COUNT CHECKS ---');
    const results = [];

    for (const domain of DOMAIN_TABLE_MAP) {
        try {
            assertAllowedPgTable(domain.pgTable);
            assertAllowedSqlTable(domain.sqlTable);

            const pgRows  = await pgQuery(pgClient, `SELECT COUNT(*) AS cnt FROM ${domain.pgTable}`);
            const pgCount = parseInt(pgRows[0].cnt, 10);

            const sqlResult = await sql.query(`SELECT COUNT(*) AS cnt FROM ${domain.sqlTable}`);
            const sqlCount  = sqlResult.recordset[0].cnt;

            const diff   = pgCount - sqlCount;
            const status = diff === 0 ? 'MATCH' : 'MISMATCH';

            log(`[COUNT] ${domain.label}: ProductAPI=${pgCount}, Starship/Elements=${sqlCount}, Diff=${diff} → ${status}`);
            results.push({ domain: domain.label, pgCount, sqlCount, diff, status });
        } catch (err) {
            log(`[COUNT] ${domain.label}: ERROR – ${err.message}`);
            results.push({ domain: domain.label, error: err.message });
        }
    }

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 – FIELD-LEVEL COMPARISON (Products core fields)
//   Detect null / missing values and field-level mismatches on core product data.
// ─────────────────────────────────────────────────────────────────────────────

async function runFieldLevelChecks(pgClient) {
    log('--- FIELD-LEVEL COMPARISON (Products) ---');
    const results = [];

    // 2a. Products with null name/title or status
    const nullFields = await pgQuery(pgClient, `
        SELECT id, name, status
        FROM   products
        WHERE  name IS NULL OR status IS NULL
    `);
    log(`[FIELD] Products with NULL name or status: ${nullFields.length}`);
    results.push({ check: 'null_name_or_status', count: nullFields.length, samples: nullFields.slice(0, 5) });

    // 2b. Products whose status differs between the two systems
    //     Fetch from each DB separately, then compare in application code.
    try {
        const papiStatusRows = await pgQuery(pgClient, `
            SELECT id, status FROM products
        `);
        const papiStatusMap = new Map(papiStatusRows.map(r => [String(r.id), r.status]));

        const starshipResult = await sql.query(`SELECT product_id, status FROM synced_products`);
        const mismatches = starshipResult.recordset.filter(r => {
            const papiStatus = papiStatusMap.get(String(r.product_id));
            return papiStatus !== undefined && papiStatus !== r.status;
        }).map(r => ({
            product_id:      r.product_id,
            starship_status: r.status,
            papi_status:     papiStatusMap.get(String(r.product_id)),
        }));

        log(`[FIELD] Products with status mismatch: ${mismatches.length}`);
        results.push({ check: 'status_mismatch', count: mismatches.length, samples: mismatches.slice(0, 5) });
    } catch (err) {
        log(`[FIELD] status_mismatch check error: ${err.message}`);
        results.push({ check: 'status_mismatch', error: err.message });
    }

    // 2c. Itinerary records missing sequence order
    const nullSeq = await pgQuery(pgClient, `
        SELECT id, product_id, sequence_order
        FROM   itineraries
        WHERE  sequence_order IS NULL
    `);
    log(`[FIELD] Itineraries with NULL sequence_order: ${nullSeq.length}`);
    results.push({ check: 'itinerary_null_sequence', count: nullSeq.length, samples: nullSeq.slice(0, 5) });

    // 2d. Departures with missing date or capacity
    const nullDep = await pgQuery(pgClient, `
        SELECT id, product_id, departure_date, capacity
        FROM   departures
        WHERE  departure_date IS NULL OR capacity IS NULL
    `);
    log(`[FIELD] Departures with NULL date or capacity: ${nullDep.length}`);
    results.push({ check: 'departure_null_date_or_capacity', count: nullDep.length, samples: nullDep.slice(0, 5) });

    // 2e. Prices with missing base amount or currency
    const nullPrice = await pgQuery(pgClient, `
        SELECT id, product_id, base_amount, currency
        FROM   prices
        WHERE  base_amount IS NULL OR currency IS NULL
    `);
    log(`[FIELD] Prices with NULL base_amount or currency: ${nullPrice.length}`);
    results.push({ check: 'price_null_amount_or_currency', count: nullPrice.length, samples: nullPrice.slice(0, 5) });

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 – REFERENTIAL INTEGRITY
//   Detect orphan records and missing foreign keys.
// ─────────────────────────────────────────────────────────────────────────────

async function runReferentialIntegrityChecks(pgClient) {
    log('--- REFERENTIAL INTEGRITY ---');
    const results = [];

    const orphanChecks = [
        {
            label: 'Itineraries without parent product',
            query: `
                SELECT i.id, i.product_id
                FROM   itineraries i
                LEFT JOIN products p ON p.id = i.product_id
                WHERE  p.id IS NULL
            `,
        },
        {
            label: 'Policies without parent product',
            query: `
                SELECT pl.id, pl.product_id
                FROM   policies pl
                LEFT JOIN products p ON p.id = pl.product_id
                WHERE  p.id IS NULL
            `,
        },
        {
            label: 'Departures without parent product',
            query: `
                SELECT d.id, d.product_id
                FROM   departures d
                LEFT JOIN products p ON p.id = d.product_id
                WHERE  p.id IS NULL
            `,
        },
        {
            label: 'Related Products with missing child product',
            query: `
                SELECT rp.id, rp.product_id, rp.related_product_id
                FROM   related_products rp
                LEFT JOIN products p ON p.id = rp.related_product_id
                WHERE  p.id IS NULL
            `,
        },
        {
            label: 'Prices without parent product',
            query: `
                SELECT pr.id, pr.product_id
                FROM   prices pr
                LEFT JOIN products p ON p.id = pr.product_id
                WHERE  p.id IS NULL
            `,
        },
    ];

    for (const check of orphanChecks) {
        const rows = await pgQuery(pgClient, check.query);
        const status = rows.length === 0 ? 'OK' : 'ORPHANS_FOUND';
        log(`[RI] ${check.label}: ${rows.length} orphan(s) → ${status}`);
        results.push({ check: check.label, orphanCount: rows.length, status, samples: rows.slice(0, 5) });
    }

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 – SYNC STATUS
//   Identify products present in Starship/Elements but absent in Product API,
//   and vice-versa (source-of-truth gap analysis).
//   Product IDs are fetched from each system separately, then compared in JS
//   to avoid unsupported cross-database joins.
// ─────────────────────────────────────────────────────────────────────────────

async function runSyncStatusChecks(pgClient) {
    log('--- SYNC STATUS (Starship/Elements vs Product API) ---');
    const results = [];

    try {
        // Fetch all product IDs from both systems
        const papiIdRows     = await pgQuery(pgClient, `SELECT id FROM products`);
        const papiIds        = new Set(papiIdRows.map(r => String(r.id)));

        const starshipResult = await sql.query(`SELECT product_id FROM synced_products`);
        const starshipIds    = new Set(starshipResult.recordset.map(r => String(r.product_id)));

        // Products in Starship/Elements but NOT in Product API
        const missingInPapi = [...starshipIds].filter(id => !papiIds.has(id));
        log(`[SYNC] Products in Starship/Elements but missing from Product API: ${missingInPapi.length}`);
        results.push({
            check:   'missing_in_product_api',
            count:   missingInPapi.length,
            samples: missingInPapi.slice(0, 5),
        });

        // Published products in Product API not yet synced to Starship/Elements
        const publishedRows  = await pgQuery(pgClient, `SELECT id, name FROM products WHERE status = 'published'`);
        const unsyncedRows   = publishedRows.filter(r => !starshipIds.has(String(r.id)));
        log(`[SYNC] Published products in Product API not yet synced to Starship/Elements: ${unsyncedRows.length}`);
        results.push({
            check:   'unsynced_published_products',
            count:   unsyncedRows.length,
            samples: unsyncedRows.slice(0, 5),
        });
    } catch (err) {
        log(`[SYNC] Sync status check error: ${err.message}`);
        results.push({ check: 'sync_status', error: err.message });
    }

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 – DEPARTURES AVAILABILITY & CAPACITY SPOT CHECK
// ─────────────────────────────────────────────────────────────────────────────

async function runDepartureChecks(pgClient) {
    log('--- DEPARTURES: AVAILABILITY & CAPACITY ---');
    const results = [];

    // Departures with zero or negative capacity
    const zeroCapacity = await pgQuery(pgClient, `
        SELECT id, product_id, departure_date, capacity
        FROM   departures
        WHERE  capacity <= 0
    `);
    log(`[DEP] Departures with zero/negative capacity: ${zeroCapacity.length}`);
    results.push({ check: 'zero_or_negative_capacity', count: zeroCapacity.length, samples: zeroCapacity.slice(0, 5) });

    // Departures in the past that are still marked available
    const staleAvailable = await pgQuery(pgClient, `
        SELECT id, product_id, departure_date, availability_status
        FROM   departures
        WHERE  departure_date < NOW()
          AND  availability_status = 'available'
    `);
    log(`[DEP] Past departures still marked 'available': ${staleAvailable.length}`);
    results.push({ check: 'stale_available_departures', count: staleAvailable.length, samples: staleAvailable.slice(0, 5) });

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 – PRICE INTEGRITY (season/promo rules)
// ─────────────────────────────────────────────────────────────────────────────

async function runPriceIntegrityChecks(pgClient) {
    log('--- PRICE INTEGRITY ---');
    const results = [];

    // Prices where promo end date is before start date
    const invalidPromoDates = await pgQuery(pgClient, `
        SELECT id, product_id, promo_start_date, promo_end_date
        FROM   prices
        WHERE  promo_start_date IS NOT NULL
          AND  promo_end_date   IS NOT NULL
          AND  promo_end_date < promo_start_date
    `);
    log(`[PRICE] Prices with invalid promo date range: ${invalidPromoDates.length}`);
    results.push({ check: 'invalid_promo_date_range', count: invalidPromoDates.length, samples: invalidPromoDates.slice(0, 5) });

    // Prices with negative base amount
    const negativePrice = await pgQuery(pgClient, `
        SELECT id, product_id, base_amount, currency
        FROM   prices
        WHERE  base_amount < 0
    `);
    log(`[PRICE] Prices with negative base amount: ${negativePrice.length}`);
    results.push({ check: 'negative_base_amount', count: negativePrice.length, samples: negativePrice.slice(0, 5) });

    return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ORCHESTRATOR
// ─────────────────────────────────────────────────────────────────────────────

async function runValidation() {
    log('========== PRODUCT DATA INTEGRITY & CONSISTENCY VERIFICATION (PRODUCTION) ==========');

    const pgClient = new Client(pgConfig);
    const report = {
        timestamp:          new Date().toISOString(),
        environment:        'PRODUCTION',
        countChecks:        [],
        fieldLevelChecks:   [],
        referentialIntegrity: [],
        syncStatus:         [],
        departureChecks:    [],
        priceIntegrityChecks: [],
        summary:            {},
    };

    let hasDiscrepancies = false;

    try {
        await pgClient.connect();
        log('PostgreSQL (Product API) connection established.');

        await sql.connect(sqlConfig);
        log('SQL Server (Starship/Elements) connection established.');

        // Run all verification sections
        report.countChecks          = await runCountChecks(pgClient);
        report.fieldLevelChecks     = await runFieldLevelChecks(pgClient);
        report.referentialIntegrity = await runReferentialIntegrityChecks(pgClient);
        report.syncStatus           = await runSyncStatusChecks(pgClient);
        report.departureChecks      = await runDepartureChecks(pgClient);
        report.priceIntegrityChecks = await runPriceIntegrityChecks(pgClient);

        // ── Build summary ──────────────────────────────────────────────────────
        const countMismatches   = report.countChecks.filter(r => r.status === 'MISMATCH').length;
        const fieldIssues       = report.fieldLevelChecks.filter(r => r.count > 0).length;
        const riIssues          = report.referentialIntegrity.filter(r => r.orphanCount > 0).length;
        const syncGaps          = report.syncStatus.filter(r => r.count > 0).length;
        const depIssues         = report.departureChecks.filter(r => r.count > 0).length;
        const priceIssues       = report.priceIntegrityChecks.filter(r => r.count > 0).length;
        const totalIssues       = countMismatches + fieldIssues + riIssues + syncGaps + depIssues + priceIssues;

        hasDiscrepancies = totalIssues > 0;

        report.summary = {
            countMismatches,
            fieldIssues,
            referentialIntegrityIssues: riIssues,
            syncGaps,
            departureIssues:  depIssues,
            priceIssues,
            totalIssues,
            overallStatus: hasDiscrepancies ? 'DISCREPANCIES_FOUND' : 'ALL_CHECKS_PASSED',
        };

        log('========== SUMMARY ==========');
        log(`Count mismatches            : ${countMismatches}`);
        log(`Field-level issues          : ${fieldIssues}`);
        log(`Referential integrity issues: ${riIssues}`);
        log(`Sync gaps                   : ${syncGaps}`);
        log(`Departure issues            : ${depIssues}`);
        log(`Price integrity issues      : ${priceIssues}`);
        log(`─────────────────────────────`);
        log(`TOTAL ISSUES                : ${totalIssues}`);
        log(`OVERALL STATUS              : ${report.summary.overallStatus}`);

        if (hasDiscrepancies) {
            log('ACTION REQUIRED – Escalate to Data Engineering / Platform team for re-sync.');
        } else {
            log('✅ QA Signoff – Production consistency confirmed.');
        }

    } catch (err) {
        report.fatalError = err.message;
        log(`FATAL ERROR – ${err.message}`);
        hasDiscrepancies = true;
    } finally {
        try { await pgClient.end(); } catch (err) { log(`WARN – PostgreSQL disconnect error: ${err.message}`); }
        try { await sql.close();   } catch (err) { log(`WARN – SQL Server disconnect error: ${err.message}`); }

        // Write JSON report (for Confluence/JIRA attachment)
        fs.writeFileSync(REPORT_FILE, JSON.stringify(report, null, 2));
        log(`Full report written to: ${REPORT_FILE}`);
        log('========== VERIFICATION COMPLETE ==========');
    }

    // Exit 1 when discrepancies found so CI/alerting can trigger re-sync workflow
    if (hasDiscrepancies) {
        process.exit(1);
    }
}

// Run
runValidation();