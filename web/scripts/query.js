// Dotenv for environment configs
require('dotenv').config({ path: __dirname + '/../../.env' })

// Postgres client
const { Client } = require('pg');
const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: process.env.POSTGRES_PASSWORD,
    port: 5432,
});
client.connect();

// Data types handling from PSQL to JS
// From string back to numeric
var types = require('pg').types
types.setTypeParser(types.builtins.NUMERIC, (value) => {
    return parseFloat(value);
});

// Query
const select_query = `
with bq as (
    select exchange, base_id, quote_id
    from symbol_exchange
    where exchange='bitfinex' and symbol='ETHBTC'
 )
 select extract(epoch from oh.time) as "time",
    oh.opening_price as "open",
    oh.highest_price as "high",
    oh.lowest_price as "low",
    oh.closing_price as "close"
 from ohlcvs oh
    inner join bq on oh.exchange=bq.exchange
       and oh.base_id=bq.base_id and oh.quote_id=bq.quote_id
 limit 10;
`;

function queryCallback(r) {
    console.log(r)
};

async function query(q) {
    let result
    try {
        result = await client.query(q)
    } catch (err) {
        console.log('EXCEPTION: ' + err.stack)
        result = null
    } finally {
        client.end()
    }
    return result
};

async function run_query(q) {
    try {
        const { rows } = await query(q)
        return rows
    } catch (err) {
        console.log('EXCEPTION: ' + err.stack)
    }
};

// run_query(select_query)
run_query(select_query).then(r => queryCallback(r))