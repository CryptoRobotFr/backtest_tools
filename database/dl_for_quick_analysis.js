const ccxt = require('ccxt');
const fs = require('fs')
const createCsvWriter = require('csv-writer').createArrayCsvWriter;
const exchange_limit = JSON.parse(fs.readFileSync('./database/exchange_limit.json', 'utf8'));
const tf_ms = JSON.parse(fs.readFileSync('./database/tf_ms.json', 'utf8'));
const coin_list = JSON.parse(fs.readFileSync('./database/coin_list.json', 'utf8'));


function date_to_timestamp(my_date) {
    my_date = my_date.split("-");
    let newDate = new Date(Date.UTC(my_date[2], my_date[1] - 1, my_date[0]));
    return newDate.getTime();
}

function timestamp_to_date(my_tf) {
    my_date = new Date(my_tf);
    str_date = `${my_date.getUTCDate()}-${my_date.getUTCMonth()+1}-${my_date.getUTCFullYear()} ${my_date.getUTCHours()}:${my_date.getUTCMinutes()}`
    return str_date;
}

function current_utc_date() {
    const now = new Date();
    now.toUTCString();
    now.toISOString();
    return Math.floor(now);
}

function eliminate_double_ts(arr) {
    let i,
        len = arr.length
    to_remove = []

    for (i = 1; i < len; i++) {
        if (arr[i][0] === arr[i - 1][0]) {
            to_remove.push(i)
                // arr.splice(i, 1);
                // len--;
        }
    }
    for (i = to_remove.length - 1; i >= 0; i--) {
        arr.splice(to_remove[i], 1);
    }
    return arr;
}

function delete_directory() {
    // directory path
    const dir = 'database/quick_analysis';

    // delete directory recursively
    try {
        fs.rmSync(dir, { recursive: true, force: true });

        console.log(`${dir} is deleted!`);
    } catch (err) {
        console.error(`Error while deleting ${dir}.`);
    }
}

const delay = millis => new Promise((resolve, reject) => {
    setTimeout(_ => resolve(), millis);
});

async function dl_one_symbol(exchange, timeframe, limit, current_symbol) {
    return new Promise(resolve => {
        since_date = current_utc_date() - limit * tf_ms[timeframe]
        exchange.fetchOHLCV(symbol = current_symbol, timeframe = timeframe, since = since_date, limit = limit).then(async result_ohlcv => {
            let file_pair = current_symbol.replace('/', '-');
            let dirpath = './database/quick_analysis/';
            let filepath = dirpath + file_pair + ".csv";

            await fs.promises.mkdir(dirpath, { recursive: true });

            const csvWriter = createCsvWriter({
                header: ['date', 'open', 'high', 'low', 'close', 'volume'],
                path: filepath
            });

            csvWriter.writeRecords(result_ohlcv) // returns a promise
                .then(() => {
                    // process.stdout.write(`\rSuccessfully downloaded ${result_ohlcv.length} candles in ${filepath}`);
                    resolve(true);
                }).catch(err => {
                    console.log(err);
                    resolve(false);
                });

        });
    });
}

async function get_all_coin(exchange, timeframe, limit) {
    let markets = await exchange.load_markets()
    let symbols = exchange.symbols
    symbols_usdt = symbols.filter(symbol => symbol.substr(-4) == "USDT");
    symbols_usdt = symbols_usdt.filter(symbol => symbol.includes("BULL") == false);
    symbols_usdt = symbols_usdt.filter(symbol => symbol.includes("DOWN") == false);
    symbols_usdt = symbols_usdt.filter(symbol => symbol.includes("UP") == false);
    symbols_usdt = symbols_usdt.filter(symbol => symbol.includes("BEAR") == false);
    let total_request = symbols_usdt.length
        // let total_request = 1
    let current_request = 0
    for (symbol in symbols_usdt) {
        dl_one_symbol(exchange, timeframe, limit, symbols_usdt[symbol]).then(resp => {
            current_request++
            // console.log(current_request)
            process.stdout.write(`\rLoading ${current_request}/${total_request} coins downloaded`);
        }).catch(err => {
            current_request++
            console.log(err)
        })
    }
}


delete_directory()
let exchange = new ccxt.binance({ enableRateLimit: true })
get_all_coin(exchange, "1h", 1000)
