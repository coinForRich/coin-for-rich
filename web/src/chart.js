var exchange = document.getElementById("exchange").textContent
var base_id = document.getElementById("base_id").textContent
var quote_id = document.getElementById("quote_id").textContent
var start = 1609480800000
var end = 1612159200000
console.log(exchange, base_id, quote_id, start, end)

var data = null
var loading = false

var chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: 1200,
    height: 300,
    layout: {
        backgroundColor: '#ffffff',
        textColor: 'rgba(33, 56, 77, 1)',
    },
    grid: {
        vertLines: {
            color: 'rgba(197, 203, 206, 0.7)',
        },
        horzLines: {
            color: 'rgba(197, 203, 206, 0.7)',
        },
    },
    timeScale: {
        timeVisible: true,
        secondsVisible: false,
    },
});

var candleSeries = chart.addCandlestickSeries();
var timeScale = chart.timeScale();
var timer = null;

function dateToTimestamp(date) {
    return new Date(Date.UTC(date.year, date.month - 1, date.day, 0, 0, 0, 0));
}

// Returns timestamp `step_amt * steps` away from `milliseconds`
function getTimestampSteps(milliseconds, step_amt, steps) {
    const ret = milliseconds + step_amt * steps
    return ret
}

// Draw new candle series, merge data if there's new and existing data
function drawCandleSeries(candle_data) {
    if (candle_data !== null) {
        try {
            saveDataFromEndpoint(candle_data)
            candleSeries.setData(data)
        }
        catch(err) {
            console.log(err)
        }
    }
}

// Save response data from endpoint to `data`
function saveDataFromEndpoint(resp_data) {
    if (resp_data !== null) {
        if (data == null) {
            data = resp_data
            console.log("new data: ")
            console.log(data)
        }
        else {
            // Not sure if Set is good because it may
            //  mess up data order
            data = [...new Set([...resp_data, ...data])]
            console.log("data has been merged with new data: ")
            console.log(data)
        }
    }
    loading = false
}

// Read data from ohlcv endpoint and save to `data`
async function readDrawOHLC(exchange, base_id, quote_id, start, end) {
    loading = true
    let ohlcv_endpoint = 
        `http://${window.location.host}/ohlc/?exchange=${exchange}&base_id=${base_id}&quote_id=${quote_id}&start=${start}&end=${end}&mls=true`
    fetch(ohlcv_endpoint)
        .then(response => response.json())
        .then(resp_data => 
            drawCandleSeries(resp_data)
        )
}

// Initialize new chart
readDrawOHLC(exchange, base_id, quote_id, start, end)

// Update chart when viewport changes
timeScale.subscribeVisibleLogicalRangeChange(() => {
    if (timer !== null) {
        return;
    }
    timer = setTimeout(() => {
        var logicalRange = timeScale.getVisibleLogicalRange();
        if (logicalRange !== null && data !== undefined) {
            var barsInfo = candleSeries.barsInLogicalRange(logicalRange);
            console.log(barsInfo)
            if (barsInfo !== null && barsInfo.barsBefore < 100 && loading === false) {
                /* var firstTime = getBusinessDayBeforeCurrentAt(data[0].time, 1); */
                var oneMinBefore = getTimestampSteps(data[0].time * 1000, -60000, 1);
                /* var lastTime = getBusinessDayBeforeCurrentAt(firstTime, Math.max(100, -barsInfo.barsBefore + 100)); */
                var nMinBefore = getTimestampSteps(oneMinBefore, -60000, 100);
                console.log(oneMinBefore)
                console.log(nMinBefore)
                /* Read more data from db, then append to current data */
                readDrawOHLC(exchange, base_id, quote_id, nMinBefore, oneMinBefore)
            }
        }
        timer = null;
    }, 50);
});


// Time range switcher
// function createSimpleSwitcher(items, activeItem, activeItemChangedCallback) {
// 	var switcherElement = document.createElement('div');
// 	switcherElement.classList.add('switcher');

// 	var intervalElements = items.map(function(item) {
// 		var itemEl = document.createElement('button');
// 		itemEl.innerText = item;
// 		itemEl.classList.add('switcher-item');
// 		itemEl.classList.toggle('switcher-active-item', item === activeItem);
// 		itemEl.addEventListener('click', function() {
// 			onItemClicked(item);
// 		});
// 		switcherElement.appendChild(itemEl);
// 		return itemEl;
// 	});

// 	function onItemClicked(item) {
// 		if (item === activeItem) {
// 			return;
// 		}

// 		intervalElements.forEach(function(element, index) {
// 			element.classList.toggle('switcher-active-item', items[index] === item);
// 		});

// 		activeItem = item;

// 		activeItemChangedCallback(item);
// 	}

// 	return switcherElement;
// }

// var intervals = ['1D', '1W', '1M', '1Y'];

// var dayData = [
// 	{ time: '2018-10-19', value: 26.19 },
// 	{ time: '2018-10-22', value: 25.87 },
// 	{ time: '2018-10-23', value: 25.83 },
// 	{ time: '2018-10-24', value: 25.78 },
// 	{ time: '2018-10-25', value: 25.82 },
// 	{ time: '2018-10-26', value: 25.81 },
// 	{ time: '2018-10-29', value: 25.82 },
// 	{ time: '2018-10-30', value: 25.71 },
// 	{ time: '2018-10-31', value: 25.82 },
// 	{ time: '2018-11-01', value: 25.72 },
// 	{ time: '2018-11-02', value: 25.74 },
// 	{ time: '2018-11-05', value: 25.81 },
// 	{ time: '2018-11-06', value: 25.75 },
// 	{ time: '2018-11-07', value: 25.73 },
// 	{ time: '2018-11-08', value: 25.75 },
// 	{ time: '2018-11-09', value: 25.75 },
// 	{ time: '2018-11-12', value: 25.76 },
// 	{ time: '2018-11-13', value: 25.80 },
// 	{ time: '2018-11-14', value: 25.77 },
// 	{ time: '2018-11-15', value: 25.75 },
// 	{ time: '2018-11-16', value: 25.75 },
// 	{ time: '2018-11-19', value: 25.75 },
// 	{ time: '2018-11-20', value: 25.72 },
// 	{ time: '2018-11-21', value: 25.78 },
// 	{ time: '2018-11-23', value: 25.72 },
// 	{ time: '2018-11-26', value: 25.78 },
// 	{ time: '2018-11-27', value: 25.85 },
// 	{ time: '2018-11-28', value: 25.85 },
// 	{ time: '2018-11-29', value: 25.55 },
// 	{ time: '2018-11-30', value: 25.41 },
// 	{ time: '2018-12-03', value: 25.41 },
// 	{ time: '2018-12-04', value: 25.42 },
// 	{ time: '2018-12-06', value: 25.33 },
// 	{ time: '2018-12-07', value: 25.39 },
// 	{ time: '2018-12-10', value: 25.32 },
// 	{ time: '2018-12-11', value: 25.48 },
// 	{ time: '2018-12-12', value: 25.39 },
// 	{ time: '2018-12-13', value: 25.45 },
// 	{ time: '2018-12-14', value: 25.52 },
// 	{ time: '2018-12-17', value: 25.38 },
// 	{ time: '2018-12-18', value: 25.36 },
// 	{ time: '2018-12-19', value: 25.65 },
// 	{ time: '2018-12-20', value: 25.70 },
// 	{ time: '2018-12-21', value: 25.66 },
// 	{ time: '2018-12-24', value: 25.66 },
// 	{ time: '2018-12-26', value: 25.65 },
// 	{ time: '2018-12-27', value: 25.66 },
// 	{ time: '2018-12-28', value: 25.68 },
// 	{ time: '2018-12-31', value: 25.77 },
// 	{ time: '2019-01-02', value: 25.72 },
// 	{ time: '2019-01-03', value: 25.69 },
// 	{ time: '2019-01-04', value: 25.71 },
// 	{ time: '2019-01-07', value: 25.72 },
// 	{ time: '2019-01-08', value: 25.72 },
// 	{ time: '2019-01-09', value: 25.66 },
// 	{ time: '2019-01-10', value: 25.85 },
// 	{ time: '2019-01-11', value: 25.92 },
// 	{ time: '2019-01-14', value: 25.94 },
// 	{ time: '2019-01-15', value: 25.95 },
// 	{ time: '2019-01-16', value: 26.00 },
// 	{ time: '2019-01-17', value: 25.99 },
// 	{ time: '2019-01-18', value: 25.60 },
// 	{ time: '2019-01-22', value: 25.81 },
// 	{ time: '2019-01-23', value: 25.70 },
// 	{ time: '2019-01-24', value: 25.74 },
// 	{ time: '2019-01-25', value: 25.80 },
// 	{ time: '2019-01-28', value: 25.83 },
// 	{ time: '2019-01-29', value: 25.70 },
// 	{ time: '2019-01-30', value: 25.78 },
// 	{ time: '2019-01-31', value: 25.35 },
// 	{ time: '2019-02-01', value: 25.60 },
// 	{ time: '2019-02-04', value: 25.65 },
// 	{ time: '2019-02-05', value: 25.73 },
// 	{ time: '2019-02-06', value: 25.71 },
// 	{ time: '2019-02-07', value: 25.71 },
// 	{ time: '2019-02-08', value: 25.72 },
// 	{ time: '2019-02-11', value: 25.76 },
// 	{ time: '2019-02-12', value: 25.84 },
// 	{ time: '2019-02-13', value: 25.85 },
// 	{ time: '2019-02-14', value: 25.87 },
// 	{ time: '2019-02-15', value: 25.89 },
// 	{ time: '2019-02-19', value: 25.90 },
// 	{ time: '2019-02-20', value: 25.92 },
// 	{ time: '2019-02-21', value: 25.96 },
// 	{ time: '2019-02-22', value: 26.00 },
// 	{ time: '2019-02-25', value: 25.93 },
// 	{ time: '2019-02-26', value: 25.92 },
// 	{ time: '2019-02-27', value: 25.67 },
// 	{ time: '2019-02-28', value: 25.79 },
// 	{ time: '2019-03-01', value: 25.86 },
// 	{ time: '2019-03-04', value: 25.94 },
// 	{ time: '2019-03-05', value: 26.02 },
// 	{ time: '2019-03-06', value: 25.95 },
// 	{ time: '2019-03-07', value: 25.89 },
// 	{ time: '2019-03-08', value: 25.94 },
// 	{ time: '2019-03-11', value: 25.91 },
// 	{ time: '2019-03-12', value: 25.92 },
// 	{ time: '2019-03-13', value: 26.00 },
// 	{ time: '2019-03-14', value: 26.05 },
// 	{ time: '2019-03-15', value: 26.11 },
// 	{ time: '2019-03-18', value: 26.10 },
// 	{ time: '2019-03-19', value: 25.98 },
// 	{ time: '2019-03-20', value: 26.11 },
// 	{ time: '2019-03-21', value: 26.12 },
// 	{ time: '2019-03-22', value: 25.88 },
// 	{ time: '2019-03-25', value: 25.85 },
// 	{ time: '2019-03-26', value: 25.72 },
// 	{ time: '2019-03-27', value: 25.73 },
// 	{ time: '2019-03-28', value: 25.80 },
// 	{ time: '2019-03-29', value: 25.77 },
// 	{ time: '2019-04-01', value: 26.06 },
// 	{ time: '2019-04-02', value: 25.93 },
// 	{ time: '2019-04-03', value: 25.95 },
// 	{ time: '2019-04-04', value: 26.06 },
// 	{ time: '2019-04-05', value: 26.16 },
// 	{ time: '2019-04-08', value: 26.12 },
// 	{ time: '2019-04-09', value: 26.07 },
// 	{ time: '2019-04-10', value: 26.13 },
// 	{ time: '2019-04-11', value: 26.04 },
// 	{ time: '2019-04-12', value: 26.04 },
// 	{ time: '2019-04-15', value: 26.05 },
// 	{ time: '2019-04-16', value: 26.01 },
// 	{ time: '2019-04-17', value: 26.09 },
// 	{ time: '2019-04-18', value: 26.00 },
// 	{ time: '2019-04-22', value: 26.00 },
// 	{ time: '2019-04-23', value: 26.06 },
// 	{ time: '2019-04-24', value: 26.00 },
// 	{ time: '2019-04-25', value: 25.81 },
// 	{ time: '2019-04-26', value: 25.88 },
// 	{ time: '2019-04-29', value: 25.91 },
// 	{ time: '2019-04-30', value: 25.90 },
// 	{ time: '2019-05-01', value: 26.02 },
// 	{ time: '2019-05-02', value: 25.97 },
// 	{ time: '2019-05-03', value: 26.02 },
// 	{ time: '2019-05-06', value: 26.03 },
// 	{ time: '2019-05-07', value: 26.04 },
// 	{ time: '2019-05-08', value: 26.05 },
// 	{ time: '2019-05-09', value: 26.05 },
// 	{ time: '2019-05-10', value: 26.08 },
// 	{ time: '2019-05-13', value: 26.05 },
// 	{ time: '2019-05-14', value: 26.01 },
// 	{ time: '2019-05-15', value: 26.03 },
// 	{ time: '2019-05-16', value: 26.14 },
// 	{ time: '2019-05-17', value: 26.09 },
// 	{ time: '2019-05-20', value: 26.01 },
// 	{ time: '2019-05-21', value: 26.12 },
// 	{ time: '2019-05-22', value: 26.15 },
// 	{ time: '2019-05-23', value: 26.18 },
// 	{ time: '2019-05-24', value: 26.16 },
// 	{ time: '2019-05-28', value: 26.23 },
// ];

// var weekData = [
// 	{ time: '2016-07-18', value: 26.10 },
// 	{ time: '2016-07-25', value: 26.19 },
// 	{ time: '2016-08-01', value: 26.24 },
// 	{ time: '2016-08-08', value: 26.22 },
// 	{ time: '2016-08-15', value: 25.98 },
// 	{ time: '2016-08-22', value: 25.85 },
// 	{ time: '2016-08-29', value: 25.98 },
// 	{ time: '2016-09-05', value: 25.71 },
// 	{ time: '2016-09-12', value: 25.84 },
// 	{ time: '2016-09-19', value: 25.89 },
// 	{ time: '2016-09-26', value: 25.65 },
// 	{ time: '2016-10-03', value: 25.69 },
// 	{ time: '2016-10-10', value: 25.67 },
// 	{ time: '2016-10-17', value: 26.11 },
// 	{ time: '2016-10-24', value: 25.80 },
// 	{ time: '2016-10-31', value: 25.70 },
// 	{ time: '2016-11-07', value: 25.40 },
// 	{ time: '2016-11-14', value: 25.32 },
// 	{ time: '2016-11-21', value: 25.48 },
// 	{ time: '2016-11-28', value: 25.08 },
// 	{ time: '2016-12-05', value: 25.06 },
// 	{ time: '2016-12-12', value: 25.11 },
// 	{ time: '2016-12-19', value: 25.34 },
// 	{ time: '2016-12-26', value: 25.20 },
// 	{ time: '2017-01-02', value: 25.33 },
// 	{ time: '2017-01-09', value: 25.56 },
// 	{ time: '2017-01-16', value: 25.32 },
// 	{ time: '2017-01-23', value: 25.71 },
// 	{ time: '2017-01-30', value: 25.85 },
// 	{ time: '2017-02-06', value: 25.77 },
// 	{ time: '2017-02-13', value: 25.94 },
// 	{ time: '2017-02-20', value: 25.67 },
// 	{ time: '2017-02-27', value: 25.60 },
// 	{ time: '2017-03-06', value: 25.54 },
// 	{ time: '2017-03-13', value: 25.84 },
// 	{ time: '2017-03-20', value: 25.96 },
// 	{ time: '2017-03-27', value: 25.90 },
// 	{ time: '2017-04-03', value: 25.97 },
// 	{ time: '2017-04-10', value: 26.00 },
// 	{ time: '2017-04-17', value: 26.13 },
// 	{ time: '2017-04-24', value: 26.02 },
// 	{ time: '2017-05-01', value: 26.30 },
// 	{ time: '2017-05-08', value: 26.27 },
// 	{ time: '2017-05-15', value: 26.24 },
// 	{ time: '2017-05-22', value: 26.02 },
// 	{ time: '2017-05-29', value: 26.20 },
// 	{ time: '2017-06-05', value: 26.12 },
// 	{ time: '2017-06-12', value: 26.20 },
// 	{ time: '2017-06-19', value: 26.46 },
// 	{ time: '2017-06-26', value: 26.39 },
// 	{ time: '2017-07-03', value: 26.52 },
// 	{ time: '2017-07-10', value: 26.57 },
// 	{ time: '2017-07-17', value: 26.65 },
// 	{ time: '2017-07-24', value: 26.45 },
// 	{ time: '2017-07-31', value: 26.37 },
// 	{ time: '2017-08-07', value: 26.13 },
// 	{ time: '2017-08-14', value: 26.21 },
// 	{ time: '2017-08-21', value: 26.31 },
// 	{ time: '2017-08-28', value: 26.33 },
// 	{ time: '2017-09-04', value: 26.38 },
// 	{ time: '2017-09-11', value: 26.38 },
// 	{ time: '2017-09-18', value: 26.50 },
// 	{ time: '2017-09-25', value: 26.39 },
// 	{ time: '2017-10-02', value: 25.95 },
// 	{ time: '2017-10-09', value: 26.15 },
// 	{ time: '2017-10-16', value: 26.43 },
// 	{ time: '2017-10-23', value: 26.22 },
// 	{ time: '2017-10-30', value: 26.14 },
// 	{ time: '2017-11-06', value: 26.08 },
// 	{ time: '2017-11-13', value: 26.27 },
// 	{ time: '2017-11-20', value: 26.30 },
// 	{ time: '2017-11-27', value: 25.92 },
// 	{ time: '2017-12-04', value: 26.10 },
// 	{ time: '2017-12-11', value: 25.88 },
// 	{ time: '2017-12-18', value: 25.82 },
// 	{ time: '2017-12-25', value: 25.82 },
// 	{ time: '2018-01-01', value: 25.81 },
// 	{ time: '2018-01-08', value: 25.95 },
// 	{ time: '2018-01-15', value: 26.03 },
// 	{ time: '2018-01-22', value: 26.04 },
// 	{ time: '2018-01-29', value: 25.96 },
// 	{ time: '2018-02-05', value: 25.99 },
// 	{ time: '2018-02-12', value: 26.00 },
// 	{ time: '2018-02-19', value: 26.06 },
// 	{ time: '2018-02-26', value: 25.77 },
// 	{ time: '2018-03-05', value: 25.81 },
// 	{ time: '2018-03-12', value: 25.88 },
// 	{ time: '2018-03-19', value: 25.72 },
// 	{ time: '2018-03-26', value: 25.75 },
// 	{ time: '2018-04-02', value: 25.70 },
// 	{ time: '2018-04-09', value: 25.73 },
// 	{ time: '2018-04-16', value: 25.74 },
// 	{ time: '2018-04-23', value: 25.69 },
// 	{ time: '2018-04-30', value: 25.76 },
// 	{ time: '2018-05-07', value: 25.89 },
// 	{ time: '2018-05-14', value: 25.89 },
// 	{ time: '2018-05-21', value: 26.00 },
// 	{ time: '2018-05-28', value: 25.79 },
// 	{ time: '2018-06-04', value: 26.11 },
// 	{ time: '2018-06-11', value: 26.43 },
// 	{ time: '2018-06-18', value: 26.30 },
// 	{ time: '2018-06-25', value: 26.58 },
// 	{ time: '2018-07-02', value: 26.33 },
// 	{ time: '2018-07-09', value: 26.33 },
// 	{ time: '2018-07-16', value: 26.32 },
// 	{ time: '2018-07-23', value: 26.20 },
// 	{ time: '2018-07-30', value: 26.03 },
// 	{ time: '2018-08-06', value: 26.15 },
// 	{ time: '2018-08-13', value: 26.17 },
// 	{ time: '2018-08-20', value: 26.28 },
// 	{ time: '2018-08-27', value: 25.86 },
// 	{ time: '2018-09-03', value: 25.69 },
// 	{ time: '2018-09-10', value: 25.69 },
// 	{ time: '2018-09-17', value: 25.64 },
// 	{ time: '2018-09-24', value: 25.67 },
// 	{ time: '2018-10-01', value: 25.55 },
// 	{ time: '2018-10-08', value: 25.59 },
// 	{ time: '2018-10-15', value: 26.19 },
// 	{ time: '2018-10-22', value: 25.81 },
// 	{ time: '2018-10-29', value: 25.74 },
// 	{ time: '2018-11-05', value: 25.75 },
// 	{ time: '2018-11-12', value: 25.75 },
// 	{ time: '2018-11-19', value: 25.72 },
// 	{ time: '2018-11-26', value: 25.41 },
// 	{ time: '2018-12-03', value: 25.39 },
// 	{ time: '2018-12-10', value: 25.52 },
// 	{ time: '2018-12-17', value: 25.66 },
// 	{ time: '2018-12-24', value: 25.68 },
// 	{ time: '2018-12-31', value: 25.71 },
// 	{ time: '2019-01-07', value: 25.92 },
// 	{ time: '2019-01-14', value: 25.60 },
// 	{ time: '2019-01-21', value: 25.80 },
// 	{ time: '2019-01-28', value: 25.60 },
// 	{ time: '2019-02-04', value: 25.72 },
// 	{ time: '2019-02-11', value: 25.89 },
// 	{ time: '2019-02-18', value: 26.00 },
// 	{ time: '2019-02-25', value: 25.86 },
// 	{ time: '2019-03-04', value: 25.94 },
// 	{ time: '2019-03-11', value: 26.11 },
// 	{ time: '2019-03-18', value: 25.88 },
// 	{ time: '2019-03-25', value: 25.77 },
// 	{ time: '2019-04-01', value: 26.16 },
// 	{ time: '2019-04-08', value: 26.04 },
// 	{ time: '2019-04-15', value: 26.00 },
// 	{ time: '2019-04-22', value: 25.88 },
// 	{ time: '2019-04-29', value: 26.02 },
// 	{ time: '2019-05-06', value: 26.08 },
// 	{ time: '2019-05-13', value: 26.09 },
// 	{ time: '2019-05-20', value: 26.16 },
// 	{ time: '2019-05-27', value: 26.23 },
// ];

// var monthData = [
// 	{ time: '2006-12-01', value: 25.40 },
// 	{ time: '2007-01-01', value: 25.50 },
// 	{ time: '2007-02-01', value: 25.11 },
// 	{ time: '2007-03-01', value: 25.24 },
// 	{ time: '2007-04-02', value: 25.34 },
// 	{ time: '2007-05-01', value: 24.82 },
// 	{ time: '2007-06-01', value: 23.85 },
// 	{ time: '2007-07-02', value: 23.24 },
// 	{ time: '2007-08-01', value: 23.05 },
// 	{ time: '2007-09-03', value: 22.26 },
// 	{ time: '2007-10-01', value: 22.52 },
// 	{ time: '2007-11-01', value: 20.84 },
// 	{ time: '2007-12-03', value: 20.37 },
// 	{ time: '2008-01-01', value: 23.90 },
// 	{ time: '2008-02-01', value: 22.58 },
// 	{ time: '2008-03-03', value: 21.74 },
// 	{ time: '2008-04-01', value: 22.50 },
// 	{ time: '2008-05-01', value: 22.38 },
// 	{ time: '2008-06-02', value: 20.58 },
// 	{ time: '2008-07-01', value: 20.60 },
// 	{ time: '2008-08-01', value: 20.82 },
// 	{ time: '2008-09-01', value: 17.50 },
// 	{ time: '2008-10-01', value: 17.70 },
// 	{ time: '2008-11-03', value: 15.52 },
// 	{ time: '2008-12-01', value: 18.58 },
// 	{ time: '2009-01-01', value: 15.40 },
// 	{ time: '2009-02-02', value: 11.68 },
// 	{ time: '2009-03-02', value: 14.89 },
// 	{ time: '2009-04-01', value: 16.24 },
// 	{ time: '2009-05-01', value: 18.33 },
// 	{ time: '2009-06-01', value: 18.08 },
// 	{ time: '2009-07-01', value: 20.07 },
// 	{ time: '2009-08-03', value: 20.35 },
// 	{ time: '2009-09-01', value: 21.53 },
// 	{ time: '2009-10-01', value: 21.48 },
// 	{ time: '2009-11-02', value: 20.28 },
// 	{ time: '2009-12-01', value: 21.39 },
// 	{ time: '2010-01-01', value: 22.00 },
// 	{ time: '2010-02-01', value: 22.31 },
// 	{ time: '2010-03-01', value: 22.82 },
// 	{ time: '2010-04-01', value: 22.58 },
// 	{ time: '2010-05-03', value: 21.02 },
// 	{ time: '2010-06-01', value: 21.45 },
// 	{ time: '2010-07-01', value: 22.42 },
// 	{ time: '2010-08-02', value: 23.61 },
// 	{ time: '2010-09-01', value: 24.40 },
// 	{ time: '2010-10-01', value: 24.46 },
// 	{ time: '2010-11-01', value: 23.64 },
// 	{ time: '2010-12-01', value: 22.90 },
// 	{ time: '2011-01-03', value: 23.73 },
// 	{ time: '2011-02-01', value: 23.52 },
// 	{ time: '2011-03-01', value: 24.15 },
// 	{ time: '2011-04-01', value: 24.37 },
// 	{ time: '2011-05-02', value: 24.40 },
// 	{ time: '2011-06-01', value: 24.45 },
// 	{ time: '2011-07-01', value: 24.24 },
// 	{ time: '2011-08-01', value: 24.00 },
// 	{ time: '2011-09-01', value: 22.77 },
// 	{ time: '2011-10-03', value: 24.21 },
// 	{ time: '2011-11-01', value: 23.40 },
// 	{ time: '2011-12-01', value: 23.90 },
// 	{ time: '2012-01-02', value: 24.84 },
// 	{ time: '2012-02-01', value: 25.04 },
// 	{ time: '2012-03-01', value: 24.90 },
// 	{ time: '2012-04-02', value: 25.06 },
// 	{ time: '2012-05-01', value: 24.63 },
// 	{ time: '2012-06-01', value: 25.07 },
// 	{ time: '2012-07-02', value: 25.30 },
// 	{ time: '2012-08-01', value: 25.08 },
// 	{ time: '2012-09-03', value: 25.27 },
// 	{ time: '2012-10-01', value: 25.39 },
// 	{ time: '2012-11-01', value: 25.06 },
// 	{ time: '2012-12-03', value: 25.03 },
// 	{ time: '2013-01-01', value: 25.26 },
// 	{ time: '2013-02-01', value: 25.20 },
// 	{ time: '2013-03-01', value: 25.30 },
// 	{ time: '2013-04-01', value: 25.38 },
// 	{ time: '2013-05-01', value: 25.22 },
// 	{ time: '2013-06-03', value: 24.88 },
// 	{ time: '2013-07-01', value: 24.98 },
// 	{ time: '2013-08-01', value: 24.60 },
// 	{ time: '2013-09-02', value: 24.65 },
// 	{ time: '2013-10-01', value: 24.62 },
// 	{ time: '2013-11-01', value: 24.65 },
// 	{ time: '2013-12-02', value: 24.70 },
// 	{ time: '2014-01-01', value: 24.98 },
// 	{ time: '2014-02-03', value: 24.95 },
// 	{ time: '2014-03-03', value: 25.45 },
// 	{ time: '2014-04-01', value: 25.40 },
// 	{ time: '2014-05-01', value: 25.51 },
// 	{ time: '2014-06-02', value: 25.34 },
// 	{ time: '2014-07-01', value: 25.30 },
// 	{ time: '2014-08-01', value: 25.36 },
// 	{ time: '2014-09-01', value: 25.16 },
// 	{ time: '2014-10-01', value: 25.53 },
// 	{ time: '2014-11-03', value: 25.40 },
// 	{ time: '2014-12-01', value: 25.70 },
// 	{ time: '2015-01-01', value: 25.95 },
// 	{ time: '2015-02-02', value: 25.81 },
// 	{ time: '2015-03-02', value: 25.63 },
// 	{ time: '2015-04-01', value: 25.39 },
// 	{ time: '2015-05-01', value: 25.62 },
// 	{ time: '2015-06-01', value: 25.23 },
// 	{ time: '2015-07-01', value: 25.47 },
// 	{ time: '2015-08-03', value: 25.18 },
// 	{ time: '2015-09-01', value: 25.30 },
// 	{ time: '2015-10-01', value: 25.68 },
// 	{ time: '2015-11-02', value: 25.63 },
// 	{ time: '2015-12-01', value: 25.57 },
// 	{ time: '2016-01-01', value: 25.55 },
// 	{ time: '2016-02-01', value: 25.05 },
// 	{ time: '2016-03-01', value: 25.61 },
// 	{ time: '2016-04-01', value: 25.91 },
// 	{ time: '2016-05-02', value: 25.84 },
// 	{ time: '2016-06-01', value: 25.94 },
// 	{ time: '2016-07-01', value: 26.19 },
// 	{ time: '2016-08-01', value: 26.06 },
// 	{ time: '2016-09-01', value: 25.65 },
// 	{ time: '2016-10-03', value: 25.80 },
// 	{ time: '2016-11-01', value: 25.06 },
// 	{ time: '2016-12-01', value: 25.20 },
// 	{ time: '2017-01-02', value: 25.70 },
// 	{ time: '2017-02-01', value: 25.78 },
// 	{ time: '2017-03-01', value: 25.90 },
// 	{ time: '2017-04-03', value: 26.02 },
// 	{ time: '2017-05-01', value: 26.02 },
// 	{ time: '2017-06-01', value: 26.39 },
// 	{ time: '2017-07-03', value: 26.30 },
// 	{ time: '2017-08-01', value: 26.14 },
// 	{ time: '2017-09-01', value: 26.39 },
// 	{ time: '2017-10-02', value: 26.12 },
// 	{ time: '2017-11-01', value: 25.81 },
// 	{ time: '2017-12-01', value: 25.82 },
// 	{ time: '2018-01-01', value: 26.06 },
// 	{ time: '2018-02-01', value: 25.78 },
// 	{ time: '2018-03-01', value: 25.75 },
// 	{ time: '2018-04-02', value: 25.72 },
// 	{ time: '2018-05-01', value: 25.75 },
// 	{ time: '2018-06-01', value: 26.58 },
// 	{ time: '2018-07-02', value: 26.14 },
// 	{ time: '2018-08-01', value: 25.86 },
// 	{ time: '2018-09-03', value: 25.67 },
// 	{ time: '2018-10-01', value: 25.82 },
// 	{ time: '2018-11-01', value: 25.41 },
// 	{ time: '2018-12-03', value: 25.77 },
// 	{ time: '2019-01-01', value: 25.35 },
// 	{ time: '2019-02-01', value: 25.79 },
// 	{ time: '2019-03-01', value: 25.77 },
// 	{ time: '2019-04-01', value: 25.90 },
// 	{ time: '2019-05-01', value: 26.23 },
// ];

// var yearData = [
// 	{ time: '2006-01-02', value: 24.89 },
// 	{ time: '2007-01-01', value: 25.50 },
// 	{ time: '2008-01-01', value: 23.90 },
// 	{ time: '2009-01-01', value: 15.40 },
// 	{ time: '2010-01-01', value: 22.00 },
// 	{ time: '2011-01-03', value: 23.73 },
// 	{ time: '2012-01-02', value: 24.84 },
// 	{ time: '2013-01-01', value: 25.26 },
// 	{ time: '2014-01-01', value: 24.98 },
// 	{ time: '2015-01-01', value: 25.95 },
// 	{ time: '2016-01-01', value: 25.55 },
// 	{ time: '2017-01-02', value: 25.70 },
// 	{ time: '2018-01-01', value: 26.06 },
// 	{ time: '2019-01-01', value: 26.23 },
// ];

// var seriesesData = new Map([
//   ['1D', dayData ],
//   ['1W', weekData ],
//   ['1M', monthData ],
//   ['1Y', yearData ],
// ]);

// var switcherElement = createSimpleSwitcher(intervals, intervals[0], syncToInterval);

// var chartElement = document.createElement('div');

// var chart = LightweightCharts.createChart(chartElement, {
// 	width: 600,
//   height: 300,
// 	layout: {
// 		backgroundColor: '#000000',
// 		textColor: '#d1d4dc',
// 	},
// 	grid: {
// 		vertLines: {
// 			visible: false,
// 		},
// 		horzLines: {
// 			color: 'rgba(42, 46, 57, 0.5)',
// 		},
// 	},
// 	rightPriceScale: {
// 		borderVisible: false,
// 	},
// 	timeScale: {
// 		borderVisible: false,
// 	},
// 	crosshair: {
// 		horzLine: {
// 			visible: false,
// 		},
// 	},
// });

// document.body.appendChild(chartElement);
// document.body.appendChild(switcherElement);

// var areaSeries = null;

// function syncToInterval(interval) {
// 	if (areaSeries) {
// 		chart.removeSeries(areaSeries);
// 		areaSeries = null;
// 	}
// 	areaSeries = chart.addAreaSeries({
//     topColor: 'rgba(76, 175, 80, 0.56)',
//     bottomColor: 'rgba(76, 175, 80, 0.04)',
//     lineColor: 'rgba(76, 175, 80, 1)',
//     lineWidth: 2,
// 	});
// 	areaSeries.setData(seriesesData.get(interval));
// }

// syncToInterval(intervals[0]);



// var period = {
//     timeFrom: { day: 1, month: 1, year: 2018 },
//     timeTo: { day: 1, month: 1, year: 2019 },
// };
// var data = generateBarsData(period);
// candleSeries.setData(data);



// var timer = null;
// timeScale.subscribeVisibleLogicalRangeChange(() => {
//     if (timer !== null) {
//         return;
//     }
//     timer = setTimeout(() => {
//         var logicalRange = timeScale.getVisibleLogicalRange();
//         if (logicalRange !== null) {
//             var barsInfo = candleSeries.barsInLogicalRange(logicalRange);
//             if (barsInfo !== null && barsInfo.barsBefore < 10) {
//                 var firstTime = getBusinessDayBeforeCurrentAt(data[0].time, 1);
//                 var lastTime = getBusinessDayBeforeCurrentAt(firstTime, Math.max(100, -barsInfo.barsBefore + 100));
//                 var newPeriod = {
//                     timeFrom: lastTime,
//                     timeTo: firstTime,
//                 };
//                 data = [...generateBarsData(newPeriod), ...data];
//                 candleSeries.setData(data);
//             }
//         }
//         timer = null;
//     }, 50);
// });

// function getBusinessDayBeforeCurrentAt(date, daysDelta) {
//     const dateWithDelta = new Date(Date.UTC(date.year, date.month - 1, date.day - daysDelta, 0, 0, 0, 0));
//     return { year: dateWithDelta.getFullYear(), month: dateWithDelta.getMonth() + 1, day: dateWithDelta.getDate() };
// }

// function generateBarsData(period) {
//     var res = [];
//     var controlPoints = generateControlPoints(res, period);
//     for (var i = 0; i < controlPoints.length - 1; i++) {
//         var left = controlPoints[i];
//         var right = controlPoints[i + 1];
//         fillBarsSegment(left, right, res);
//     }
//     return res;
// }

// function fillBarsSegment(left, right, points) {
//     var deltaY = right.price - left.price;
//     var deltaX = right.index - left.index;
//     var angle = deltaY / deltaX;
//     for (var i = left.index; i <= right.index; i++) {
//         var basePrice = left.price + (i - left.index) * angle;
//         var openNoise = (0.1 - Math.random() * 0.2) + 1;
//         var closeNoise = (0.1 - Math.random() * 0.2) + 1;
//         var open = basePrice * openNoise;
//         var close = basePrice * closeNoise;
//         var high = Math.max(basePrice * (1 + Math.random() * 0.2), open, close);
//         var low = Math.min(basePrice * (1 - Math.random() * 0.2), open, close);
//         points[i].open = open;
//         points[i].high = high;
//         points[i].low = low;
//         points[i].close = close;
//     }
// }

// function generateControlPoints(res, period, dataMultiplier) {
//     var time = period !== undefined ? period.timeFrom : { day: 1, month: 1, year: 2018 };
//     var timeTo = period !== undefined ? period.timeTo : { day: 1, month: 1, year: 2019 };
//     var days = getDiffDays(time, timeTo);
//     dataMultiplier = dataMultiplier || 1;
//     var controlPoints = [];
//     controlPoints.push({ index: 0, price: getRandomPrice() * dataMultiplier });
//     for (var i = 0; i < days; i++) {
//         if (i > 0 && i < days - 1 && Math.random() < 0.05) {
//             controlPoints.push({ index: i, price: getRandomPrice() * dataMultiplier });
//         }
//         res.push({ time: time });
//         time = nextBusinessDay(time);
//     }
//     controlPoints.push({ index: res.length - 1, price: getRandomPrice() * dataMultiplier });
//     return controlPoints;
// }

// function getDiffDays(dateFrom, dateTo) {
//     var df = convertBusinessDayToUTCTimestamp(dateFrom);
//     var dt = convertBusinessDayToUTCTimestamp(dateTo);
//     var diffTime = Math.abs(dt.getTime() - df.getTime());
//     return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
// }

// function convertBusinessDayToUTCTimestamp(date) {
//     return new Date(Date.UTC(date.year, date.month - 1, date.day, 0, 0, 0, 0));
// }

// function nextBusinessDay(time) {
//     var d = convertBusinessDayToUTCTimestamp({ year: time.year, month: time.month, day: time.day + 1 });
//     return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate() };
// }

// function getRandomPrice() {
//     return 10 + Math.round(Math.random() * 10000) / 100;
// }