var current_exchange = document.getElementById("exchange").textContent
var current_base_id = document.getElementById("base_id").textContent
var current_quote_id = document.getElementById("quote_id").textContent
var current_end = Date.now()
var current_start = current_end - 60000 * 60 * 24

// Chart time periods
var periods = ['1h','6h','1D','3D','7D','1M','3M','1Y','3Y']

// Mapping of time period to data interval
var periodIntervalMap = new Map([
    ['1h', '1m'],
    ['6h', '5m'],
    ['1D', '15m'],
    ['3D', '30m'],
    ['7D', '1h'],
    ['1M', '6h'],
    ['3M', '12h'],
    ['1Y', '1D'],
    ['3Y', '7D']
])

// Mapping of time period to number of intervals in each period
//  e.g.: 1h has 60 1-minute intervals; 6h has 72 5-minute intervals
var periodNumIntervalMap = new Map([
    ['1h', 60],
    ['6h', 72],
    ['1D', 96],
    ['3D', 144],
    ['7D', 168],
    ['1M', 120],
    ['3M', 180],
    ['1Y', 365],
    ['3Y', 157]
])

// Mapping of interval to number of milliseconds per interval
//  Used to get historical data, hence the minus
var intervalPreviousMillisecondsMap = new Map([
    ['1m', -60000],
    ['5m', -60000 * 5],
    ['15m', -60000 * 15],
    ['30m', -60000 * 30],
    ['1h', -60000 * 60],
    ['6h', -60000 * 60 * 6],
    ['12h', -60000 * 60 * 12],
    ['1D', -60000 * 60 * 24],
    ['7D', -60000 * 60 * 24 * 7]
])

// Mapping of time period to the corresponding data array
var dataPeriodMap = new Map([
    ['1h', null],
    ['6h', null],
    ['1D', null],
    ['3D', null],
    ['7D', null],
    ['1M', null],
    ['3M', null],
    ['1Y', null],
    ['3Y', null]
])

// Mapping of time period to the corresponding latest data bar
var latestBarPeriodMap = new Map([
    ['1h', null],
    ['6h', null],
    ['1D', null],
    ['3D', null],
    ['7D', null],
    ['1M', null],
    ['3M', null],
    ['1Y', null],
    ['3Y', null]
])

var current_period = '1h'
var current_interval = periodIntervalMap.get(current_period)
var current_data = null
var current_historical = null
var latest_bar = null
var ws_hold = false
var drawing_processes = 0

var candles_ws = null
var chartWidth = 1250
var chartHeight = 450
var chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: chartWidth,
    height: chartHeight,
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
var candleSeries = chart.addCandlestickSeries()
var timeScale = chart.timeScale()
var updateChartTimer = null


function dateToTimestamp(date) {
    return new Date(Date.UTC(date.year, date.month - 1, date.day, 0, 0, 0, 0))
};

// Returns timestamp `millisecondsPerStep * steps` away from `milliseconds`
function getTimestampSteps(milliseconds, millisecondsPerStep, steps) {
    const ret = milliseconds + millisecondsPerStep * steps
    return ret
};

// Time range switcher
function createSimpleSwitcher(items, activeItem, activeItemChangedCallback) {
	var switcherElement = document.createElement('div')
	switcherElement.classList.add('switcher')

	var intervalElements = items.map(function(item) {
		var itemEl = document.createElement('button')
		itemEl.innerText = item
		itemEl.classList.add('switcher-item');
		itemEl.classList.toggle('switcher-active-item', item === activeItem);
		itemEl.addEventListener('click', function() {
			onItemClicked(item)
		});
		switcherElement.appendChild(itemEl)
		return itemEl
	})
	function onItemClicked(item) {
		if (item === activeItem) {
			return;
		}

		intervalElements.forEach(function(element, index) {
			element.classList.toggle('switcher-active-item', items[index] === item)
		})

		activeItem = item
		activeItemChangedCallback(item)
	}
	return switcherElement
};

// Click period callback
function syncToInterval(period) {    
    // current_data = null
    // current_period = period
    let interval = periodIntervalMap.get(period)
    let existing_data = dataPeriodMap.get(period)
    let end = null
    let start = null
    let historical = null

    ws_hold = true

    console.log(period)
    console.log(interval)
    
    if (candleSeries) {
        if (candles_ws !== null) {
            let unsubscribe_msg = JSON.stringify({
                event_type: "unsubscribe",
                data_type: "ohlcv",
                exchange: current_exchange,
                base_id: current_base_id,
                quote_id: current_quote_id,
                interval: current_interval
            })
            candles_ws.send(unsubscribe_msg)
        }
		chart.removeSeries(candleSeries)
		candleSeries = null
        candleSeries = chart.addCandlestickSeries()
	}
    
    if (existing_data === null) {
        end = Date.now()
        start = getTimestampSteps(
            end,
            intervalPreviousMillisecondsMap.get(interval),
            periodNumIntervalMap.get(period))
        historical = true
    }
    else {
        console.log("reusing existing data")
        let last_datum = existing_data[existing_data.length - 1]
        let last_timestamp = last_datum.time * 1000

        end = Date.now()
        start = getTimestampSteps(
            last_timestamp,
            (-intervalPreviousMillisecondsMap.get(interval)),
            1)
        historical = false

        // TODO: probably not need this
        // Set data using existing first
        // candleSeries.setData(existing_data)
    }

    // Update OHLC and draw/re-draw
    readRestDrawOHLC(
        current_exchange, current_base_id,
        current_quote_id, start,
        end, interval,
        historical, period, true
    )

    if (candles_ws !== null) {
        let subscribe_msg = JSON.stringify({
            event_type: "subscribe",
            data_type: "ohlcv",
            exchange: current_exchange,
            base_id: current_base_id,
            quote_id: current_quote_id,
            interval: interval,
            mls: false
        })
        candles_ws.send(subscribe_msg)
    }
    else {
        readWSUpdateOHLC(
            current_exchange, current_base_id,
            current_quote_id, interval, period
        )
    }
};

function saveDrawCandleSeries(candle_data, history, period) {
    if (candle_data !== undefined
        && candle_data !== null
        && candle_data.length != 0
        && !("detail" in candle_data)) {
            if (candle_data[0].open === null) {
                console.log("got partially null data from REST API: ")
                console.log(candle_data)
            }
            // Now there's some new data to process
            else {
                let existing_data = dataPeriodMap.get(period)
                if (existing_data === null) {
                    dataPeriodMap.set(period, candle_data)
                }
                else {
                    // Not sure if Set is good because it *may*
                    //  mess up data order
                    let merged = null
                    if (history === true) {
                        merged = [...new Set([...candle_data, ...existing_data])]
                        // merged = [...candle_data, ...existing_data]
                    }
                    else {
                        console.log('merging new data, not historial')
                        merged = [...new Set([...existing_data, ...candle_data])]
                    }
                    dataPeriodMap.set(period, merged)
                }
                
                // // Update current_data and latest_bar according to current_period
                // current_data = dataPeriodMap.get(current_period)
                // latest_bar = current_data[current_data.length - 1]
                // latestBarPeriodMap.set(current_period, latest_bar)
                // candleSeries.setData(current_data)
            }
        }
    else {
        console.log("got full null data from REST API: ")
        console.log(candle_data)
    }

    // Update current_data and latest_bar according to current_period
    current_data = dataPeriodMap.get(current_period)
    if (current_data !== null) {
        latest_bar = current_data[current_data.length - 1]
        latestBarPeriodMap.set(current_period, latest_bar)
        candleSeries.setData(current_data)
    }
    
    // DO NOT reset variables
    //  if there are > 1 drawing processes
    if (drawing_processes <= 1) {
        ws_hold = false
        current_historical = true
    }

    drawing_processes -= 1
};

async function getOHLCVEndpoint(exch, bid, qid, s, e, i, history, period) {
    let ohlcv_endpoint = 
        `http://${window.location.host}/api/ohlcvs?exchange=${exch}&base_id=${bid}&quote_id=${qid}&start=${s}&end=${e}&interval=${i}&results_mls=false&empty_ts=true`
    fetch(ohlcv_endpoint)
        .then(response => response.json())
        .then(resp_data => 
            saveDrawCandleSeries(resp_data, history, period)
        )
};

// Read data from ohlcv REST endpoint and save to `data`
async function readRestDrawOHLC(exch, bid, qid, s, e, i, history, period, period_change) {
    current_historical = history
    drawing_processes += 1
    
    if (period_change) {
        current_period = period
        current_interval = periodIntervalMap.get(period)
        current_data = dataPeriodMap.get(period)
        if (current_data !== null) {
            latest_bar = current_data[current_data.length - 1]
            latestBarPeriodMap.set(period, latest_bar)
            candleSeries.setData(current_data)
        }
        await getOHLCVEndpoint(exch, bid, qid, s, e, i, history, period)
    }
    else {
        if (drawing_processes <= 1) {
            await getOHLCVEndpoint(exch, bid, qid, s, e, i, history, period)
        }
    }
};

// Read data from ohlc WS endpoint
function readWSUpdateOHLC(exch, bid, qid, i, period) {
    if (candles_ws === null) {
        candles_ws = new WebSocket(`ws://${window.location.host}/api/ohlcvs`)
    }
    
    candles_ws.onopen = function() {
        // Update var `latest_bar`
        // latest_bar = latestBarPeriodMap.get(period)
        
        let subscribe_msg = JSON.stringify({
            event_type: "subscribe",
            data_type: "ohlcv",
            exchange: exch,
            base_id: bid,
            quote_id: qid,
            interval: i,
            mls: false
        })
        // console.log(candles_msg)
        candles_ws.send(subscribe_msg)
    }

    candles_ws.onmessage = function(event) {
        let parsed_data = JSON.parse(event.data)
        
        // Websocket only process new messages if:
        if (!ws_hold
            && current_historical
            && "time" in parsed_data 
            && parsed_data.open !== null) {
            
            // Test block
            // if (parsed_data.open === null) {
            //     console.log("got partially null bar from ws:")
            //     console.log(parsed_data)
            // }
        
            // TODO: use this part - Compare parsed data to the latest bar data 
            //  to keep new bars persistent on chart
            if (latest_bar === null || parsed_data.time >= latest_bar.time) {
                candleSeries.update(parsed_data)
                latest_bar = parsed_data
                latestBarPeriodMap.set(current_period, latest_bar)
                
                if (
                    latest_bar.time > current_data[current_data.length - 1].time
                ) {
                    console.log("pushing latest_bar into current_data")
                    current_data.push(latest_bar)
                }
                else if (
                    latest_bar.time == current_data[current_data.length - 1].time
                ) {
                    console.log("replacing latest_bar into current_data")
                    current_data.pop()
                    current_data.push(latest_bar)
                }     
            }
            
                // if (latest_bar === null || parsed_data.time == latest_bar.time) {
                //     // console.log('Saving parsed data to latest bar, here is latest_bar: ')
                //     candleSeries.update(parsed_data)
                // }
                // else if (parsed_data.time > latest_bar.time) {
                //     // console.log('Pushing latest bar to data, here is data: ')
                //     current_data.push(latest_bar)
                //     candleSeries.setData(current_data)
                //     latestBarPeriodMap.set(current_period, parsed_data)
                //     // console.log(data)
                //     // console.log('Saving parsed data to latest bar')
    
        }
    }

    candles_ws.onerror = function(event) {
        console.log('Socket is closed due to error. Reconnecting in 1 second...')
        candles_ws = null
        setTimeout(function() {
            readWSUpdateOHLC(exch, bid, qid, i)
        }, 1000)
    }
};

// Time range switcher buttons
var switcherElement = createSimpleSwitcher(periods, periods[0], syncToInterval)
document.getElementById('chart').appendChild(switcherElement)

// Rest and Websocket API to chart, default
syncToInterval(periods[0])

// Go-to-real-time button
var width = 27
var height = 27
var button = document.createElement('div')
button.className = 'go-to-realtime-button'
button.style.left = (chartWidth - width - 60) + 'px'
button.style.top = (chartHeight) + 'px'
button.style.color = '#4c525e'
button.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 14 14" width="14" height="14"><path fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="2" d="M6.5 1.5l5 5.5-5 5.5M3 4l2.5 3L3 10"></path></svg>'
document.getElementById('chart').appendChild(button)
button.addEventListener('click', function() {
	timeScale.scrollToRealTime();
})

button.addEventListener('mouseover', function() {
	button.style.background = 'rgba(250, 250, 250, 1)';
	button.style.color = '#000';
})

button.addEventListener('mouseout', function() {
	button.style.background = 'rgba(250, 250, 250, 0.6)';
	button.style.color = '#4c525e';
})

// Update chart with historical data when viewport changes
timeScale.subscribeVisibleLogicalRangeChange(() => {
    if (updateChartTimer !== null) {
        return;
    }

    // let current_data = dataPeriodMap.get(current_period)
    
    updateChartTimer = setTimeout(() => {
        // console.log("TIMER")
        let logicalRange = timeScale.getVisibleLogicalRange();
        if (logicalRange !== null && current_data !== null) {
            let barsInfo = candleSeries.barsInLogicalRange(logicalRange);
            if (barsInfo !== null && barsInfo.barsBefore < 200 && drawing_processes < 1) {
                let oneIntervalBefore = getTimestampSteps(
                    current_data[0].time * 1000,
                    intervalPreviousMillisecondsMap.get(current_interval),
                    1)
                let nIntervalBefore = getTimestampSteps(
                    oneIntervalBefore,
                    intervalPreviousMillisecondsMap.get(current_interval),
                    periodNumIntervalMap.get(current_period))
                
                // current_historical = true
                readRestDrawOHLC(
                    current_exchange, current_base_id,
                    current_quote_id, nIntervalBefore,
                    oneIntervalBefore, current_interval,
                    true, current_period, false
                )
            }
        }
        updateChartTimer = null;
    }, 50)

    var buttonVisible = timeScale.scrollPosition() < 0;
    button.style.display = buttonVisible ? 'block' : 'none';
})
