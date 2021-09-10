function unpack(data, key) {
    return data.map(function (e) {
        return e[key];
    });
}

function unpackDate(data, dateKey) {
    return data.map(function (e) {
        return new Date(e[dateKey] * 1000);
    });
}

// Table for top daily returns
async function tableTopGeoDailyReturn() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/geodr?limit=500`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawTableGeoDR(resp_data)
        )
};

function drawTableGeoDR(data) {
    let data_formatted = [
        unpack(data, 'exchange'),
        unpack(data, 'base_id'),
        unpack(data, 'quote_id'),
        unpack(data, 'daily_return_pct')
    ]

    // console.log(data_formatted)

    let table_data = [{
        type: 'table',
        columnwidth: 400,
        header: {
            values: [['exchange'], ['base'], ['quote'], ['geometric average return (%)']],
            align: ["left", "center"],
            line: { width: 1, color: '#506784' },
            fill: { color: '#119DFF' },
            font: { family: "Trebuchet MS", size: 14, color: "white" }
        },
        cells: {
            values: data_formatted,
            align: ["left", "center"],
            line: { color: "#506784", width: 1 },
            fill: { color: ['#25FEFD', 'white'] },
            font: { family: "Trebuchet MS", size: 14, color: ["#506784"] }
        }
    }]

    let layout = {
        title: "Top 500 Symbols with Highest Daily Returns since Last Week"
    }

    Plotly.newPlot('DRTable', table_data, layout)
}

// Table for top 500 weekly return
async function tableTopWeeklyReturn() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/wr?limit=500`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawTableWR(resp_data)
        )
};

function drawTableWR(data) {
    let data_formatted = [
        unpack(data, 'time'),
        unpack(data, 'exchange'),
        unpack(data, 'base_id'),
        unpack(data, 'quote_id'),
        unpack(data, 'weekly_return_pct')
    ]

    // console.log(data_formatted)

    let table_data = [{
        type: 'table',
        header: {
            values: [['time'], ['exchange'], ['base'], ['quote'], ['lastest weekly return (%)']],
            align: ["left", "center"],
            line: { width: 1, color: '#506784' },
            fill: { color: '#119DFF' },
            font: { family: "Trebuchet MS", size: 14, color: "white" }
        },
        cells: {
            values: data_formatted,
            align: ["left", "center"],
            line: { color: "#506784", width: 1 },
            fill: { color: ['#25FEFD', 'white'] },
            font: { family: "Trebuchet MS", size: 14, color: ["#506784"] }
        }
    }]

    let layout = {
        title: "Top 500 Symbols with Highest Weekly Returns since Last Week"
    }

    Plotly.newPlot('WRTable', table_data, layout)
}

// Pie chart for top volume bases traded
async function pieTopQuoteVolume() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/top20qvlm`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawPieTopQuoteVolume(resp_data)
        )
};

function drawPieTopQuoteVolume(data) {
    let pie_data = [{
        type: "pie",
        values: unpack(data, 'total_volume'),
        labels: unpack(data, 'bqgrp'),
        textinfo: "label+percent",
        textposition: "outside",
        automargin: true
      }]
    
    let layout = {
        title: "Top 20 Mostly Traded Base IDs-Quote IDs by Quoted Volume since Last Week",
        height: 800,
        width: 800,
        // margin: {"t": 0, "b": 0, "l": 0, "r": 0},
        showlegend: false
    }

    Plotly.newPlot('topQuoteVlm', pie_data, layout)
}

// Histogram of geometric daily return
async function HistGeoDailyReturn() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/geodr?cutoff_upper_pct=10000&limit=-1`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawHistGeoDR(resp_data))
}

function drawHistGeoDR(data) {
    let hist_data = unpack(data, 'daily_return_pct')

    let trace = [{
        x: hist_data,
        type: 'histogram',
    }]

    let layout = {
        title: "Histogram of Geometric Average Daily Return since Last Week"
    }

    Plotly.newPlot('HistGeoDR', trace, layout)
}

// Histogram of weekly return
async function HistWeeklyReturn() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/wr?cutoff_upper_pct=10000&limit=-1`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawHistWR(resp_data))
}

function drawHistWR(data) {
    let hist_data = unpack(data, 'weekly_return_pct')

    let trace = [{
        x: hist_data,
        type: 'histogram',
    }]

    let layout = {
        title: "Histogram of Weekly Return since Last Week"
    }

    Plotly.newPlot('HistWR', trace, layout)
}

tableTopGeoDailyReturn()
tableTopWeeklyReturn()
pieTopQuoteVolume()
HistGeoDailyReturn()
HistWeeklyReturn()
