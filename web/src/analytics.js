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
async function tableTop500DailyReturn() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/top500dr`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawTableTop500DR(resp_data)
        )
};

function drawTableTop500DR(data) {
    let data_formatted = [
        unpack(data, 'ranking'),
        unpack(data, 'exchange'),
        unpack(data, 'base_id'),
        unpack(data, 'quote_id'),
        unpack(data, 'gavg_daily_return')
    ]

    console.log(data_formatted)

    let table_data = [{
        type: 'table',
        header: {
            values: [['ranking'], ['exchange'], ['base'], ['quote'], ['gavg']],
            align: ["left", "center"],
            line: { width: 1, color: '#506784' },
            fill: { color: '#119DFF' },
            font: { family: "Arial", size: 12, color: "white" }
        },
        cells: {
            values: data_formatted,
            align: ["left", "center"],
            line: { color: "#506784", width: 1 },
            fill: { color: ['#25FEFD', 'white'] },
            font: { family: "Arial", size: 11, color: ["#506784"] }
        }
    }]

    let layout = {
        title: "Top 500 Symbols with Highest Daily Returns since Last Week"
    }

    Plotly.newPlot('top500DRTable', table_data, layout)
}

tableTop500DailyReturn()

// Table for top volume bases traded
async function pieTop10VolumeBase() {
    data_loading = true
    let endpoint =
        `http://${window.location.host}/api/analytics/top10vlmb`
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data =>
            drawPieTop10VLMB(resp_data)
        )
};

function drawPieTop10VLMB(data) {
    let pie_data = [{
        type: "pie",
        values: unpack(data, 'ttl_vol'),
        labels: unpack(data, 'base_id'),
        textinfo: "label+percent",
        textposition: "outside",
        automargin: true
      }]
    
    let layout = {
        title: "Top 10 Mostly Trade Base IDs by Volume since Last Week",
        height: 800,
        width: 800,
        margin: {"t": 0, "b": 0, "l": 0, "r": 0},
        showlegend: false
    }

    Plotly.newPlot('top10VLMBPie', pie_data, layout)
}

pieTop10VolumeBase()
