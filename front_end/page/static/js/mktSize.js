
async function getOptMktSizePositionData() {
    const response = await fetch('/opt_sym_mkt_size');
    const data = await response.json();
    const mkt_d = JSON.parse(data.mkt);
    const oi_d = JSON.parse(data.oi);
    return [mkt_d, oi_d];
}

var optMktChart;
var optOiChart;

function hideNegativeCurves(chart) {
    chart.data.datasets.forEach(function (dataset, i) {
        var lastValue = dataset.data[dataset.data.length - 1];
        if (lastValue <= 0) {
            chart.getDatasetMeta(i).hidden = true;
        } else {
            chart.getDatasetMeta(i).hidden = false;
        }
    });
}

async function drawOptSymMktSizeCharts() {
    const [mkt_data, oi_data] = await getOptMktSizePositionData();
    
    const mktCanvas = document.getElementById('mktSizeCanvas');
    const oiCanvas = document.getElementById('OiSizeCanvas');

    const generateDatasets = (data) => {
        const datasets = [];
        const columns = Object.keys(data[0]);
        const dateIndex = columns.indexOf('trading_date');
        
        for (let i = 1; i < columns.length; i++) {
            const randomColor = '#' + Math.floor(Math.random()*16777215).toString(16); // Generate random color
            datasets.push({
                label: columns[i],
                data: data.map(item => item[columns[i]]),
                borderColor: randomColor,
                pointRadius: 0,
                showLine: true
            });
        }

        return datasets;
    };

    optMktChart = new Chart(mktCanvas, {
        type: 'line',
        data: {
            labels: mkt_data.map(item => item.trading_date),
            datasets: generateDatasets(mkt_data)
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'Option Symbols Mkt Cap Size' // Add your desired title here
                }
            },
            interaction: {
                mode: 'nearest',
                intersect: false
            },
            responsive: true,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                },
            },
            animation: false
        }
    });

    optOiChart = new Chart(oiCanvas, {
        type: 'line',
        data: {
            labels: oi_data.map(item => item.trading_date),
            datasets: generateDatasets(oi_data)
        },
        options: {
            plugins: {
                title: {
                    display: true,
                    text: 'Option Symbols Oi Size' // Add your desired title here
                }
            },
            interaction: {
                mode: 'nearest',
                intersect: false
            },
            responsive: true,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                },
            },
            animation: false
        }
    });

    hideNegativeCurves(optOiChart);
    hideNegativeCurves(optMktChart);

    return true;
}

async function updateOptMktSizeCharts() {
    const [mkt_data, oi_data] = await getOptMktSizePositionData();

    const updateDatasets = (chart, newData) => {
        const columns = Object.keys(newData[0]);
        const dateIndex = columns.indexOf('trading_date');

        for (let i = 1; i < columns.length; i++) {
            const columnName = columns[i];
            const newDataArray = newData.map(item => item[columnName]);
            const datasetIndex = chart.data.datasets.findIndex(dataset => dataset.label === columnName);

            if (datasetIndex !== -1) {
                chart.data.datasets[datasetIndex].data = newDataArray;
            } else {
                const randomColor = '#' + Math.floor(Math.random()*16777215).toString(16); // Generate random color
                chart.data.datasets.push({
                    label: columnName,
                    data: newDataArray,
                    borderColor: randomColor,
                    pointRadius: 0,
                    showLine: true
                });
            }
        }
    };

    updateDatasets(optMktChart, mkt_data);
    updateDatasets(optOiChart, oi_data);

    hideNegativeCurves(optOiChart);
    hideNegativeCurves(optMktChart);

    optMktChart.update();
    optOiChart.update();
}

if (drawOptSymMktSizeCharts()) {
    setInterval(updateOptMktSizeCharts, 60000);
}