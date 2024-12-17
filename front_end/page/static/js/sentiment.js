var pnlDistChart;


async function getPnlDistData() {
const resp = await fetch('/pnl_dist');
const data = await resp.json();
return data;
}

var pnlDistDatasetKeys;

async function startPnlDistChart() {
const data = await getPnlDistData();
const PnlDistCanvas = document.getElementById('pnlDistCanvas');

// 获取数据的所有键，除去不需要作为数据集的键
pnlDistDatasetKeys = Object.keys(data[0]).filter(key => key !== 'trading_date');

// 为每个键创建一个数据集
const datasets = pnlDistDatasetKeys.map(key => ({
    label: key,
    data: data.map(item => item[key]),
    borderColor: getColorPnlDist(key), 
    pointRadius: 0,
    showLine: true,
}));

pnlDistChart = new Chart(PnlDistCanvas, {
    type: 'line',
    data: {
    labels: data.map(item => item.trading_date),
    datasets: datasets
    },
    options: {
    interaction: {
        mode: 'index',
        intersect: false
    },
    responsive: true,
    aspectRatio: 6,
    scales: {
        x: {
            grid: {
            display: true,
            color: '#CCCCCC'
            },
            ticks: {
            color: '#CCCCCC'
            }
        },
        y: {
        grid: {
            display: true,
            color: '#CCCCCC'
        }
        }
    }
    }
});

return true;
}

function getColorPnlDist(key) {
const colorDict = {
    '<=-3%': '548687',
    '(-3%, -2%]': '8FBC94',
    '(-2%, -1%]': 'C5E99B',
    '(-1%, 0%]': 'EFFFE9',
    '(0%, 1%)': 'f9cdad',
    '[1%, 2%)': 'fc9d9a',
    '[2%, 3%)': 'fe4365',
    '>=3%': 'bd1550',
}
var color = '#' + colorDict[key];
return color ? color : '#f4f7f7';
}

async function updatePnlDistChart() {
const data = await getPnlDistData();
pnlDistChart.data.labels = data.map(item => item.trading_date);

// 更新每个数据集
pnlDistChart.data.datasets.forEach(dataset => {
    dataset.data = data.map(item => item[dataset.label]);
});

pnlDistChart.update();
}

if (startPnlDistChart()) {
/// 图片已经初始化，可以开启定时更新;
setInterval(updatePnlDistChart, 16000);
}


async function getTodayPnlDist() {
const resp = await fetch('/current_pnl_dist');
const data = await resp.json();
return data;
}


let pnlDistPieChart = null; // 用于存储图表实例，以便可以更新

async function drawTodayPnlDistPieChart() {
const data = await getTodayPnlDist(); // 获取数据

const ctx = document.getElementById('todayPnlDistCanvas').getContext('2d');

const labels = data.map(item => item.range); // 获取所有的范围作为标签
const cntData = data.map(item => item.cnt); // 获取所有的计数作为数据
const backgroundColors = data.map(item => getColorPnlDist(item.range)); // 根据range获取颜色

// 如果图表已经存在，则更新数据
if (pnlDistPieChart) {
    pnlDistPieChart.data.labels = labels;
    pnlDistPieChart.data.datasets[0].data = cntData;
    pnlDistPieChart.data.datasets[0].backgroundColor = backgroundColors;
    pnlDistPieChart.update();
} else {
    // 创建新的饼图
    pnlDistPieChart = new Chart(ctx, {
    type: 'pie',
    data: {
        labels: labels,
        datasets: [{
        label: 'PnL Distribution',
        data: cntData,
        backgroundColor: backgroundColors,
        hoverBackgroundColor: backgroundColors
        }]
    },
    options: {
        responsive: true,
        plugins: {
        legend: {
            position: 'top',
        },
        tooltip: {
            enabled: true
        }
        }
    }
    });
}
return true;
}

if (drawTodayPnlDistPieChart()) {
setInterval(drawTodayPnlDistPieChart, 16000);
}