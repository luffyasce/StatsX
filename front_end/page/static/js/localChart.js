var keyContainer = document.getElementById('keyContainer');
var contractParam = keyContainer.getAttribute('data-key');

let titleElement = document.getElementById('pageTitle');

function getMarkDict() {
    const cookies = document.cookie;
    // 解析Cookie中的数据
    const markDict = JSON.parse(cookies.split("; ").find(cookie => cookie.startsWith("markDict=")).split("=")[1]);
    return markDict;
}

function updateTitle() {
    const now = new Date();
    const hours = now.getHours().toString().padStart(2, '0');
    const minutes = now.getMinutes().toString().padStart(2, '0');
    const seconds = now.getSeconds().toString().padStart(2, '0');
    const timeString = `${hours}:${minutes}:${seconds}`;

    let storedMarkedDict = getMarkDict();
    titleElement.innerHTML = contractParam + " " + storedMarkedDict[contractParam] + " | " + timeString;
}


/// 全局定义几个chart，用以更新dataset
var ChartIVRange;
var lineChart001;
var lineChart002;
var lineChart01;
var barChart01;
var barChart02;
var barChart03;
var lineChart02;
var barChart04;
var chart0301;
var chart0302;
var chart0401;
var chart0402;
var barChartHFO;
var chartOptPriceTest;

function negPosBar(value, multiplier) {
    const disp = document.createElement('div');
    disp.classList.add('size-bar');
    disp.style.backgroundColor = value > 0 ? '#D81159' : '#379392';
    disp.style.width = `${Math.min(Math.abs(value) * multiplier, 50)}px`;
    disp.style.height = `2px`;
    return disp;
}


function formatter(header, original_val_, val_, cell) {
    if (header == 'net_pos_corr') {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        const bar = negPosBar(original_val_, 50);
        cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
        cell.appendChild(bar);
    } else if (header == 'broker') {
        var backgroundColor = "#FFFFF3";
        repl_ = original_val_.replace(/(W|L)/g, function(match, p1) {
            if (p1 === 'W') {
                backgroundColor = "#F7AA97";
            } else if (p1 === 'L') {
                backgroundColor = "#CADBE9";
            }
            return '';
        });
        Array.prototype.slice.call(cell.parentNode.children, 2).forEach(function(child) {
            child.style.backgroundColor = backgroundColor;
        });
        cell.innerHTML = repl_;
    } else if (header.includes('net_pos')) {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        const adjVal = original_val_ / 50000;
        const bar = negPosBar(adjVal, 50);
        cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
        cell.appendChild(bar);
    } else if (header.includes('net_chg')) {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        const adjVal = original_val_ / 5000;
        const bar = negPosBar(adjVal, 50);
        cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
        cell.appendChild(bar);
    }
}

async function updateValidPosTable() {
    const response = await fetch('/valid_pos_by_contract/' + contractParam);
    const data = await response.json();

    const table = document.getElementById('validPos');

    table.innerHTML = '';

    if (!data || data.length == 0) {
        return;
    }

    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const headerRow = table.insertRow();
    headers.forEach(header => {
        const th = document.createElement('th');
        if (header.includes('corr')) {
            th.style.backgroundColor = "#9DC3C1";
        } else if (header.includes('net_pos')) {
            th.style.backgroundColor = "#a3daff";
        } else if (header.includes('net_chg')) {
            th.style.backgroundColor = "#fbd14b";
        } 
        th.textContent = header.toUpperCase();
        headerRow.appendChild(th);
    });

    data.forEach(item => {
        const row = table.insertRow();
        headers.forEach(header => {
            const cell = row.insertCell();
            var original_val_ = item[header];
            // if val_ is a number, then format it as XXX,XXX,XXX.XX else leave it as it is            
            if ((!isNaN(original_val_)) && (original_val_ != null)) {
                var val_ = original_val_.toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");;
            } else {
                var val_ = original_val_;
            }
            cell.textContent = val_;
            
            formatter(header, original_val_, val_, cell);
        });
    });
}

async function updateVipPosTable() {
    const response = await fetch('/vip_position/' + contractParam);
    const data = await response.json();

    const table = document.getElementById('vipPos');

    table.innerHTML = '';

    if (!data || data.length == 0) {
        return;
    }

    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const headerRow = table.insertRow();
    headers.forEach(header => {
        const th = document.createElement('th');
        if (header.includes('corr')) {
            th.style.backgroundColor = "#9DC3C1";
        } else if (header.includes('net_pos')) {
            th.style.backgroundColor = "#a3daff";
        } else if (header.includes('net_chg')) {
            th.style.backgroundColor = "#fbd14b";
        } 
        th.textContent = header.toUpperCase();
        headerRow.appendChild(th);
    });

    // Generate table rows dynamically based on data
    var prev_sym = '';
    var prev_con = '';

    data.forEach(item => {
        const row = table.insertRow();
        headers.forEach(header => {
            const cell = row.insertCell();
            if (header == 'symbol') {
                if (prev_sym != item[header]) {
                cell.textContent = item[header];
                prev_sym = item[header];
                }
            } else if (header == 'contract') {
                if (prev_con != item[header]) {
                cell.textContent = item[header];
                prev_con = item[header];
                }
            } else {
                var original_val_ = item[header];
                // if val_ is a number, then format it as XXX,XXX,XXX.XX else leave it as it is            
                if ((!isNaN(original_val_)) && (original_val_ != null)) {
                    var val_ = original_val_.toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");;
                } else {
                    var val_ = original_val_;
                }
                cell.textContent = val_;
                
                formatter(header, original_val_, val_, cell);
            }
        });
    });
}


async function updateOptionVipPosTable() {
    const response = await fetch('/vip_option_position/' + contractParam);
    const data = await response.json();

    const table = document.getElementById('vipOptPos');

    table.innerHTML = '';

    if (!data || data.length == 0) {
        return;
    }

    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const headerRow = table.insertRow();
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header.toUpperCase();
        headerRow.appendChild(th);
    });

    data.forEach(item => {
        const row = table.insertRow();
        headers.forEach(header => {
            const cell = row.insertCell();
            var original_val_ = item[header];
            // if val_ is a number, then format it as XXX,XXX,XXX.XX else leave it as it is            
            if ((!isNaN(original_val_)) && (original_val_ != null)) {
                var val_ = original_val_.toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");;
            } else {
                var val_ = original_val_;
            }
            cell.textContent = val_;
            
            formatter(header, original_val_, val_, cell);
        });
    });
}


async function requestData() {
    const response = await fetch('/stakes/' + contractParam);
    const data = await response.json();
    const price_data = JSON.parse(data.price_data);
    const opt_price_data = JSON.parse(data.opt_price_data);
    const stake_data = JSON.parse(data.stake_data);
    const md_assess = JSON.parse(data.md_assess);
    const iv_curve = JSON.parse(data.iv_curve);
    const hist_daily_md = JSON.parse(data.hist_daily_md);
    const und_act_vol = JSON.parse(data.und_act_vol);
    const und_swap_vol = JSON.parse(data.und_swap_vol);
    const opt_act_vol = JSON.parse(data.opt_act_vol);
    const opt_swap_vol = JSON.parse(data.opt_swap_vol);
    const iv_range_data = JSON.parse(data.cp_iv_data);
    const hist_fo_data = JSON.parse(data.hist_fo_data);
    const opt_price_test = JSON.parse(data.opt_price_test);

    return [price_data, opt_price_data, stake_data, md_assess, iv_curve, hist_daily_md, und_act_vol, und_swap_vol, opt_act_vol, opt_swap_vol, iv_range_data, hist_fo_data, opt_price_test];
}

function getRandomColor(keyParam) {
    const colors = {
        "long-open": "#fe4365",
        "short-close": "#fc9d9a",
        "long-swap": "#f9cdad",
        "neut-open": "#f8ecc9",
        "neut-close": "#c8c8a9",
        "neut-swap": "#c6e5d9",
        "short-open": "#79bd9a",
        "long-close": "#a8dba8",
        "short-swap": "#cff09e",
    };
    return colors[keyParam]
}

var VolKeys;

function dataset_formation(dat, xLabel) {
    const datasets = [];
    const keys = Object.keys(dat[0]);
    VolKeys = keys.filter(key => key !== xLabel);
    VolKeys.forEach((key, index) => {
        datasets.push({
            label: key.toUpperCase(),
            data: dat.map(item => item[key]),
            borderColor: getRandomColor(key),
            yAxisID: 'y',
            pointRadius: 0,
            showLine: true
        });
    });
    return datasets;
}

// 创建一个函数来填充两条线之间的区域
function __fillArea(chart, dataset1, dataset2, fillColor, alpha) {
    var meta1 = chart.getDatasetMeta(dataset1);
    var meta2 = chart.getDatasetMeta(dataset2);

    if (meta1.data.length == 0 || meta2.data.length == 0) {
        return;
    }

    var ctx = chart.ctx;
    ctx.save();

    ctx.fillStyle = fillColor;
    ctx.globalAlpha = alpha;

    ctx.beginPath();

    for (var i = 0; i < meta1.data.length; i++) {
        var pt1 = meta1.data[i];
        var pt2 = meta2.data[i];
        var x = pt1.x; // 获取X坐标

        if (i === 0) {
            ctx.moveTo(x, pt1.y);
        } else {
            ctx.lineTo(x, pt1.y);
        }
    }

    for (var i = meta2.data.length - 1; i >= 0; i--) {
        var pt2 = meta2.data[i];
        ctx.lineTo(pt2.x, pt2.y);
    }

    ctx.closePath();
    ctx.fill();

    ctx.restore();
}

function fillArea(chart, dataset1, dataset2, fillColor, alpha) {
    try {
        __fillArea(chart, dataset1, dataset2, fillColor, alpha);
    } catch (error) {
        console.error("ERR fill area" + error.message);
    }
}

async function drawLocalCharts() {
    const [ts_data, opt_ts_data, stake_data, md_assess, iv_data, hist_daily_md, und_act_vol, und_swap_vol, opt_act_vol, opt_swap_vol, iv_range_data, hist_fo_data, opt_price_test] = await requestData();

    var histMdUndCloseMin = Math.min(...hist_daily_md.filter(item => item.und_close !== null).map(item => item.und_close));
    var histMdUndCloseMax = Math.max(...hist_daily_md.filter(item => item.und_close !== null).map(item => item.und_close));
    var currMdUndCloseMin = Math.min(...ts_data.filter(item => item.last !== null).map(item => item.last));
    var currMdUndCloseMax = Math.max(...ts_data.filter(item => item.last !== null).map(item => item.last));

    const canvasIVRange = document.getElementById('CanvasIVRange');

    const canvasOptPriceTest = document.getElementById('CanvasTestPrice');

    const canvasMdUpLeft = document.getElementById('UpLeftMdCanvas');
    const canvasHistMd = document.getElementById('histMdCanvas');
    
    const canvasLeft = document.getElementById('chartCanvas-left');
    const canvasRight1 = document.getElementById('chartCanvas-right1');
    const canvasRight2 = document.getElementById('chartCanvas-right2');
    const canvasRight3 = document.getElementById('chartCanvas-right3');

    const ivCurveCanvas = document.getElementById('ivCurveCanvas');
    const MaCanvas = document.getElementById('MaCanvas');

    const canvas0301 = document.getElementById('Canvas3Left');
    const canvas0302 = document.getElementById('Canvas3Right');

    const canvas0401 = document.getElementById('Canvas4Left');
    const canvas0402 = document.getElementById('Canvas4Right');

    const canvasHFO = document.getElementById('histFoCanvas');

    ChartIVRange = new Chart(canvasIVRange, {
        type: 'line',
        data: {
        labels: iv_range_data.map(item => item.trading_date),
        datasets: [
            {
                label: 'CALL UPPER',
                data: iv_range_data.map(item => item.call_upper),
                borderColor: '#ff4e50',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'CALL LOWER',
                data: iv_range_data.map(item => item.call_lower),
                borderColor: '#fc913a',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'PUT UPPER',
                data: iv_range_data.map(item => item.put_upper),
                borderColor: '#005f6b',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'PUT LOWER',
                data: iv_range_data.map(item => item.put_lower),
                borderColor: '#008c9e',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'CALL AVG',
                data: iv_range_data.map(item => item.call_avg),
                borderColor: '#f9d423',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'PUT AVG',
                data: iv_range_data.map(item => item.put_avg),
                borderColor: '#80d4f6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            aspectRatio: 6,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            }
        }
    });
    
    fillArea(ChartIVRange, 0, 1, "#f1bbba", 0.3);
    fillArea(ChartIVRange, 2, 3, "#aacfd0", 0.3);


    lineChart001 = new Chart(canvasMdUpLeft, {
        type: 'line',
        data: {
        labels: opt_ts_data.map(item => item.datetime_minute),
        datasets: [
            {
                label: contractParam,
                data: opt_ts_data.map(item => item.opt_last),
                borderColor: '#80d4f6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'Option OI',
                data: opt_ts_data.map(item => item.opt_oi),
                borderColor: '#090707',
                yAxisID: 'optOi',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'RM LONG',
                data: opt_ts_data.map(item => item.rm_long),
                borderColor: '#ee2560',
                yAxisID: 'rm',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'RM SHORT',
                data: opt_ts_data.map(item => item.rm_short),
                borderColor: '#4f953b',
                yAxisID: 'rm',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'IV',
                data: opt_ts_data.map(item => item.iv),
                borderColor: '#6a60a9',
                yAxisID: 'iv',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                type: 'bar',
                label: 'RM DELTA',
                data: opt_ts_data.map(item => item.rm_delta),
                backgroundColor: '#FFBC42',
                yAxisID: 'rmd',
            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                },
                optOi: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                rm: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                rmd: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                iv: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                }
            }
        }
    });

    lineChart002 = new Chart(canvasHistMd, {
        type: 'line',
        data: {
        labels: hist_daily_md.map(item => item.trading_date),
        datasets: [
            {
                label: contractParam.split('-')[0],
                data: hist_daily_md.map(item => item.und_close),
                borderColor: '#80d4f6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: contractParam,
                data: hist_daily_md.map(item => item.opt_close),
                borderColor: '#011627',
                yAxisID: 'opt',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'IV',
                data: hist_daily_md.map(item => item.iv),
                borderColor: '#6a60a9',
                yAxisID: 'iv',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                type: 'bar',
                label: 'RM LONG',
                data: hist_daily_md.map(item => item.rm_long),
                backgroundColor: '#ee2560',
                yAxisID: 'rm',
                hidden: true,
            },
            {
                type: 'bar',
                label: 'RM SHORT',
                data: hist_daily_md.map(item => item.rm_short),
                backgroundColor: '#4f953b',
                yAxisID: 'rm',
                hidden: true,
            },
            {
                type: 'bar',
                label: 'VP MAIN',
                data: hist_daily_md.map(item => item.vp_main),
                backgroundColor: '#F6B352',
                yAxisID: 'vp',
                hidden: true,
            },
            {
                type: 'bar',
                label: 'VP SYMBOL',
                data: hist_daily_md.map(item => item.vp_symbol),
                backgroundColor: '#F68657',
                yAxisID: 'vp',
                hidden: true,
            },
            {
                label: 'SPOT',
                data: hist_daily_md.map(item => item.spot),
                borderColor: '#C5C6B6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
                hidden: true,
            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: lineChart001.height,
            scales: {
                y: {
                    grid: {
                        display: true
                    },
                    type: 'linear',
                    position: 'left',
                    // min: histMdUndCloseMin,
                    // max: histMdUndCloseMax,
                },
                opt: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                rm: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                vp: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                iv: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                }
            }
        }
    });

    barChartHFO = new Chart(canvasHFO, {
        type: 'bar',
        data: {
        labels: hist_fo_data.map(item => item.price),
        datasets: [
            {
                label: 'stake',
                data: stake_data.map(item => item.stake),
                backgroundColor: '#D81159'
            }
        ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            height: lineChart001.height,
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    min: histMdUndCloseMin,
                    max: histMdUndCloseMax,
                }
            }
        }
    });

    lineChart01 = new Chart(canvasLeft, {
        type: 'line',
        data: {
        labels: ts_data.map(item => item.datetime_minute),
        datasets: [
            {
                label: contractParam.split('-')[0],
                data: ts_data.map(item => item.last),
                borderColor: '#80d4f6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'OI',
                data: ts_data.map(item => item.open_interest),
                borderColor: '#090707',
                yAxisID: 'undOi',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'RM LONG',
                data: ts_data.map(item => item.rm_long),
                borderColor: '#ee2560',
                yAxisID: 'rm',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                label: 'RM SHORT',
                data: ts_data.map(item => item.rm_short),
                borderColor: '#4f953b',
                yAxisID: 'rm',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                type: 'bar',
                label: 'RM DELTA',
                data: ts_data.map(item => item.rm_delta),
                backgroundColor: '#FFBC42',
                yAxisID: 'rmd',
            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            scales: {
                x: {
                    ticks: {
                        maxTicksLimit: 6,
                        maxRotation: 0,
                        minRotation: 0
                    }
                },
                y: {
                    grid: {
                        display: true
                    },
                    // min: currMdUndCloseMin,
                    // max: currMdUndCloseMax,
                },
                undOi: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                rm: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                },
                rmd: {
                    display: false,
                    position: 'right', // 可能需要根据实际情况调整位置
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
    
    // 创建右边的水平柱状图
    barChart01 = new Chart(canvasRight1, {
        type: 'bar',
        data: {
        labels: stake_data.map(item => item.last),
        datasets: [
            {
                label: 'vol stake',
                data: stake_data.map(item => item.volume_stake),
                backgroundColor: '#D81159'
            }
        ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            height: lineChart01.height,
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    min: currMdUndCloseMin,
                    max: currMdUndCloseMax,
                }
            }
        }
    });

    barChart02 = new Chart(canvasRight2, {
        type: 'bar',
        data: {
        labels: stake_data.map(item => item.last),
        datasets: [
            {
                label: 'oi stake',
                data: stake_data.map(item => item.oi_stake),
                backgroundColor: '#8EC0E4'
            }
        ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            height: lineChart01.height,
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    min: currMdUndCloseMin,
                    max: currMdUndCloseMax,
                }
            }
        }
    });

    barChart03 = new Chart(canvasRight3, {
        type: 'bar',
        data: {
        labels: stake_data.map(item => item.last),
        datasets: [
            {
                label: 'big orders',
                data: stake_data.map(item => item.filtered_order_stake),
                backgroundColor: '#218380'
            },
            {
                label: 'abs big orders',
                data: stake_data.map(item => item.abs_big_order_stake),
                backgroundColor: '#ffda8e',
            }
        ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            height: lineChart01.height,
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    min: currMdUndCloseMin,
                    max: currMdUndCloseMax,
                }
            }
        }
    });

    // md assess
    barChart04 = new Chart(MaCanvas, {
        type: 'bar',
        data: {
        labels: md_assess.map(item => item.datetime_minute),
        datasets: [
            {
                label: 'rSk Value',
                data: md_assess.map(item => item.sk),
                backgroundColor: '#218380',
            },
            {
                label: 'C/P Ratio',
                data: md_assess.map(item => item.cp_ratio),
                backgroundColor: '#FDD692',
                yAxisID: 'cpr'
            },
            {
                type: 'line',
                label: 'MKV Call',
                data: md_assess.map(item => item.mkv_call),
                borderColor: '#ef5285',
                yAxisID: 'mkv',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                type: 'line',
                label: 'MKV Put',
                data: md_assess.map(item => item.mkv_put),
                borderColor: '#60c5ba',
                yAxisID: 'mkv',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            aspectRatio: 3,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                },
                cpr: {
                    display: false,
                    position: 'right',
                    grid: {
                        display: false
                    }
                },
                mkv: {
                    display: false,
                    position: 'right',
                    grid: {
                        display: false
                    }
                }
            }
        }
    });

    // 创建下方的iv曲线
    lineChart02 = new Chart(ivCurveCanvas, {
        type: 'line',
        data: {
        labels: iv_data.map(item => item.strike),
        datasets: [
            {
                label: 'IV curve',
                data: iv_data.map(item => item.iv),
                borderColor: '#80d4f6',
            },
            {
                type: 'bar',
                label: 'Mkt Value',
                data: iv_data.map(item => item.mktVal),
                backgroundColor: '#ffda8e',
                yAxisID: 'mktVal',

            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: barChart04.height,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                },
                mktVal: {
                    position: 'right', // 可能需要根据实际情况调整位置
                    display: false,
                    grid: {
                        display: false
                    }
                }
            }
        }
    });

    chart0301 = new Chart(canvas0301, {
        type: 'line',
        data: {
            labels: und_act_vol.map(item => item.datetime_minute),
            datasets: dataset_formation(und_act_vol, 'datetime_minute')
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            aspectRatio: 3,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Und Act',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    chart0302 = new Chart(canvas0302, {
        type: 'line',
        data: {
            labels: und_swap_vol.map(item => item.datetime_minute),
            datasets: dataset_formation(und_swap_vol, 'datetime_minute')
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: chart0301.height,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Und Swap',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    chart0401 = new Chart(canvas0401, {
        type: 'line',
        data: {
            labels: opt_act_vol.map(item => item.datetime_minute),
            datasets: dataset_formation(opt_act_vol, 'datetime_minute')
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            aspectRatio: 3,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Opt Act',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    chart0402 = new Chart(canvas0402, {
        type: 'line',
        data: {
            labels: opt_swap_vol.map(item => item.datetime_minute),
            datasets: dataset_formation(opt_swap_vol, 'datetime_minute')
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: chart0401.height,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Opt Swap',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    chartOptPriceTest = new Chart(canvasOptPriceTest, {
        type: 'line',
        data: {
            labels: opt_price_test.map(item => item.spot_price),
            datasets: [
                {
                    label: 'option price (one day ahead)',
                    data: opt_price_test.map(item => item.opt_price),
                    borderColor: '#80d4f6',
                }
            ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: chart0401.height,
            scales: {
                y: {
                    grid: {
                        display: true
                    }
                }
            }
        }
    });

    return true;
}

async function updateLocalCharts() {
    const [ts_data, opt_ts_data, stake_data, md_assess, iv_data, hist_daily_md, und_act_vol, und_swap_vol, opt_act_vol, opt_swap_vol, iv_range_data, hist_fo_data, opt_price_test] = await requestData();

    ChartIVRange.data.labels = iv_range_data.map(item => item.trading_date);
    ChartIVRange.data.datasets[0].data = iv_range_data.map(item => item.call_upper);
    ChartIVRange.data.datasets[1].data = iv_range_data.map(item => item.call_lower);
    ChartIVRange.data.datasets[2].data = iv_range_data.map(item => item.put_upper);
    ChartIVRange.data.datasets[3].data = iv_range_data.map(item => item.put_lower);
    ChartIVRange.data.datasets[4].data = iv_range_data.map(item => item.call_avg);
    ChartIVRange.data.datasets[5].data = iv_range_data.map(item => item.put_avg);

    lineChart001.data.labels = opt_ts_data.map(item => item.datetime_minute);
    lineChart001.data.datasets[0].data = opt_ts_data.map(item => item.opt_last);
    lineChart001.data.datasets[1].data = opt_ts_data.map(item => item.opt_oi);
    lineChart001.data.datasets[2].data = opt_ts_data.map(item => item.rm_long);
    lineChart001.data.datasets[3].data = opt_ts_data.map(item => item.rm_short);
    lineChart001.data.datasets[4].data = opt_ts_data.map(item => item.iv);
    lineChart001.data.datasets[5].data = opt_ts_data.map(item => item.rm_delta);

    lineChart002.data.labels = hist_daily_md.map(item => item.trading_date);
    lineChart002.data.datasets[0].data = hist_daily_md.map(item => item.und_close);
    lineChart002.data.datasets[1].data = hist_daily_md.map(item => item.opt_close);
    lineChart002.data.datasets[2].data = hist_daily_md.map(item => item.iv);
    lineChart002.data.datasets[3].data = hist_daily_md.map(item => item.rm_long);
    lineChart002.data.datasets[4].data = hist_daily_md.map(item => item.rm_short);
    lineChart002.data.datasets[5].data = hist_daily_md.map(item => item.vp_main);
    lineChart002.data.datasets[6].data = hist_daily_md.map(item => item.vp_symbol);
    lineChart002.data.datasets[7].data = hist_daily_md.map(item => item.spot);

    barChartHFO.data.label = hist_fo_data.map(item => item.price);
    barChartHFO.data.datasets[0].data = hist_fo_data.map(item => item.stake);

    lineChart01.data.labels = ts_data.map(item => item.datetime_minute);
    lineChart01.data.datasets[0].data = ts_data.map(item => item.last);
    lineChart01.data.datasets[1].data = ts_data.map(item => item.open_interest);
    lineChart01.data.datasets[2].data = ts_data.map(item => item.rm_long);
    lineChart01.data.datasets[3].data = ts_data.map(item => item.rm_short);
    lineChart01.data.datasets[4].data = ts_data.map(item => item.rm_delta);

    barChart01.data.labels = stake_data.map(item => item.last);
    barChart01.data.datasets[0].data = stake_data.map(item => item.volume_stake);

    barChart02.data.labels = stake_data.map(item => item.last);
    barChart02.data.datasets[0].data = stake_data.map(item => item.oi_stake);

    barChart03.data.labels = stake_data.map(item => item.last);
    barChart03.data.datasets[0].data = stake_data.map(item => item.filtered_order_stake);
    barChart03.data.datasets[1].data = stake_data.map(item => item.abs_big_order_stake);

    lineChart02.data.labels = iv_data.map(item => item.strike);
    lineChart02.data.datasets[0].data = iv_data.map(item => item.iv);
    lineChart02.data.datasets[1].data = iv_data.map(item => item.mktVal);    

    barChart04.data.labels = md_assess.map(item => item.datetime_minute);
    barChart04.data.datasets[0].data = md_assess.map(item => item.sk);
    barChart04.data.datasets[1].data = md_assess.map(item => item.cp_ratio);
    barChart04.data.datasets[2].data = md_assess.map(item => item.mkv_call);
    barChart04.data.datasets[3].data = md_assess.map(item => item.mkv_put);

    chart0301.data.labels = und_act_vol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        chart0301.data.datasets[index].data = und_act_vol.map(item => item[key]);
    });

    chart0302.data.labels = und_swap_vol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        chart0302.data.datasets[index].data = und_swap_vol.map(item => item[key]);
    });

    chart0401.data.labels = opt_act_vol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        chart0401.data.datasets[index].data = opt_act_vol.map(item => item[key]);
    });

    chart0402.data.labels = opt_swap_vol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        chart0402.data.datasets[index].data = opt_swap_vol.map(item => item[key]);
    });

    ChartIVRange.update();
    fillArea(ChartIVRange, 0, 1, "#f1bbba", 0.3);
    fillArea(ChartIVRange, 2, 3, "#aacfd0", 0.3);

    lineChart001.update();
    lineChart002.update();
    lineChart01.update();
    barChart01.update();
    barChart02.update();
    barChart03.update();
    lineChart02.update();
    barChart04.update();
    chart0301.update();
    chart0302.update();
    chart0401.update();
    chart0402.update();
    barChartHFO.update();

    
    if (opt_price_test.length !== 0) {
        chartOptPriceTest.data.labels = opt_price_test.map(item => item.spot_price);
        chartOptPriceTest.data.datasets[0].data = opt_price_test.map(item => item.opt_price);
        chartOptPriceTest.update();
    }
    
}

updateValidPosTable();
updateVipPosTable();
updateOptionVipPosTable();


function task() {
    updateLocalCharts();
    updateTitle();
}

if (drawLocalCharts()) {
    /// 图片已经初始化，可以开启定时更新;
    setInterval(task, 30000);
}