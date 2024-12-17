var undKeyContainer = document.getElementById('keyContainer');
var symbolParam = undKeyContainer.getAttribute('data-key');

/// 全局定义几个chart，用以更新dataset
var undChart1;
var undChart2;
var undChart3;
var undChart4;
var undChart5;
var undChart6;
var undChart7;
var undChart8;
var barChartHFO;

function UndNegPosBar(value, multiplier) {
    const disp = document.createElement('div');
    disp.classList.add('size-bar');
    disp.style.backgroundColor = value > 0 ? '#D81159' : '#379392';
    disp.style.width = `${Math.min(Math.abs(value) * multiplier, 50)}px`;
    disp.style.height = `2px`;
    return disp;
}


function undFormatter(header, original_val_, val_, cell) {
    if (header == 'net_pos_corr') {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        const bar = UndNegPosBar(original_val_, 50);
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
        const bar = UndNegPosBar(adjVal, 50);
        cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
        cell.appendChild(bar);
    } else if (header.includes('net_chg')) {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        const adjVal = original_val_ / 5000;
        const bar = UndNegPosBar(adjVal, 50);
        cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
        cell.appendChild(bar);
    }
}

async function undUpdateValidPosTable() {
    const response = await fetch('/valid_pos_by_contract/' + symbolParam);
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
            
            undFormatter(header, original_val_, val_, cell);
        });
    });
}

async function undUpdateVipPosTable() {
    const response = await fetch('/vip_position_sym/' + symbolParam);
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
                
                undFormatter(header, original_val_, val_, cell);
            }
        });
    });
}

async function undUpdateOptionVipPosTable() {
    const response = await fetch('/vip_option_position_und/' + symbolParam);
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
            
            undFormatter(header, original_val_, val_, cell);
        });
    });
}


async function undRequestData() {
    const response = await fetch('/stakes_sym/' + symbolParam);
    const data = await response.json();
    const contract = data.contract;
    const price_data = JSON.parse(data.price_data);
    const stake_data = JSON.parse(data.stake_data);
    const md_assess = JSON.parse(data.md_assess);
    const hist_daily_md = JSON.parse(data.hist_daily_md);
    const actVol = JSON.parse(data.act_vol);
    const swapVol = JSON.parse(data.swap_vol);
    const hist_fo_data = JSON.parse(data.hist_fo_data);
    return [contract, price_data, stake_data, md_assess, hist_daily_md, actVol, swapVol, hist_fo_data];
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

async function undDrawLocalCharts() {
    const [contract, ts_data, stake_data, md_assess, hist_daily_md, actVol, swapVol, hist_fo_data] = await undRequestData();
    
    var histMdUndCloseMin = Math.min(...hist_daily_md.filter(item => item.und_close !== null).map(item => item.und_close));
    var histMdUndCloseMax = Math.max(...hist_daily_md.filter(item => item.und_close !== null).map(item => item.und_close));
    var currMdUndCloseMin = Math.min(...ts_data.filter(item => item.last !== null).map(item => item.last));
    var currMdUndCloseMax = Math.max(...ts_data.filter(item => item.last !== null).map(item => item.last));

    const canvas1Left = document.getElementById('Canvas1Left');
    const canvas1Right = document.getElementById('Canvas1Right');
    
    const canvasLeft = document.getElementById('chartCanvas-left');
    const canvasRight1 = document.getElementById('chartCanvas-right1');
    const canvasRight2 = document.getElementById('chartCanvas-right2');
    const canvasRight3 = document.getElementById('chartCanvas-right3');

    const canvas3Left = document.getElementById('Canvas3Left');
    const canvas3Right = document.getElementById('Canvas3Right');

    const canvasHFO = document.getElementById('histFoCanvas');

    undChart1 = new Chart(canvas1Left, {
        type: 'bar',
        data: {
        labels: md_assess.map(item => item.datetime_minute),
        datasets: [
            {
                label: 'rSk Value',
                data: md_assess.map(item => item.sk),
                backgroundColor: '#218380',
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
                }
            }
        }
    });

    undChart2 = new Chart(canvas1Right, {
        type: 'line',
        data: {
        labels: hist_daily_md.map(item => item.trading_date),
        datasets: [
            {
                label: contract,
                data: hist_daily_md.map(item => item.und_close),
                borderColor: '#80d4f6',
                pointRadius: 0, // 不显示数据点
                showLine: true, // 显示线条
            },
            {
                type: 'bar',
                label: 'RM LONG',
                data: hist_daily_md.map(item => item.rm_long),
                backgroundColor: '#ee2560',
                yAxisID: 'rm',
            },
            {
                type: 'bar',
                label: 'RM SHORT',
                data: hist_daily_md.map(item => item.rm_short),
                backgroundColor: '#4f953b',
                yAxisID: 'rm',
            },
            {
                type: 'bar',
                label: 'VP MAIN',
                data: hist_daily_md.map(item => item.vp_main),
                backgroundColor: '#F6B352',
                yAxisID: 'vp',
            },
            {
                type: 'bar',
                label: 'VP SYMBOL',
                data: hist_daily_md.map(item => item.vp_symbol),
                backgroundColor: '#F68657',
                yAxisID: 'vp',
            },
            {
                label: "SPOT",
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
            height: undChart1.height,
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
            height: undChart1.height,
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

    undChart3 = new Chart(canvasLeft, {
        type: 'line',
        data: {
        labels: ts_data.map(item => item.datetime_minute),
        datasets: [
            {
                label: contract,
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
    undChart4 = new Chart(canvasRight1, {
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
            height: undChart3.height,
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

    undChart5 = new Chart(canvasRight2, {
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
            height: undChart3.height,
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

    undChart6 = new Chart(canvasRight3, {
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
            height: undChart3.height,
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

    undChart7 = new Chart(canvas3Left, {
        type: 'line',
        data: {
            labels: actVol.map(item => item.datetime_minute),
            datasets: dataset_formation(actVol, 'datetime_minute')
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
                    text: 'Act',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    undChart8 = new Chart(canvas3Right, {
        type: 'line',
        data: {
            labels: swapVol.map(item => item.datetime_minute),
            datasets: dataset_formation(swapVol, 'datetime_minute')
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            maintainAspectRatio: false,
            height: undChart7.height,
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
                    text: 'Swap',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });

    return true;
}

async function undUpdateLocalCharts() {
    const [contract, ts_data, stake_data, md_assess, hist_daily_md, actVol, swapVol, hist_fo_data] = await undRequestData();

    undChart1.data.labels = md_assess.map(item => item.datetime_minute);
    undChart1.data.datasets[0].data = md_assess.map(item => item.sk);

    undChart2.data.labels = hist_daily_md.map(item => item.trading_date);
    undChart2.data.datasets[0].data = hist_daily_md.map(item => item.und_close);
    undChart2.data.datasets[1].data = hist_daily_md.map(item => item.rm_long);
    undChart2.data.datasets[2].data = hist_daily_md.map(item => item.rm_short);
    undChart2.data.datasets[3].data = hist_daily_md.map(item => item.vp_main);
    undChart2.data.datasets[4].data = hist_daily_md.map(item => item.vp_symbol);
    undChart2.data.datasets[5].data = hist_daily_md.map(item => item.spot);

    barChartHFO.data.label = hist_fo_data.map(item => item.price);
    barChartHFO.data.datasets[0].data = hist_fo_data.map(item => item.stake);

    undChart3.data.labels = ts_data.map(item => item.datetime_minute);
    undChart3.data.datasets[0].data = ts_data.map(item => item.last);
    undChart3.data.datasets[1].data = ts_data.map(item => item.open_interest);
    undChart3.data.datasets[2].data = ts_data.map(item => item.rm_long);
    undChart3.data.datasets[3].data = ts_data.map(item => item.rm_short);
    undChart3.data.datasets[4].data = ts_data.map(item => item.rm_delta);

    undChart4.data.labels = stake_data.map(item => item.last);
    undChart4.data.datasets[0].data = stake_data.map(item => item.volume_stake);

    undChart5.data.labels = stake_data.map(item => item.last);
    undChart5.data.datasets[0].data = stake_data.map(item => item.oi_stake);

    undChart6.data.labels = stake_data.map(item => item.last);
    undChart6.data.datasets[0].data = stake_data.map(item => item.filtered_order_stake);
    undChart6.data.datasets[1].data = stake_data.map(item => item.abs_big_order_stake);

    undChart7.data.labels = actVol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        undChart7.data.datasets[index].data = actVol.map(item => item[key]);
    });

    undChart8.data.labels = swapVol.map(item => item.datetime_minute);
    VolKeys.forEach((key, index) => {
        undChart8.data.datasets[index].data = swapVol.map(item => item[key]);
    });

    undChart1.update();
    undChart2.update();
    undChart3.update();
    undChart4.update();
    undChart5.update();
    undChart6.update();
    undChart7.update();
    undChart8.update();
    barChartHFO.update();
}

undUpdateValidPosTable();
undUpdateVipPosTable();
undUpdateOptionVipPosTable();

if (undDrawLocalCharts()) {
    /// 图片已经初始化，可以开启定时更新;
    setInterval(undUpdateLocalCharts, 30000);
}