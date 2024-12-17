var brokerKeyContainer = document.getElementById('keyContainer');
var brokers = brokerKeyContainer.getAttribute('data-key');


function bkrNegPosBar(value, multiplier) {
    const disp = document.createElement('div');
    disp.classList.add('size-bar');
    disp.style.backgroundColor = value > 0 ? '#D81159' : '#379392';
    disp.style.width = `${Math.min(Math.abs(value) * multiplier, 50)}px`;
    disp.style.height = `2px`;
    return disp;
}


function brkFormatter(header, original_val_, val_, cell) {
    if (header == 'net_pos_corr') {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        cell.innerHTML = `<span tag=${original_val_} style="font-size: 12px; color: ${color_};">${val_}</span>`;
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
        cell.innerHTML = `<span tag=${original_val_} style="font-size: 12px; color: ${color_};">${val_}</span>`;
    } else if (header.includes('net_chg')) {
        const color_ = original_val_ > 0 ? '#D81159' : '#379392';
        cell.innerHTML = `<span tag=${original_val_} style="font-size: 12px; color: ${color_};">${val_}</span>`;
    }
}

async function getBrokerPositionData() {
    const response = await fetch('/selected_vip_position/' + brokers);
    const data = await response.json();

    return data;
}

function bkrUpdateVipPosTable(data) {
    const table = document.getElementById('vipPos');

    table.innerHTML = '';

    if (!data || data.length == 0) {
        return;
    }

    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const thead = table.createTHead(); // Create tHead if not exists
    const headerRow = thead.insertRow();
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

    const tbody = table.createTBody();

    data.forEach(item => {
        const row = tbody.insertRow();
        headers.forEach(header => {
            const cell = row.insertCell();
            if (header == 'symbol') {
                if (prev_sym != item[header]) {
                    cell.textContent = item[header];
                    prev_sym = item[header];
                }
            } else if (header == 'contract') {
                cell.textContent = item[header];
                prev_con = item[header];
            } else {
                var original_val_ = item[header];
                // if val_ is a number, then format it as XXX,XXX,XXX.XX else leave it as it is            
                if ((!isNaN(original_val_)) && (original_val_ != null)) {
                    var val_ = original_val_.toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");;
                } else {
                    var val_ = original_val_;
                }
                cell.textContent = val_;
                
                brkFormatter(header, original_val_, val_, cell);
            }
        });
    });
}

function getColor(keyParam) {
    const colors = [
        '#800000', '#8B0000', '#A52A2A', '#B22222', '#DC143C', '#FF0000', '#FF4500', '#FF6347', '#FF7F50', '#FF8C00', '#FFA500', '#FFD700'
    ];
    return colors[keyParam]
}

function formDatasets(dat, xLabel) {
    const datasets = [];
    const keys = Object.keys(dat[0]);
    VolKeys = keys.filter(key => key !== xLabel);
    VolKeys.forEach((key, index) => {
        datasets.push({
            label: key.toUpperCase(),
            data: dat.map(item => item[key]),
            borderColor: getColor(parseInt(key.slice(-2)) - 1),
            yAxisID: 'y',
            pointRadius: 0,
            showLine: true
        });
    });
    return datasets;
}


function drawPosHistLines(canvas, data) {
    const posHistChart = new Chart(canvas, {
        type: 'line',
        data: {
            labels: data.map(item => item.trading_date),
            datasets: formDatasets(data, 'trading_date'),
        },
        options: {
            interaction: {
                mode: 'nearest',
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
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Position Process',
                    font: {
                        size: 16
                    }
                }
            }
        }
    });
}

function holdingSymbols(data) {
    var symbolPosDict = data.reduce(function(result, row) {
        var x = row.symbol;
        var y = row.net_pos;
        
        if (!result[x]) {
            result[x] = 0;
        }
        
        result[x] += y;
        
        return result;
    }, {});

    var symbolChgDict = data.reduce(function(result, row) {
        var x = row.symbol;
        var y = row.net_chg;
        
        if (!result[x]) {
            result[x] = 0;
        }
        
        result[x] += y;
        
        return result;
    }, {});

    var sortedSymbolPos = Object.entries(symbolPosDict)
    .sort(function(a, b) {
        return b[1] - a[1];
    })
    .map(function(entry) {
        return entry[0];
    });

    const canvas = document.getElementById("symPosCanvas");

    const symPosChart = new Chart(canvas, {
        type: 'bar',
        data: {
        labels: sortedSymbolPos,
        datasets: [
            {
                label: 'total hld',
                data: sortedSymbolPos.map(s => symbolPosDict[s]),
                backgroundColor: sortedSymbolPos.map(s => symbolPosDict[s] > 0 ? '#fe4365' : '#3b8686'),
            },
            {
                label: 'total chg',
                data: sortedSymbolPos.map(s => symbolChgDict[s] || 0),
                backgroundColor: '#E3E36A',
            }
        ]
        },
        options: {
            interaction: {
                mode: 'index',
                intersect: false
            },
            responsive: true,
            aspectRatio: 6,
            onClick: function(event, elements) {
                if (elements.length > 0) {
                var clickedSymbol = sortedSymbolPos[elements[0].index];
                
                // 发送POST请求
                fetch("/VIP_position_hist", {
                    method: "POST",
                    body: JSON.stringify({ symbol: clickedSymbol, broker: brokers }),
                    headers: {
                        "Content-Type": "application/json"
                    }
                })
                .then(function(response) {
                    return response.json();
                }).then(function(data) {
                    const lineContainer = document.getElementById('posHistContainer');
                    const oldLineCanvas = document.getElementById('posHistCanvas');
                    if (oldLineCanvas) {
                        lineContainer.removeChild(oldLineCanvas);
                    }
                    const newLineCanvas = document.createElement('canvas');
                    newLineCanvas.id = 'posHistCanvas';
                    lineContainer.appendChild(newLineCanvas);
                    drawPosHistLines(newLineCanvas, data);
                })
                }
            }
        }
    });
}


async function displayPage() {
    const posData = await getBrokerPositionData();
    bkrUpdateVipPosTable(posData);
    holdingSymbols(posData);
    return true;
}


displayPage().then(function() {
    var vipPosTable = document.getElementById('vipPos');
    var vipPosRows = Array.from(vipPosTable.getElementsByTagName('tr'));
    var originalHeader = vipPosTable.tHead.innerHTML; // 保存原始表头内容
    
    // 监听table父元素的点击事件
    vipPosTable.parentNode.addEventListener('click', function(e) {
        if (e.target.tagName === 'TH' && e.target.closest('table') === vipPosTable && e.target.textContent.includes('NET')) {
            var columnIndex = Array.from(e.target.parentNode.children).indexOf(e.target); // 获取点击的表头的列索引
            var isAscending = false; // 默认降序排序
            var sortedRows = vipPosRows.slice(1).sort(function(rowA, rowB) {
                var cellA = parseFloat(rowA.getElementsByTagName('td')[columnIndex].querySelector('span').getAttribute('tag'));
                var cellB = parseFloat(rowB.getElementsByTagName('td')[columnIndex].querySelector('span').getAttribute('tag'));
                cellA = isNaN(cellA) ? Number.NEGATIVE_INFINITY : cellA;
                cellB = isNaN(cellB) ? Number.NEGATIVE_INFINITY : cellB;
                return isAscending ? cellA - cellB : cellB - cellA;
            });
            // 清空table中的内容
            while (vipPosTable.tBodies[0].firstChild) {
                vipPosTable.tBodies[0].removeChild(vipPosTable.tBodies[0].firstChild);
            }
            // 重新添加原始表头
            vipPosTable.tHead.innerHTML = originalHeader;
            // 将排序后的行重新添加到table中
            sortedRows.forEach(function(row) {
                vipPosTable.tBodies[0].appendChild(row);
            });
        }
    });
});