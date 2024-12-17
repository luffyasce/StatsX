page_root = document.domain;

const websocket = new WebSocket('ws://' + `${page_root}:15169`);

websocket.onopen = function() {
    heartBeat();
};

function heartBeat() {
    websocket.send(1);
}

setInterval(heartBeat, 15000);

function drawCharts(data) {
    const liveChartsContainer = document.getElementById('liveChartsContainer');
    liveChartsContainer.innerHTML = ''; // 清空容器
    Object.entries(data).forEach(([key, mdData]) => {
        mdData = JSON.parse(mdData);

        const vPctg = 0.05;     // 默认坐标尺度为5%涨幅
        const yValues = mdData.map(item => item.last);
        const vMax = Math.max(...yValues);      // 目标范围内最高价
        const vMin = Math.min(...yValues);      // 目标范围内最低价
        const vRange = Math.min(...yValues) * (1 + vPctg);      // 默认图表最高价
        const actYMax = Math.max(vMax, vRange);     // 实际图表最高价
        const currentP = yValues.at(-1);
        const realizedRange = ((vMax / vMin) - 1) * 100;
        const currentRange = ((currentP / vMin) - 1) * 100;
        const yTitle = "@" + currentRange.toFixed(2).toString() + "%" + "(" + realizedRange.toFixed(2).toString() + "%)";

        var chartDiv = document.createElement('div');
        chartDiv.style.width = '33%'; // 每行两个图表
        chartDiv.style.display = 'inline-block'; // 并排显示
        chartDiv.style.verticalAlign = 'top'; // 顶部对齐
        
        const canvas = document.createElement('canvas');
        canvas.id = `chart-${key}`; // 给每个canvas一个唯一的ID

        liveChartsContainer.appendChild(chartDiv);
        // 初始化图表
        const ctx = canvas.getContext('2d');
        var chart = new Chart(ctx, {
            type: 'line', // 图表类型
            data: {
                labels: mdData.map(item => item.datetime_minute),
                datasets: [
                    {
                        label: 'price',
                        data: mdData.map(item => item.last),
                        borderColor: '#f4f7f7',
                        pointRadius: 0, // 不显示数据点
                        showLine: true, // 显示线条
                    },
                    {
                        label: 'open interest',
                        data: mdData.map(item => item.open_interest),
                        borderColor: '#FFBC42',
                        pointRadius: 0, // 不显示数据点
                        showLine: true, // 显示线条
                        yAxisID: 'oi'
                    },
                    {
                        type: 'bar',
                        label: 'volume',
                        data: mdData.map(item => item.volume),
                        backgroundColor: 'pink',
                        yAxisID: 'vol',
                    },
                ]
            },
            options: {
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                responsive: true,
                animation: false,
                aspectRatio: 1.5,
                scales: {
                    x: {
                        grid: {
                            display: true,
                            color: '#CCCCCC'
                        },
                    },
                    y: {
                        grid: {
                            display: true,
                            color: '#CCCCCC'
                        },
                        ticks: {
                            color: '#CCCCCC'
                        },
                        max: actYMax
                    },
                    oi: {
                        display: false,
                        position: 'right', // 可能需要根据实际情况调整位置
                        grid: {
                            display: false
                        }
                    },
                    vol: {
                        display: false,
                        position: 'right', // 可能需要根据实际情况调整位置
                        grid: {
                            display: false
                        }
                    }
                },
                plugins: {
                    title: {
                        display: true,
                        text: key + " " + yTitle, // 设置标题文本
                        padding: {
                            top: 10,
                            bottom: 30
                        },
                        font: {
                            size: 18 // 标题字体大小
                        }
                    }
                }
            }
        });
        chartDiv.appendChild(canvas);

    });
}


websocket.onmessage = function(event) {
    // 处理从后端接收的数据
    const data = JSON.parse(event.data);
    drawCharts(data);
};

websocket.onclose = function() {
    console.log('WebSocket 连接关闭');
};