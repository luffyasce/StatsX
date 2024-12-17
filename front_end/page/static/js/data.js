const apex_root = document.domain;

const root_url = 'http://' + `${apex_root}:15167`;
const chart_url = 'http://' + `${apex_root}:15168`;


/// 更新快慢时间参数
const updateFastTimeInterval = 2 * 1000;
const updateMidTimeInterval = 8 * 1000;
const updateSlowTimeInterval = 16 * 1000;

let newTargetAudio = document.getElementById("newTargetAudio");


let optionSymbols;
let OiIncSymbols;

async function fetchOiIncSymbols() {
  while (OiIncSymbols == null) {
    const response = await fetch(root_url + '/oi_increasing_symbols');
    const data = await response.json();
    OiIncSymbols = data;
  }
}


async function fetchOptionSymbols() {
  while (optionSymbols == null) {
    const response = await fetch(root_url + '/option_symbols');
    const data = await response.json();
    optionSymbols = data;
  }
}

window.addEventListener('load', function() {
  /// 载入时绑定的统一函数入口
  quest = sessionStorage.getItem('questString') || "";
  get_earliest_expiry();
  fetchOptionSymbols();
  fetchOiIncSymbols();
  showAllSymbolStatus();
});


function clearTable(table) {
  // 使用firstChild和removeChild来逐个删除行
  while (table.firstChild) {
    table.removeChild(table.firstChild);
  }
}


const positiveDiv = document.getElementById('longTargets');
const negativeDiv = document.getElementById('shortTargets');
const nilDiv = document.getElementById('nilTargets');

const positiveContainer = document.createElement('div');
positiveContainer.style.display = 'flex';
const negativeContainer = document.createElement('div');
negativeContainer.style.display = 'flex';
const nilContainer = document.createElement('div');
nilContainer.style.display = 'flex';


let optionOnly = document.getElementById('optionOnly').checked ? true : false;

let signalTargetsOnly = document.getElementById('signalTargetsOnly').checked ? true : false;

document.getElementById('optionOnly').addEventListener('change', function() {
  optionOnly = this.checked ? true : false;
  changeSymbolStatusVisibility(optionOnly, optionSymbols);
});

document.getElementById('signalTargetsOnly').addEventListener('change', function() {
  signalTargetsOnly = this.checked ? true : false;
});

let OiIncOnly = document.getElementById('OiIncOnly').checked ? true : false;

function updateOnIncOnlyAndChangeVis() {
  fetch(root_url + '/oi_increasing_symbols')
  .then(response => response.json())
  .then(data => {
    OiIncSymbols = data;
    if (OiIncOnly) {
      changeSymbolStatusVisibility(OiIncOnly, OiIncSymbols);
    }
  })
}

setInterval(updateOnIncOnlyAndChangeVis, updateSlowTimeInterval);

document.getElementById('OiIncOnly').addEventListener('change', function() {
  OiIncOnly = this.checked ? true : false;
  changeSymbolStatusVisibility(OiIncOnly, OiIncSymbols);
});



let brokerTargetList = [];
let oiTargetList = [];
let mergedTargetList = [];

function filterOnlyTargetQuestString() {
  mergedTargetList = brokerTargetList.filter(x => oiTargetList.includes(x));
  mergedTargetList.forEach(t => {
    if (!quest.includes(t)) {
      quest += " " + t;
    }
  });
  sessionStorage.setItem('questString', quest);
}

setInterval(filterOnlyTargetQuestString, updateFastTimeInterval);

function addEveListenerDiv(divElement, eventHandler) {
  divElement.addEventListener('contextmenu', eventHandler);
}

let questTableTargetSelectedUnderlying = [];

let prevTargets;
let recentDirectDaysCnt;
let optMkdDirection;
// let directionIndicator;
let increasingOiContractTargets = [];
let increasingOiSymbolTargets = [];

let prevRmMarks = {};

fetch(root_url + '/prev_targets')
.then(response => response.json())
.then(data => {
  prevTargets = data;
})

fetch(root_url + '/recent_direction_days_cnt')
.then(response => response.json())
.then(data => {
  recentDirectDaysCnt = data;
})

fetch(root_url + '/opt_mkd_direction')
.then(response => response.json())
.then(data => {
  optMkdDirection = data;
})

// fetch(root_url + '/direction_indicator')
// .then(response => response.json())
// .then(data => {
//   directionIndicator = data;
// })

function getIncreasingOiTargets() {
  fetch(root_url + '/oi_targets')
  .then(response => response.json())
  .then(data => {
    increasingOiContractTargets = data.contracts;
    increasingOiSymbolTargets = data.symbols;
  })
}

setInterval(getIncreasingOiTargets, updateFastTimeInterval);

let longTargetList = [];
let shortTargetList = [];
let nilTargetList = [];


function showAllSymbolStatus() {
  fetch(root_url + '/targets')
  .then(response => response.json())
  .then(data => {
    Object.entries(data).forEach(([key, valueDict]) => {
      const directionConsistDays = recentDirectDaysCnt[key] || 0;
      const link = document.createElement('a');
      link.href = '/image/' + key;
      link.target = '_blank';
      link.textContent = key;

      const consistencyVal = valueDict['consistency'];
      const rmDeltaVal = valueDict['rm_delta'];

      const rmMarking = rmDeltaVal > 0 ? '↑' : rmDeltaVal < 0 ? '↓' : '';
      const rmColoring = rmDeltaVal > 0 ? '#FF4081' : rmDeltaVal < 0 ? '#B2FF59' : '#fff1b9';

      const rmText = `<span style="font-size: 13px; color: ${rmColoring};"> ${rmMarking}</span>`;
      const rmElement = document.createElement('div');
      rmElement.innerHTML = rmText;

      const prevBlocks = document.createElement('div');
      prevBlocks.className = 'prevBlocks';
      const prevValue = prevTargets[key] || 0;
      const optMkdValue = optMkdDirection[key] || 0;
      // const directIndicator = directionIndicator[key] || 0;
      for (let j = 0; j < Math.abs(prevValue); j++) {
        const prevBlock = document.createElement('div');
        prevBlock.className = 'prevBlock';
        prevBlock.style.backgroundColor = prevValue > 0 ? '#ff7473' : '#a5d296';
        prevBlocks.appendChild(prevBlock);
      }

      const gapBetweenTargets = document.createElement('div');
      gapBetweenTargets.innerHTML = "&nbsp;&nbsp;&nbsp;";

      const directionConsistDaysSup = Array.from(String(directionConsistDays), digit => {
        const span = document.createElement('span');
        span.style.verticalAlign = 'super';
        span.style.fontSize = '0.6em'; // adjust as needed
        span.style.color = '#FFEEE4'; // adjust as needed
        span.textContent = digit;

        return span.outerHTML;
      }).join('');

      const direcMarkSub = document.createElement('span');
      // direcMarkSub.textContent = ((consistencyVal === 0) && (optMkdValue !== 0)) ? '?' : '';     // for consistent_res_s = consistent_res_und.apply(np.sign) + consistent_res_opt
      
      // for consistent_res_s = consistent_res_und
      direcMarkSub.textContent = consistencyVal != 0 ? (Math.abs(consistencyVal) > 1 ? '大' : '小') : "";     
      direcMarkSub.style.verticalAlign = 'sub'; 
      direcMarkSub.style.fontSize = '0.6em'; // adjust as needed
      direcMarkSub.style.color = '#FFFFF3'; // adjust as needed

      const markersElement = document.createElement('div');
      const markersContainer = document.createElement('span');
      markersContainer.style.display = 'inline';
      markersContainer.style.verticalAlign = 'top';
      markersContainer.innerHTML = directionConsistDaysSup + direcMarkSub.outerHTML;
      markersElement.appendChild(markersContainer);

      const blocks = document.createElement('div');
      blocks.className = 'blocks';
      for (let i = 0; i < Math.abs(consistencyVal); i++) {
        const block = document.createElement('div');
        block.className = 'block';
        block.style.backgroundColor = consistencyVal > 0 ? '#FF4081' : '#B2FF59';
        blocks.appendChild(block);
      }
      
      if (optionSymbols.includes(key)) {
        link.style.backgroundColor = "#52616a";
      }

      if (consistencyVal > 0) {
        link.style.color = (prevValue * consistencyVal) > 0 ? '#FF4081': '#f9c00c';
        positiveContainer.appendChild(prevBlocks);
        positiveContainer.appendChild(blocks);
        positiveContainer.appendChild(link);
        positiveContainer.appendChild(rmElement);
        positiveContainer.appendChild(markersElement);
        positiveContainer.appendChild(gapBetweenTargets);
        prevRmMarks[key] = {'loc': 'pos', 'val': rmDeltaVal};
        link.id = key;
        prevBlocks.id = key;
        blocks.id = key;
        rmElement.id = key;
        markersElement.id = key;
        gapBetweenTargets.id = key;
        longTargetList.push(key);
      } else if (consistencyVal < 0) {
        link.style.color = (prevValue * consistencyVal) > 0 ? '#B2FF59': '#f9c00c';
        negativeContainer.appendChild(prevBlocks);
        negativeContainer.appendChild(blocks);
        negativeContainer.appendChild(link);
        negativeContainer.appendChild(rmElement);
        negativeContainer.appendChild(markersElement);
        negativeContainer.appendChild(gapBetweenTargets);
        prevRmMarks[key] = {'loc': 'neg', 'val': rmDeltaVal};
        link.id = key;
        prevBlocks.id = key;
        blocks.id = key;
        rmElement.id = key;
        markersElement.id = key;
        gapBetweenTargets.id = key;
        shortTargetList.push(key);
      } else {
        link.style.color = optMkdValue === 0 ?'#fff1b9' : optMkdValue > 0 ? '#FF4081' : '#B2FF59';
        nilContainer.appendChild(prevBlocks);
        nilContainer.appendChild(blocks);
        nilContainer.appendChild(link);
        nilContainer.appendChild(rmElement);
        nilContainer.appendChild(markersElement);
        nilContainer.appendChild(gapBetweenTargets);
        prevRmMarks[key] = {'loc': 'nil', 'val': rmDeltaVal};
        link.id = key;
        prevBlocks.id = key;
        blocks.id = key;
        rmElement.id = key;
        markersElement.id = key;
        gapBetweenTargets.id = key;
        nilTargetList.push(key);
      }
    });
  });

  positiveDiv.appendChild(positiveContainer);
  negativeDiv.appendChild(negativeContainer);
  nilDiv.appendChild(nilContainer);

  addEveListenerDiv(positiveDiv, handleTargetsContextMenu);
  addEveListenerDiv(negativeDiv, handleTargetsContextMenu);
  addEveListenerDiv(nilDiv, handleTargetsContextMenu);

}

function __hide_symbols__(container, disp, SymbolsX) {
  const children = container.children;
  for (let i = 0; i < children.length; i++) {
    if (!SymbolsX.includes(children[i].id)) {
      children[i].style.display = disp;
    }
  }
}

function changeSymbolStatusVisibility(onlyCheck, SymbolsX) {
  if ((onlyCheck) && (SymbolsX != null)) {
    __hide_symbols__(positiveContainer, 'none', SymbolsX);
    __hide_symbols__(negativeContainer, 'none', SymbolsX);
    __hide_symbols__(nilContainer, 'none', SymbolsX);
  } else {
    __hide_symbols__(positiveContainer, '', SymbolsX);
    __hide_symbols__(negativeContainer, '', SymbolsX);
    __hide_symbols__(nilContainer, '', SymbolsX);
  }
}

const messageDict = {};

function addMessage(loc_i, key, prevLS, currLS) {
  message = `${currentTimeString} Direction Change: ${loc_i}  ${key}  ${prevLS} => ${currLS}`;
  const msgDictKey = `${key}_${loc_i}`;
  messageDict[msgDictKey] = {'time': currentTimeString, 'msg': message};
}

const notification = document.getElementById('notification');

notification.addEventListener('click', (event) => {
  var symTag = notification.getAttribute("sym-tag");
  symTag = symTag.split('_')[0];
  if (notification.style.display != 'none') {
    var page_url = chart_url + '/localChartUnd/' + symTag;
    window.open(page_url, '_blank');
  }
});

const undQstForm = document.getElementById('undPageForm');

function openQuestedUndPage(event) {
  event.preventDefault();

  const val_ = document.getElementById('undContract').value;
  const splitVals = val_.split(' ');

  for (let i = 0; i < splitVals.length; i++) {
    const trimmed = splitVals[i].trim();
    if (trimmed != '') {
      var page_url = chart_url + '/localChartUnd/' + splitVals[i].toUpperCase();
      window.open(page_url, '_blank');
    }
  }
}

undQstForm.addEventListener('submit', openQuestedUndPage);

function showNotification() {
  const notificationText = document.getElementById('notification-text');
  let maxEntry = Object.entries(messageDict).reduce((prev, curr) => {
    return (prev[1].time > curr[1].time) ? prev : curr;
  });

  var currentSymbol = maxEntry[0];
  var currentInf = maxEntry[1];

  var currentDt = currentInf['time'];
  var currentMsg = currentInf['msg'];

  notification.setAttribute("msg-tag", currentDt);
  notification.setAttribute("sym-tag", currentSymbol);
  notificationText.innerText = currentMsg;
  notification.style.display = 'block';
  const loc = currentSymbol.split('_')[1];
  const colorBackground = loc.includes('Ex') ? "#fdc23e" : "#fffff5";
  notification.style.backgroundColor = colorBackground;
}

function closeNotification() {
  const symTag = notification.getAttribute("sym-tag");
  delete messageDict[symTag];
  notification.style.display = 'none';
}

function driveNotificationCenter() {
  if (Object.keys(messageDict).length === 0) {
    return;
  }
  showNotification();
}

setInterval(driveNotificationCenter, updateFastTimeInterval);

function updateTargetSymbolRmMarks() {
  fetch(root_url + '/targets')
    .then(response => response.json())
    .then(data => {
      if ((optionOnly) && (optionSymbols != null)) {
        data = Object.fromEntries(
          Object.entries(data).filter(([key, _]) => optionSymbols.includes(key))
        );
      }
      if ((OiIncOnly) && (OiIncSymbols != null)) {
        data = Object.fromEntries(
          Object.entries(data).filter(([key, _]) => OiIncSymbols.includes(key))
        );
      }
      Object.entries(data).forEach(([key, valueDict]) => {
        const rmDeltaVal = valueDict['rm_delta'];
        const prevRmDeltaVal = prevRmMarks[key]['val'];
        const loc_i = prevRmMarks[key]['loc'];

        if (rmDeltaVal !== prevRmDeltaVal) {

          const prevLS = prevRmDeltaVal > 0 ? '↑' : prevRmDeltaVal < 0 ? '↓' : '-';
          const currLS = rmDeltaVal > 0 ? '↑' : rmDeltaVal < 0 ? '↓' : '-';

          addMessage(loc_i, key, prevLS, currLS);
          // The rmDeltaVal has changed, update the rmElement and prevRmMarks
          const rmMarking = rmDeltaVal > 0 ? '↑' : rmDeltaVal < 0 ? '↓' : '';
          const rmColoring = rmDeltaVal > 0 ? '#FF4081' : rmDeltaVal < 0 ? '#B2FF59' : '#fff1b9';
          const rmText = `<span style="font-size: 13px; color: ${rmColoring};"> ${rmMarking}&nbsp;&nbsp;&nbsp;</span>`;
          const rmElement = document.createElement('div');
          rmElement.innerHTML = rmText;

          // Update the DOM with the new rmElement
          const containerToReplace = loc_i === 'pos' ? positiveContainer : loc_i === 'neg' ? negativeContainer : nilContainer;

          // Replace the old rmElement with the new one
          containerToReplace.querySelectorAll('a').forEach(link => {
            if (link.textContent === key) {
              rmElement.style.opacity = 0;
              const previousRmElement = link.nextSibling; 
              if (previousRmElement) {
                link.parentElement.replaceChild(rmElement, previousRmElement);

                setTimeout(() => {
                  rmElement.style.transition = 'opacity 0.5s';
                  rmElement.style.opacity = '1';
                }, 10);

                
              }
            }
          });

          prevRmMarks[key] = { 'loc': loc_i, 'val': rmDeltaVal };
        }
      });
    });
}

setInterval(updateTargetSymbolRmMarks, updateSlowTimeInterval)

var currentSotInterID = null;

function handleTargetsContextMenu(event) {
  event.preventDefault();
  const lk_ = event.target.closest('a');
  if (lk_) {
    let symClick = lk_.textContent; 
    if (symClick == null) {
      return;
    } else if (optionSymbols.includes(symClick)) {
      if (currentSotInterID != null) {
        clearInterval(currentSotInterID);
      }
      currentSotInterID = setInterval(updateSymbolOptionsTable, updateFastTimeInterval, symClick);
    } else {
      openSymbolFuturePage(symClick);
    }
  }
}

const SOTTable = document.getElementById('symbol-opt-table');
const closeSOTButton = document.getElementById('closeSOT');

function openSymbolFuturePage(lastSymClick) {
  var page_url = chart_url + '/localChartUnd/' + lastSymClick;
  window.open(page_url, '_blank');
}

function updateSymbolOptionsTable(lastSymClick) {
  fetch(root_url + '/sym_targets/' + lastSymClick)
  .then(response => response.json())
  .then(data => {
    updateTable(data, 'symbol-opt-table');
  });
  closeSOTButton.textContent = lastSymClick;
}

closeSOTButton.onclick = () => {
  clearInterval(currentSotInterID);
  closeSOTButton.textContent = 'X';
  clearTable(SOTTable);
  setTimeout(() => {
    clearTable(SOTTable);
  }, updateFastTimeInterval * 2);
  
}

const selectBrokerForm = document.getElementById('vip_broker_form');

function popSelectedBroker(event) {
  event.preventDefault();
  var val_ = document.getElementById('brokers').value;
  var page_url = chart_url + '/vipPosition/' + val_;
  window.open(page_url, '_blank');
}

selectBrokerForm.addEventListener('submit', popSelectedBroker);

var futMktCapValue = 0;

const mktCapForm = document.getElementById('mktCap-filter');

function getMktCapValue(event) {
  event.preventDefault();
  var val_ = document.getElementById('mktCapValue').value;
  if (!(val_ === "" || isNaN(val_))) {
    futMktCapValue = val_ * 1e6;
  }
}

mktCapForm.addEventListener('submit', getMktCapValue);

let fitSymbol = [];


function updateHighlightTargets(data) {
  fitSymbol = [];
  data.filter(item => item.future_chg > futMktCapValue).forEach(
    item => {
      fitSymbol.push(item.symbol)
    }
  );
}

function markHighLightSymbols() {
  [positiveContainer, negativeContainer, nilContainer].forEach(container => {
    for (let i = 0; i < container.children.length; i++) {
      const element = container.children[i];
      if (element.tagName === 'A') {
        const symbol = element.textContent;
        var outlineThickness = 0;
        outlineThickness += fitSymbol.includes(symbol) ? 2 : 0;
        outlineThickness += increasingOiSymbolTargets.includes(symbol) ? 2 : 0;
        const color = '#FFFFF3';
        element.style.outline = outlineThickness == 0 ? 'none' : `${outlineThickness}px solid ${color}`;
      }
    }
  })
}

var mktCapBarChart;
var priceDevChart;

var topSymbols = [];
var IncPosSymbols = [];
let indexTitle = document.getElementById('indexTitle');

function setTitle() {
  if (topSymbols.length == 0) {
    return;
  }
  let symbolStr = topSymbols.join('-');
  indexTitle.innerHTML = symbolStr;
  fetch((root_url + '/save_top_symbols'), {
    method: 'POST', 
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      'topSymbols': symbolStr
    }), 
  })
}

setInterval(setTitle, updateFastTimeInterval);

function openTopSymbolsMainContractTrackingPage() {
  var page_url = chart_url + '/topSymbols';
  window.open(page_url, '_blank');
}

async function requestMktCapChgData() {
  const response = await fetch(root_url + '/this_mkt_val_chg');
  var data = await response.json();
  if ((optionOnly) && (optionSymbols != null)) {
    data = data.filter(item => optionSymbols.includes(item.symbol));
  }
  if ((OiIncOnly) && (OiIncSymbols != null)) {
    data = data.filter(item => OiIncSymbols.includes(item.symbol));
  }
  if (signalTargetsOnly) {
    var signalTargetList = longTargetList.concat(shortTargetList);
    data = data.filter(item => signalTargetList.includes(item.symbol));
  }
  topSymbols = data.slice(0, 6).map(item => item.symbol);
  IncPosSymbols = data.filter(item => item.future_chg > 0).map(item => item.symbol);
  return data;
}

async function drawThisMktCapChart() {
  var data = await requestMktCapChgData();
  const mktCapCanvas = document.getElementById('mktCapCanvas');
  const pDevCanvas = document.getElementById('priceDeviationCanvas');
  
  const labels = data.map(item => item.symbol);

  priceDevChart = new Chart(pDevCanvas, {
    type: 'bar',
    data: {
    labels: data.map(item => item.symbol),
    datasets: [
        {
          label: 'price deviation%',
          data: data.map(item => item.price_deviate),
          backgroundColor: data.map(item => item.price_deviate > 0 ? 'red' : 'green'),
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
        scales: {
            x: {
                grid: {
                  display: true,
                  color: '#CCCCCC'
                },
                ticks: {
                  color: function(context) {
                    const index = context.index;
                    const label = labels[index];
                    if (longTargetList.includes(label)) {
                        return '#ef5285'; 
                    } else if (shortTargetList.includes(label)) {
                        return '#60c5ba';
                    } else if (nilTargetList.includes(label)) {
                        return '#CCCCCC';
                    } else {
                        return '#CCCCCC';
                    }
                  }
                }
            },
            y: {
                grid: {
                    display: true,
                    color: '#CCCCCC'
                },
                ticks: {
                  color: '#CCCCCC',
                  // callback: function (value, index, values) {
                  //   return value.toExponential();
                  // }
                }
              }
            }
          }
        }
      )
    

  mktCapBarChart = new Chart(mktCapCanvas, {
      type: 'bar',
      data: {
      labels: data.map(item => item.symbol),
      datasets: [
          {
            label: 'future cap chg',
            data: data.map(item => item.future_chg),
            backgroundColor: data.map(item => item.future_chg > 0 ? '#FFBC42' : '#FDD692'),
            yAxisID: 'futCapChg',
          },
          {
            label: 'future oi chg%',
            data: data.map(item => item.fut_oi_pctg),
            backgroundColor: '#F8FAFF',
            yAxisID: 'futOiChg',
            hidden: true
          },
          {
            type: 'line',
            label: 'avg future cap chg',
            data: data.map(item => item.average_future_chg),
            borderColor: '#FBFFB9',
            pointRadius: 0, // 不显示数据点
            showLine: true, // 显示线条
            borderWidth: 1,
            borderDash: [7, 3],
            yAxisID: 'futCapChg',
            hidden: true
          },
          {
            label: 'call cap chg',
            data: data.map(item => item.call_chg),
            backgroundColor: '#ef5285',
            yAxisID: 'opt',
            hidden: true
          },
          {
            label: 'put cap chg',
            data: data.map(item => item.put_chg),
            backgroundColor: '#60c5ba',
            yAxisID: 'opt',
            hidden: true
          },
          {
            label: 'cp factor',
            data: data.map(item => item.cp_factor),
            backgroundColor: data.map(item => item.cp_factor > 0 ? '#ef5285' : '#60c5ba'),
            yAxisID: 'cpf',
          },
          {
            type: 'line',
            label: 'call iv pos',
            data: data.map(item => item.call_avg),
            borderColor: '#F16B6F',
            pointRadius: 0, // 不显示数据点
            showLine: true, // 显示线条
            hidden: true
          },
          {
            type: 'line',
            label: 'put iv pos',
            data: data.map(item => item.put_avg),
            borderColor: '#8CD790',
            pointRadius: 0, // 不显示数据点
            showLine: true, // 显示线条
            hidden: true
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
          scales: {
              x: {
                  grid: {
                    display: true,
                    color: '#CCCCCC'
                  },
                  ticks: {
                    color: function(context) {
                      const index = context.index;
                      const label = labels[index];
                      if (longTargetList.includes(label)) {
                          return '#ef5285'; 
                      } else if (shortTargetList.includes(label)) {
                          return '#60c5ba';
                      } else if (nilTargetList.includes(label)) {
                          return '#CCCCCC';
                      } else {
                          return '#CCCCCC';
                      }
                    }
                  }
              },
              y: {
                  grid: {
                      display: true,
                      color: '#CCCCCC'
                  },
                  ticks: {
                    color: '#CCCCCC',
                    // callback: function (value, index, values) {
                    //   return value.toExponential();
                    // }
                  }
              },
              futOiChg: {
                display: false,
                position: 'right', // 可能需要根据实际情况调整位置
                grid: {
                    display: false
                },
                ticks: {
                  color: '#CCCCCC'
                }
                
              },
              opt: {
                display: false,
                position: 'right', // 可能需要根据实际情况调整位置
                grid: {
                    display: false
                },
                ticks: {
                  color: '#CCCCCC'
                }
              },
              cpf: {
                display: false,
                position: 'right', // 可能需要根据实际情况调整位置
                grid: {
                    display: false
                },
                ticks: {
                  color: '#CCCCCC'
                }
              },
              futCapChg: {
                display: false,
                position: 'right', // 可能需要根据实际情况调整位置
                grid: {
                    display: false
                }
              }
          }
      }
  });

  updateHighlightTargets(data);
  
  return true;
}

async function updateThisMktCapChart() {
  var data = await requestMktCapChgData();
  const labels = data.map(item => item.symbol);
  
  mktCapBarChart.data.labels = data.map(item => item.symbol);
  mktCapBarChart.data.datasets[0].data = data.map(item => item.future_chg);
  mktCapBarChart.data.datasets[1].data = data.map(item => item.fut_oi_pctg);
  mktCapBarChart.data.datasets[2].data = data.map(item => item.average_future_chg);
  mktCapBarChart.data.datasets[3].data = data.map(item => item.call_chg);
  mktCapBarChart.data.datasets[4].data = data.map(item => item.put_chg);
  mktCapBarChart.data.datasets[5].data = data.map(item => item.cp_factor);
  mktCapBarChart.data.datasets[5].backgroundColor = data.map(item => item.cp_factor > 0 ? '#ef5285' : '#60c5ba'),
  mktCapBarChart.data.datasets[6].data = data.map(item => item.call_avg);
  mktCapBarChart.data.datasets[7].data = data.map(item => item.put_avg);

  priceDevChart.data.labels = data.map(item => item.symbol);
  priceDevChart.data.datasets[0].data = data.map(item => item.price_deviate);
  priceDevChart.data.datasets[0].backgroundColor = data.map(item => item.price_deviate > 0 ? 'red' : 'green');
  
  updateHighlightTargets(data);

  mktCapBarChart.options.scales.x.ticks.color = function(context) {
    const index = context.index;
    const label = labels[index];
    if (longTargetList.includes(label)) {
        return '#ef5285'; 
    } else if (shortTargetList.includes(label)) {
        return '#60c5ba';
    } else if (nilTargetList.includes(label)) {
        return '#CCCCCC';
    } else {
        return '#CCCCCC';
    }
  };

  priceDevChart.options.scales.x.ticks.color = function(context) {
    const index = context.index;
    const label = labels[index];
    if (longTargetList.includes(label)) {
        return '#ef5285'; 
    } else if (shortTargetList.includes(label)) {
        return '#60c5ba';
    } else if (nilTargetList.includes(label)) {
        return '#CCCCCC';
    } else {
        return '#CCCCCC';
    }
  };

  mktCapBarChart.update();
  priceDevChart.update();
}

if (drawThisMktCapChart()) {
  /// 图片已经初始化，可以开启定时更新;
  setInterval(updateThisMktCapChart, updateMidTimeInterval);
}

setInterval(markHighLightSymbols, updateSlowTimeInterval);

const questForm = document.getElementById('quest-form');
const ratioLimitForm = document.getElementById('ratio-filter');
const expiryLimitForm = document.getElementById('expiry-filter');
const lossLimitForm = document.getElementById('loss-filter');
const responseTable = document.getElementById('response-table');

let markDict = {};

const markLevels = {
  zero: `<span style="font-size: 13px; color: yellow;">💡</span>`,
  one: `<span style="font-size: 13px; color: yellow;">🌟</span>`,
  two: `<span style="font-size: 13px; color: yellow;">🌙</span>`,
  three: `<span style="font-size: 13px; color: yellow;">☀️</span>`,
}

function markSymbol(row, index_num, mark) {
  /// 首先 标的方向和一致性预期方向一致
  /// 先确定1. 持续的增仓；
  /// 再确定1. 本交易日大单资金满足要求，2. 历史持续大单资金满足要求
  /// 再确定 期权价格还在底部
  /// 以上就是一个可能的具备较好盈利潜质的交易机会所需满足的条件。
  /// 最后如果期权波动率曲线的斜率是满足要求的，则说明市场已经认可这个交易观点。但此时，期权价格在这个方向上可能已经脱离底部区间，所以把这一条放在最后
  const nReq = 2;
  const dReq = 3;
  const rReq = 1;
  const pReq = 1;
  const cpVal = mark.cp === 'C' ? 1 : -1;
  if ((mark.x != 0) && (mark.x * cpVal < 0)) {
    return row.cells[index_num].innerHTML;
  }
  const cellInnerHtml = mark.nVal >= nReq ? (
    ((Math.abs(mark.dVal) >= dReq) && (mark.dVal * cpVal > 0)) ? (
      mark.pVal >= pReq ? (
        ((Math.abs(mark.rVal) >= rReq) && (mark.rVal * cpVal > 0)) ? markLevels.three : markLevels.two
      ) : markLevels.one
    ) : markLevels.zero
  ) : row.cells[index_num].innerHTML; 
  return cellInnerHtml;
}

function generateBarDisplay(color, heightVal, widthVal) {
  const bar_display = document.createElement('div');
  bar_display.classList.add('size-bar');
  bar_display.style.backgroundColor = color;
  bar_display.style.width = `${Math.min(Math.max(0, widthVal), 50)}px`;
  bar_display.style.height = `${heightVal}px`;
  return bar_display;
}

function generateNegPosBar(value, multiplier) {
  const disp = document.createElement('div');
  disp.classList.add('size-bar');
  disp.style.backgroundColor = value > 0 ? '#FF4081' : '#B2FF59';
  disp.style.width = `${Math.min(Math.abs(value) * multiplier, 50)}px`;
  disp.style.height = `2px`;
  return disp;
}

function addBlock(rmVal, blocks, typ) {
  const rm_block = document.createElement('div');
  rm_block.className = typ;
  rm_block.style.backgroundColor = rmVal > 0 ? '#FF4081' : rmVal < 0 ? '#B2FF59' : '#FFFFF3';
  blocks.appendChild(rm_block);
}

function format_cells(header, original_val_, val_, cell, markVal) {
  if (header == 'contract') {
    const rms = original_val_.split('$')[1];
    original_val_ = original_val_.split('$')[0];
    const rmParts = rms.split('#');
    const rm1 = parseFloat(rmParts[0]);
    const rm2 = parseFloat(rmParts[1]);
    const rmu1 = parseFloat(rmParts[2]);
    const rmu2 = parseFloat(rmParts[3]);

    const ud = rmu1 * rmu2 > 0 ? rmu1 : 0;
    
    const undRMblocks = document.createElement('div');
    undRMblocks.className = 'ublocks';
    addBlock(rmu1, undRMblocks, 'ublock');
    addBlock(rmu2, undRMblocks, 'ublock');
    const optRMblocks = document.createElement('div');
    optRMblocks.className = 'oblocks';
    addBlock(rm1, optRMblocks, 'oblock');
    addBlock(rm2, optRMblocks, 'oblock');
    
    repl_ = original_val_.replace(/-(C|P)-/g, function(match, p1) {
      if (p1 === 'C') {
        markVal.dVal += ud > 0 ? 1 : 0;
        return '-<span style="color: #FF4081;">C</span>-';
      } else if (p1 === 'P') {
        markVal.dVal += ud < 0 ? -1 : 0;
        return '-<span style="color: #B2FF59;">P</span>-';
      }
    });
    cell.innerHTML = repl_;
    cell.appendChild(undRMblocks);
    cell.appendChild(optRMblocks);
  } else if (header == 'mdx' || header == 'oix') {
    const parts = original_val_.split('/');
    const top = parseFloat(parts[0]);
    const bot = parseFloat(parts[1]);
    const colorTop = top < 0.5 ? '#FF4081' : '#B2FF59';
    const colorBot = bot >= 0.5 ? '#FF4081' : '#B2FF59';
    markVal.nVal += ((header == 'oix') && (top < 0.5) && (bot >= 0.5)) ? 1 : 0;
    markVal.nVal += ((header == 'mdx') && (((markVal.cp == 'C') && (bot >= 0.5)) || ((markVal.cp == 'P') && (top >= 0.5)))) ? 1 : 0;
    cell.innerHTML = `<span style="font-size: 12px; color: ${colorTop};">${num_round_up(top, 2)}</span> / <span style="font-size: 12px; color: ${colorBot};">${num_round_up(bot, 2)}</span>`;
    cell.style.backgroundColor = "#005f6b";
  } else if (header.includes('hpp')) {
    const parts = original_val_.split('/');
    const hpp = parseFloat(parts[0]);
    const hpd = parseFloat(parts[1]);
    const color_pm = hpp >= 50 ? hpp > 100 ? '#FF4081' : '#F6B352' : hpp < 0 ? '#B2FF59' : '#C5E99B';
    multip_ = 0.2;
    const barPM = generateNegPosBar(hpp, multip_);
    const color_pd = hpd >= 100 ? '#f94e3f' : hpd < 30 ? '#fff1b9' : '#a5dff9';
    markVal.pVal += ((!header.includes('und')) && (hpd < 30)) ? 1 : 0;
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${hpp}%</span> / <span style="font-size: 12px; color: ${color_pd};">${hpd}%</span>`;
    cell.appendChild(barPM);
    cell.style.backgroundColor = "#005f6b";
  } 
  /// 非数字处理都在上方
  if ((isNaN(original_val_)) || (original_val_ == null)) {
    return;
  }
  if (header == 'RATIO') {
    colorR = original_val_ >= 3 ? (original_val_ >= 5 ? '#bd1550' : '#EC7357') : '#f8ca00';
    const barDisp = generateBarDisplay(colorR, 2, original_val_ * 3);
    cell.appendChild(barDisp);
  } else if (header.includes('p_delta')) {
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    multip_ = header.includes('und') ? 5 : 0.2;
    const barPM = generateNegPosBar(original_val_, multip_);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}%</span>`;
    cell.appendChild(barPM);
  } else if (header == 'premium') {
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    const barPM = generateNegPosBar(original_val_, 0.5);
    val_ = original_val_ >= 1000 ? "1000+" : val_;
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}%</span>`;
    cell.appendChild(barPM);
  } else if (header == 'iv') {
    const color_v = '#fff1b9';
    const barIV = generateBarDisplay(color_v, 2, original_val_);
    cell.appendChild(barIV);
  } else if (header.includes('oi_delta')) {
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    const barPM = generateNegPosBar(original_val_, 0.5);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}%</span>`;
    cell.appendChild(barPM);
  } else if (header == 'UND_L/S') {
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    markVal.dVal += original_val_ > 0 ? 1 : original_val_ < 0 ? -1 : 0;
    const barPM = generateNegPosBar(original_val_, 0.5);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}</span>`;
    cell.appendChild(barPM);
    cell.style.backgroundColor = "#005f6b";
  } else if (header == 'OPT_L/S') {
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    const barPM = generateNegPosBar(original_val_, 0.5);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}</span>`;
    cell.appendChild(barPM);
    cell.style.backgroundColor = "#005f6b";
  } else if (header == 'X') {
    markVal.x = Math.sign(original_val_);
    const sym = original_val_ > 0 ? '↑' : original_val_ < 0 ? '↓' : '-';
    const symCor = original_val_ > 0 ? '#FF4081' : original_val_ < 0 ? '#B2FF59' : '#FFFFF3';
    cell.innerHTML = `<span style="font-size: 12px; color: ${symCor};">${sym}</span>`;
  } else if (header == 'expiry') {
    const txtColor = original_val_ > 10 ? '#fffcf0' : '#6E7783';
    cell.innerHTML = `<span style="font-size: 12px; color: ${txtColor};">${original_val_}</span>`;
  } else if (header == 'Est.LOSS') {
    const color_pm = "#FFBC42";
    const barPM = generateBarDisplay(color_pm, 2, original_val_/10);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}</span>`;
    cell.appendChild(barPM);
  } else if (header == 'MOV_UND') {
    // MOV_UND以万为单位，数量级在千万至亿级别
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    markVal.dVal += original_val_ > 0 ? 1 : original_val_ < 0 ? -1 : 0;
    const barPM = generateNegPosBar(original_val_, 1/400);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}</span>`;
    cell.appendChild(barPM);
    cell.style.backgroundColor = "#005f6b";
  } else if (header == 'MOV_OPT') {
    // MOV_UND以万为单位，数量级在几千至几百万级别
    const color_pm = original_val_ >= 0 ? '#FF4081' : '#B2FF59';
    const barPM = generateNegPosBar(original_val_, 1/2);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_pm};">${val_}</span>`;
    cell.appendChild(barPM);
    cell.style.backgroundColor = "#005f6b";
  } else if (header == 'Mkt.Value') {
    const color_ = original_val_ >= 1000000 ? "#e94e77" : original_val_ >= 500000 ? "#d68189" : original_val_ >= 200000 ? "#c6a49a" : "#c6e5d9";
    val_ = original_val_.toExponential(2);
    cell.innerHTML = `<span style="font-size: 12px; color: ${color_};">${val_}</span>`;
  }
}

function num_round_up(value_, to_fix) {
  var val_ = value_.toFixed(to_fix);
  val_ = val_.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return val_;
}

function headerFormatter(header) {
  header = header.toLowerCase().replace('delta', 'δ').replace('theta', 'θ');
  header = header.replace('underlying', 'und').replace('exchange', 'ex');
  header = header.replace('value', 'val').replace('expiry', 'exp');
  header = header.replace('est.', '');
  header = header.toUpperCase();
  return header;
}

// Function to update the live table with new data
function updateTable(data, table_name) {
  const table = document.getElementById(table_name);
  clearTable(table);

  if (!data || data.length == 0) {
    return;
  }

  // Generate table headers dynamically based on columns in data
  const headers = Object.keys(data[0]);
  const headerRow = table.insertRow();
  headers.forEach(header => {
    if (header !== 'iv_ratio') {
      const th = document.createElement('th');
      th.textContent = headerFormatter(header);
      headerRow.appendChild(th);
    }
  });

  // Generate table rows dynamically based on data
  var prev_sym = '';
  var prev_ex = '';
  var prev_und = '';

  data.forEach(item => {
    const row = table.insertRow();
    let direct_cp_match = item['contract'].match(/-(\w)-/);
    let direct_cp = direct_cp_match ? direct_cp_match[1] : 0;
    let mark = {rVal: 0, dVal: 0, nVal: 0, pVal: 0, cp: direct_cp, x: 0};
    headers.forEach(header => {
      if (header == 'iv_ratio') {
        const val = item[header];
        mark.rVal += Math.sign(val);
      } else {
        const cell = row.insertCell();
        if (header == 'symbol') {
          if (prev_sym != item[header]) {
            cell.textContent = item[header];
            prev_sym = item[header];
          }
        } else if (header == 'exchange') {
          if (prev_ex != item[header]) {
            cell.textContent = item[header];
            prev_ex = item[header];
          }
        } else if (header == 'underlying') {
          if (prev_und != item[header]) {
            cell.textContent = item[header];
            prev_und = item[header];
          }
        } else {
          var original_val_ = item[header];
          // if val_ is a number, then format it as XXX,XXX,XXX.XX else leave it as it is            
          if ((header != 'X') && (!isNaN(original_val_)) && (original_val_ != null)) {
            var val_ = num_round_up(original_val_, 2);
          } else {
            var val_ = original_val_;
          }
          cell.textContent = val_;
          // display optimize
          format_cells(header, original_val_, val_, cell, mark);
        }
      }
    });
    let markHTML = markSymbol(row, 3, mark);
    row.cells[3].innerHTML = markHTML;
  });
}

var quest;

function getEventValue(event) {
  event.preventDefault();
  let inputValue = document.getElementById('quest').value;
  if (!quest.includes(inputValue)) {
    quest += " " + inputValue;
  }
  sessionStorage.setItem('questString', quest);
}

var ratioLimitMax = null;
var ratioLimitMin = null;

function getRatioLimits(event) {
  event.preventDefault();
  r_max = document.getElementById('filterValueMax').value;
  r_min = document.getElementById('filterValueMin').value;
  if (!(r_max === "" || isNaN(r_max))) {
    ratioLimitMax = r_max;
  } else {
    ratioLimitMax = null;
  }
  if (!(r_min === "" || isNaN(r_min))) {
    ratioLimitMin = r_min;
  } else {
    ratioLimitMin = null;
  }
}

var expMax = null;
var expMin = null;

function getExpiryLimits(event) {
  event.preventDefault();
  r_max = document.getElementById('expiryMax').value;
  r_min = document.getElementById('expiryMin').value;
  if (!(r_max === "" || isNaN(r_max))) {
    expMax = r_max;
  } else {
    expMax = null;
  }
  if (!(r_min === "" || isNaN(r_min))) {
    expMin = r_min;
  } else {
    expMin = null;
  }
}

var lossMax = null;

function getLossLimit(event) {
  event.preventDefault();
  r_max = document.getElementById('lossMax').value;
  if (!(r_max === "" || isNaN(r_max))) {
    lossMax = r_max;
  } else {
    lossMax = null;
  }
}

function liveTargetTableListener(table_name) {
  const liveTable = document.getElementById(table_name);
  liveTable.addEventListener('click', (event) => {
    const req_contract = event.target.parentNode.children[2].textContent;
    if (!quest.includes(req_contract)) {
      quest += " " + req_contract;
      sessionStorage.setItem('questString', quest);
    }
  });
}

function delResponseTableItemListener() {
  const liveTable = document.getElementById('response-table');
  liveTable.addEventListener('contextmenu', (event) => {
    event.preventDefault();
    var req_contract = event.target.parentNode.children[2].textContent;
    // convert both quest and req_contract into upper case
    quest = quest.toUpperCase();
    req_contract = req_contract.toUpperCase();
    if (quest.includes(req_contract)) {
      quest = quest.replace(req_contract, "").trim();
      sessionStorage.setItem('questString', quest);
    };
  });
}

let audioNotify = document.getElementById('audioNotify');

// Function to handle quest form submission
function handleQuestSubmit() {
  if ((!quest) || (quest.trim() == '')) {
    clearTable(responseTable);
    return;
  }
  fetch(root_url + '/quest_targets', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ quest })
  })
  .then(response => response.json())
  .then(data => {
    clearTable(responseTable);

    if (!data || data.length == 0) {
      return;
    }
    
    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const headerRow = responseTable.insertRow();
    headers.forEach(header => {
      if (header !== 'iv_ratio') {
        const th = document.createElement('th');
        th.textContent = headerFormatter(header);
        headerRow.appendChild(th);
      }
    });

    // Generate table rows dynamically based on data
    let targetSelectedUnderlying = [...new Set(data.map(row => row['underlying']))];
    if ((audioNotify.checked) && (targetSelectedUnderlying.some(item => !questTableTargetSelectedUnderlying.includes(item)))) {
      let newTargets = targetSelectedUnderlying.filter(item => !questTableTargetSelectedUnderlying.includes(item));
      newTargetAudio.onended = function() {
        newTargets.forEach(element => {
          let utterance = new SpeechSynthesisUtterance(element.split('').join(' '));
          window.speechSynthesis.speak(utterance);
        });
      }
      newTargetAudio.play();
    }
    questTableTargetSelectedUnderlying = targetSelectedUnderlying;

    data.forEach(item => {
      const row = responseTable.insertRow();
      let direct_cp_match = item['contract'].match(/-(\w)-/);
      let direct_cp = direct_cp_match ? direct_cp_match[1] : 0;
      let mark = {rVal: 0, dVal: 0, nVal: 0, pVal: 0, cp: direct_cp, x: 0};
      headers.forEach(header => {
        if (header == 'iv_ratio') {
          const val = item[header];
          mark.rVal += Math.sign(val);
        } else {
          const cell = row.insertCell();
          var original_val_ = item[header];
          if ((!isNaN(original_val_)) && (original_val_ != null)) {
            val_ = original_val_.toFixed(2);
            val_ = val_.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
          } else {
            val_ = original_val_;
          }
          cell.textContent = val_;
          // display optimize
          format_cells(header, original_val_, val_, cell, mark);
        }
      });
      let markHTML = markSymbol(row, 3, mark);
      row.cells[3].innerHTML = markHTML;
      markDict[row.cells[2].textContent] = row.cells[3].textContent;
    });
  });
}

// Add event listener to quest form submit button
questForm.addEventListener('submit', getEventValue);
ratioLimitForm.addEventListener('submit', getRatioLimits);
expiryLimitForm.addEventListener('submit', getExpiryLimits);
lossLimitForm.addEventListener('submit', getLossLimit);
liveTargetTableListener('live-table');
liveTargetTableListener('symbol-opt-table');
liveTargetTableListener('option-filter-table');
delResponseTableItemListener();
setInterval(handleQuestSubmit, updateFastTimeInterval);


let capitalChgSymbol = document.getElementById('capitalChgSymbol');
let limitOiContract = document.getElementById('limitOiContract');
let recentLimitOiContract = document.getElementById('recentLimitOiContract');
let upContract = document.getElementById('upContract');
let downContract = document.getElementById('downContract');

upContract.addEventListener('change', function() {
  if(this.checked) {
      downContract.checked = false;
  }
});

downContract.addEventListener('change', function() {
  if(this.checked) {
      upContract.checked = false;
  }
});


// Function to periodically update the live table
function updateLiveTablePeriodically() {
  fetch(root_url + '/live_targets', {
    method: 'GET',
  })
  .then(response => response.json())
  .then(data => {
    if (capitalChgSymbol.checked) {
      data = data.filter(item => {
        let symbol = item.underlying.slice(0, -4);
        return topSymbols.includes(symbol);
      });
    }
    if (limitOiContract.checked) {
      data = data.filter(item => increasingOiContractTargets.includes(item.underlying));
    }
    if (recentLimitOiContract.checked) {
      data = data.filter(item => {
        let parts = item.oix.split('/');
        let top = parseFloat(parts[0]);
        let bot = parseFloat(parts[1]);
        return top < 0.5 && bot >= 0.5;
      });
    }
    if (upContract.checked) {
      data = data.filter(item => {
        let parts = item.mdx.split('/');
        let top = parseFloat(parts[0]);
        let bot = parseFloat(parts[1]);
        return top < 0.5 && bot >= 0.5;
      });
    }
    if (downContract.checked) {
      data = data.filter(item => {
        let parts = item.mdx.split('/');
        let top = parseFloat(parts[0]);
        let bot = parseFloat(parts[1]);
        return top >= 0.5 && bot < 0.5;
      });
    }
    if (ratioLimitMin != null) {
      data = data.filter(function(row) {
        return Number(row.RATIO) >= Number(ratioLimitMin);
      });
    }
    if (ratioLimitMax != null) {
      data = data.filter(function(row) {
        return Number(row.RATIO) <= Number(ratioLimitMax);
      });
    }
    if (expMin != null) {
      data = data.filter(function(row) {
        return Number(row.expiry) >= Number(expMin);
      });
    }
    if (expMax != null) {
      data = data.filter(function(row) {
        return Number(row.expiry) <= Number(expMax);
      });
    }
    if (lossMax != null) {
      data = data.filter(function(row) {
        return Number(row['Est.LOSS']) <= Number(lossMax);
      });
    }
    brokerTargetList = data.map(t => t['contract'].split('$')[0]);
    updateTable(data, 'live-table');
  });
}

function shareMarkDictThroughLocalStore() {
  document.cookie = "markDict=" + JSON.stringify(markDict);
}

setInterval(shareMarkDictThroughLocalStore, updateFastTimeInterval);

var currentTimeString = "";

function updateTime() {
  const now = new Date();
  const hours = now.getHours().toString().padStart(2, '0');
  const minutes = now.getMinutes().toString().padStart(2, '0');
  const seconds = now.getSeconds().toString().padStart(2, '0');
  const timeString = `${hours}:${minutes}:${seconds}`;
  currentTimeString = timeString;
  document.querySelector('h1').textContent = `Live Monitor - ${timeString}`;
}

setInterval(updateTime, 1000);

// Update the live table every 0.5 seconds
setInterval(updateLiveTablePeriodically, updateFastTimeInterval);

function sortStringArray(arr) {
  arr.sort((a, b) => {
    const numA = parseInt(a);
    const numB = parseInt(b);
    return numA - numB;
  });
  
  return arr;
}

/// open chart page on click of response table

function onRespTableClickDrawChartPage() {
  const table = document.getElementById('response-table');
  table.addEventListener('click', (event) => {
    const contract = event.target.parentNode.children[2].textContent;
    var page_url = chart_url + '/localChart/' + contract;
    window.open(page_url, '_blank');
  });
}

onRespTableClickDrawChartPage();


async function getFutTargetData() {
  const response = await fetch('/future_targets');
  const data = await response.json();
  const tick = JSON.parse(data.tick);
  const main = data.main_list;
  const rmu = JSON.parse(data.rmu);
  const spot_dt = data.spot_dt;
  return [tick, main, rmu, spot_dt];
}

function updateFutTargetHeader(table, headers) {
  const thead = table.querySelector('thead');
  thead.innerHTML = ''; // 清空表头

  const headerRow = document.createElement('tr');
  for (const header of headers) {
    const th = document.createElement('th');
    th.textContent = headerFormatter(header);
    headerRow.appendChild(th);
  }

  thead.appendChild(headerRow);
}

async function updateFutureTargetTablePeriodically() {
  const [tick, main, rDict, spot_dt] = await getFutTargetData();
  const table = document.getElementById('future-targets');

  if (!tick || tick.length == 0) {
    return;
  }

  const rawHeaders = Object.keys(tick[0]);
  const frontHeaders = ['exchange', 'symbol', 'X', 'spot'];
  const showFronts = ['exchange', 'symbol', 'fwdCurve', spot_dt];
  const filtHeaders = rawHeaders.filter(item => !frontHeaders.includes(item));
  const headers = showFronts.concat(filtHeaders);

  updateFutTargetHeader(table, headers);

  const tbody = table.querySelector('tbody');
  tbody.innerHTML = '';

  var prev_ex = '';

  tick.forEach(item => {
    const row = tbody.insertRow();
    const xVal = item['X'];
    const colorX = xVal > 0 ? '#FF4081' : xVal < 0 ? '#B2FF59' : '#FFFFF3';

    const spotP = item['spot'];
    
    headers.forEach(header => {
      const cell = row.insertCell();
      if (header == 'exchange') {
        if (prev_ex != item[header]) {
          cell.textContent = item[header];
          prev_ex = item[header];
        }
      } else if (header == 'symbol') {
        cell.innerHTML = `<span style="font-size: 12px; color: ${colorX};">${item[header]}</span>`;
      } else if (header == spot_dt) {
        cell.textContent = spotP;
      } else if (header == 'fwdCurve') {
        
        var values = [];
        for (const h of filtHeaders) {
          if (item[h] != null) {
            const v = parseFloat(item[h].split('|')[0]);
            values.push(v);
          }
        }
        var withSpotValues = spotP != null ? [spotP].concat(values): values;
        const maxValue = Math.max(...withSpotValues);
        const minValue = Math.min(...withSpotValues);
        const range = maxValue - minValue;
        
        if (withSpotValues.length > 1) {
          const canvas = document.createElement('canvas');
          canvas.width = 60;
          canvas.height = 18;
          const ctx = canvas.getContext('2d');
          ctx.beginPath();
          ctx.moveTo(0, 0);

          const step = canvas.width / (withSpotValues.length - 1);
          for (let i = 0; i < withSpotValues.length; i++) {
            const x = i * step;
            const scaledValue = (withSpotValues[i] - minValue) / range; // 缩放数据点的位置
            const y = (1 - scaledValue) * canvas.height;
            if (i == 0) {
              ctx.moveTo(x, y);
            } else {
              ctx.lineTo(x, y);
            }
          }
          ctx.strokeStyle = '#EFFFE9';
          ctx.stroke();

          cell.appendChild(canvas);
        }
      } else {
        const curContract = item['symbol'] + header;
        if (item[header] != null) {
          var valueLast = parseFloat(item[header].split('|')[0]);
          var valueVol = parseFloat(item[header].split('|')[1]);
        } else {
          var valueLast = null;
          var valueVol = null;
        }
        if ((valueLast != null) && (spotP != null)) {
          var basis = ((spotP - valueLast) / spotP) * 100;
        } else {
          var basis = null;
        }
        if (basis != null) {
          const colorBasis = basis > 0 ? '#ef5285' : basis < 0 ? '#60c5ba' : '#e1eef6';
          var txtBasis = `<span style="font-size: 12px; color: ${colorBasis};">(${num_round_up(basis, 2)}%)</span>`;
        } else {
          var txtBasis = "";
        }

        var txtValueLast = valueLast != null ? num_round_up(valueLast, 2) : "";
        if (main.includes(curContract)) {
          txtValueLast = `<span style="font-size: 12px; color: #FFBC42;">${txtValueLast}</span>`;
        } 

        cell.innerHTML = txtValueLast + " " + txtBasis;

        if ((curContract in rDict) && (valueLast != null)) {
          rmu1Val = rDict[curContract].rm1;
          rmu2Val = rDict[curContract].rm2;
          const rmuBlocks = document.createElement('div');
          rmuBlocks.className = 'ublocks';
          addBlock(rmu1Val, rmuBlocks, 'ublock');
          addBlock(rmu2Val, rmuBlocks, 'ublock');
          cell.appendChild(rmuBlocks);
        }
        
        if (valueVol != null) {
          const bar = generateBarDisplay("#a5dff9", 2, Math.pow(Math.log10(valueVol), 2));
          cell.appendChild(bar);
        }

      }
    });
  });
}

updateFutureTargetTablePeriodically();
setInterval(updateFutureTargetTablePeriodically, updateSlowTimeInterval);


/// VIP ranking bar chart
fetch(root_url + '/VIP_ranking')
.then(response => response.json())
.then(data => {
  const vipRankPerformanceCanvas = document.getElementById('vipPerformanceRankingCanvas');
  vipRankPerformanceChart = new Chart(vipRankPerformanceCanvas, {
    type: 'bar',
    data: {
      labels: data.map(item => item.broker),
      datasets: [
        {
          label: 'VIP performance rank',
          data: data.map(item => item.information_score),
          backgroundColor: data.map(item => item.information_score > 0 ? '#fc9d9a' : '#a8dba8'),
        },
        {
          label: 'informed',
          data: data.map(item => item.informed),
          backgroundColor: '#fffff5',
        },
        {
          label: 'uninformed',
          data: data.map(item => item.uninformed),
          backgroundColor: '#7f9eb2',
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
})

var histIvChart;


async function getHistIvData() {
  const resp = await fetch(root_url + '/hist_avg_iv');
  const data = await resp.json();
  return data;
}

async function startHistIvChart() {
  data = await getHistIvData();
  const histIvAvgCanvas = document.getElementById('histIvAvgCanvas');
  histIvChart = new Chart(histIvAvgCanvas, {
    type: 'line',
    data: {
      labels: data.map(item => item.trading_date),
      datasets: [
        {
          label: 'call avg',
          data: data.map(item => item.call_avg),
          borderColor: '#ef5285',
          pointRadius: 0, // 不显示数据点
          showLine: true, // 显示线条
          hidden: true
        },
        {
          label: 'put avg',
          data: data.map(item => item.put_avg),
          borderColor: '#60c5ba',
          pointRadius: 0, // 不显示数据点
          showLine: true, // 显示线条
          hidden: true
        },
        {
          label: 'future mkt cap',
          data: data.map(item => item.mkt_cap),
          borderColor: '#FBFFB9',
          pointRadius: 0, // 不显示数据点
          showLine: true, // 显示线条,
          yAxisID: 'futMkt'
        },
        {
          label: 'future oi',
          data: data.map(item => item.open_interest),
          borderColor: '#FDD692',
          pointRadius: 0, // 不显示数据点
          showLine: true, // 显示线条
          yAxisID: 'futOi'
        }, 
        {
          label: 'turnover rate',
          data: data.map(item => item.turnover_rate),
          borderColor: '#D7FFF1',
          pointRadius: 0, // 不显示数据点
          showLine: true, // 显示线条
          yAxisID: 'toRate'
        }, 
        {
          type: 'bar',
          label: 'turnover',
          data: data.map(item => item.turnover),
          backgroundColor: '#218380',
          yAxisID: 'turnover',
          hidden: true
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
        },
        futMkt: {
          display: false,
          position: 'right', // 可能需要根据实际情况调整位置
          grid: {
              display: false
          }
        },
        futOi: {
          display: false,
          position: 'right', // 可能需要根据实际情况调整位置
          grid: {
              display: false
          }
        },
        toRate: {
          display: false,
          position: 'right', // 可能需要根据实际情况调整位置
          grid: {
              display: false
          }
        },
        turnover: {
          display: false,
          position: 'right', // 可能需要根据实际情况调整位置
          grid: {
              display: false
          }
        }
      }
    }
  });

  return true;
}


async function updateHistIvChart() {
  const data = await getHistIvData();
  histIvChart.data.labels = data.map(item => item.trading_date);
  histIvChart.data.datasets[0].data = data.map(item => item.call_avg);
  histIvChart.data.datasets[1].data = data.map(item => item.put_avg);
  histIvChart.data.datasets[2].data = data.map(item => item.mkt_cap);
  histIvChart.data.datasets[3].data = data.map(item => item.open_interest);
  histIvChart.data.datasets[4].data = data.map(item => item.turnover_rate);
  histIvChart.data.datasets[5].data = data.map(item => item.turnover);

  histIvChart.update();
}

if (startHistIvChart()) {
  /// 图片已经初始化，可以开启定时更新;
  setInterval(updateHistIvChart, updateSlowTimeInterval);
}


function getOiHighColor(keyParam) {
  const colors = [
      '#800000', '#8B0000', '#A52A2A', '#B22222', '#DC143C', '#FF0000', '#FF4500', '#FF6347', '#FF7F50', '#FF8C00', '#FFA500', '#FFD700'
  ];
  return colors[keyParam]
}

function formOiHighDatasets(dat, xLabel) {
  const datasets = [];
  const keys = Object.keys(dat[0]);
  VolKeys = keys.filter(key => key !== xLabel);
  VolKeys.forEach((key, index) => {
      datasets.push({
          label: key.toUpperCase(),
          data: dat.map(item => {
              if (String(item[key]).includes('-')) {
                  return parseInt(item[key].split('-')[1]);
              } else {
                  return parseInt(item[key]);
              }
          }),
          backgroundColor: dat.map(item => {
              if (String(item[key]).includes('-')) {
                  return getOiHighColor(parseInt(key.slice(-2)) - 1);
              } else {
                  return "#d6ecfa";
              }
          }),
          yAxisID: 'y',
      });
  });
  return datasets;
}


document.getElementById('call').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('put').checked = false;
  }
});

document.getElementById('put').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('call').checked = false;
  }
});

document.getElementById('option-calculator').addEventListener('submit', function(event) {
  event.preventDefault();

  var naming_ = ""
  var optType = 0;
  if (event.target['call'].checked) {
    optType = 1;
    naming_ = 'call';
  } else if (event.target['put'].checked) {
    optType = -1;
    naming_ = 'put';
  }

  var stockPrice = parseFloat(event.target['stock-price'].value);
  var strikePrice = parseFloat(event.target['strike-price'].value);
  var riskFreeRate = parseFloat(event.target['risk-free-rate'].value);
  var volatility = parseFloat(event.target['volatility'].value);
  var timeToExpiration = parseFloat(event.target['time-to-expiration'].value);

  fetch((root_url + '/opt_calc'), {
    method: 'POST', 
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      'type': optType,
      'undP': stockPrice,
      'strike': strikePrice,
      'rate': riskFreeRate,
      'vol': volatility,
      'exp': timeToExpiration
    }), 
  })
  .then(response => response.json())
  .then(data => document.getElementById('result').textContent = `Underlying trading @${stockPrice} ${naming_} option strike @${strikePrice} theoretical price is ` + data.toFixed(2));
});


function get_earliest_expiry() {
  fetch((root_url + '/exchange_earliest_expiry'))
  .then(response => response.json())
  .then(data => {
    const table = document.getElementById('exchange-earliest-expiry-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');

    thead.innerHTML = ''; // 清空表头
    tbody.innerHTML = ''; // 清空表

    const headers = Object.keys(data); // 设置表头

    // 创建表头
    const headerRow = document.createElement('tr');
    headers.forEach(header => {
      const th = document.createElement('th');
      th.textContent = headerFormatter(header);
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // 创建数据行
    const dtRow = tbody.insertRow();
    const contractsRow = tbody.insertRow();
    headers.forEach(header => {
      const dtCell = dtRow.insertCell();
      const contractsCell = contractsRow.insertCell();

      const exp_val = data[header]['dt']; 
      const exp_color = exp_val < 5 ? '#E0E3DA' : exp_val < 15 ? '#F68657' : exp_val < 25 ? '#F7AA97' : '#a5dff9';
      dtCell.innerHTML = `<span style="font-size: 15px; font: bolder; font-family: Arial, Helvetica, sans-serif; color: ${exp_color};">${exp_val}</span>`;

      contractsCell.textContent = data[header]['contracts'].join(' ');
    });
  });
}


// function update_oi_status_table() {
//   fetch((root_url + '/oi_high_targets'))
//   .then(response => response.json())
//   .then(data => {
//     const table = document.getElementById('oi-status-table');
//     clearTable(table);

//     if (!data || data.length == 0) {
//       return;
//     }
//     // Generate table headers dynamically based on columns in data
//     var headers = Object.keys(data[0]);
//     if ((optionOnly) && (optionSymbols != null)) {
//       headers = ['contract', ...headers.filter(item => optionSymbols.includes(item))];
//     }
//     if (signalTargetsOnly) {
//       var signalTargetList = longTargetList.concat(shortTargetList);
//       headers = ['contract', ...headers.filter(item => signalTargetList.includes(item))];
//     }
//     const headerRow = table.insertRow();
//     headers.forEach(header => {
//       const th = document.createElement('th');
//       th.textContent = headerFormatter(header);
//       headerRow.appendChild(th);
//     });

//     data.forEach(item => {
//       const row = table.insertRow();
//       headers.forEach(header => {
        
//         const cell = row.insertCell();

//         var original_val_ = item[header];
//         if (original_val_ && original_val_.startsWith('L-')) {
//           original_val_ = original_val_.replace('L-', '');
//           cell.style.backgroundColor = '#FF4081';
//         }
//         if (original_val_ && original_val_.endsWith('-S')) {
//           original_val_ = original_val_.replace('-S', '');
//           cell.style.border = '2px solid #feee7d';
//           cell.style.color = '#F6B352';
//         }
//         cell.textContent = original_val_;
        
        
//       });
//     });
//   });
// }

// update_oi_status_table();
// setInterval(update_oi_status_table, updateSlowTimeInterval);


// function update_oi_chg_pctg_table() {
//   fetch((root_url + '/oi_chg_pctg_rank'))
//   .then(response => response.json())
//   .then(data => {
//     const table = document.getElementById('oi-chg-pctg-table');
//     clearTable(table);

//     if (!data || data.length == 0) {
//       return;
//     }

//     if (IncPosSymbols.length !== 0) {
//       data = data.filter(item => IncPosSymbols.includes(item.symbol));
//     }

//     // Generate table headers dynamically based on columns in data
//     const headers = ['symbol', 'tot_oi_chg_pctg', ...Object.keys(data[0]).filter(i => i !== 'symbol' && i !== 'tot_oi_chg_pctg')];
//     const headerRow = table.insertRow();
//     headers.forEach(header => {
//       const th = document.createElement('th');
//       th.textContent = headerFormatter(header);
//       headerRow.appendChild(th);
//     });

//     data.forEach(item => {
//       const row = table.insertRow();
//       headers.forEach(header => {
      
//         const cell = row.insertCell();

//         var original_val_ = item[header];
//         if ((header !== 'symbol') && (original_val_ !== null)) {
//           if (header !== 'tot_oi_chg_pctg') {
//             if (original_val_ <= 50) {
//               cell.style.backgroundColor = '#ff7473';
//               cell.style.color = 'black';
//             } else if (original_val_ <= 100) {
//               cell.style.backgroundColor = '#ffc952';
//               cell.style.color = 'black';
//             } else if ((original_val_ === null) || (original_val_ > 100)) {
//               cell.style.backgroundColor = '#77AF9C';
//             }
//           }
//           original_val_ = num_round_up(original_val_, 2);
//         }
//         cell.textContent = original_val_;
      
      
//       });
//     });
//   });
// }

// update_oi_chg_pctg_table();
// setInterval(update_oi_chg_pctg_table, updateSlowTimeInterval);


function update_unusual_option_whales_table() {
  fetch((root_url + '/unusual_option_whales'))
  .then(response => response.json())
  .then(data => {
    const table = document.getElementById('unusual-option-whales-table');
    clearTable(table);

    if (!data || data.length == 0) {
      return;
    }

    // Generate table headers dynamically based on columns in data
    const headers = Object.keys(data[0]);
    const headerRow = table.insertRow();
    headers.forEach(header => {
      const th = document.createElement('th');
      th.textContent = headerFormatter(header);
      headerRow.appendChild(th);
    });

    data.forEach(item => {
      const row = table.insertRow();
      headers.forEach(header => {

        const cell = row.insertCell();
        
        var original_val_ = item[header];
        cell.textContent = original_val_;
        
      });
    });
  });
}

update_unusual_option_whales_table();
setInterval(update_unusual_option_whales_table, updateSlowTimeInterval);


function openWatchListPage() {
  window.open(root_url + '/target-watch-list', '_blank');
}



document.getElementById('filter-call').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-put').checked = false;
  }
});

document.getElementById('filter-put').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-call').checked = false;
  }
});

document.getElementById('filter-md-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-md-dec').checked = false;
  }
});

document.getElementById('filter-md-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-md-inc').checked = false;
  }
});

document.getElementById('filter-prem-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-prem-dec').checked = false;
  }
});

document.getElementById('filter-prem-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-prem-inc').checked = false;
  }
});

document.getElementById('filter-undLs-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-undLs-dec').checked = false;
  }
});

document.getElementById('filter-undLs-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-undLs-inc').checked = false;
  }
});

document.getElementById('filter-optLs-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-optLs-dec').checked = false;
  }
});

document.getElementById('filter-optLs-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-optLs-inc').checked = false;
  }
});

document.getElementById('filter-undMov-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-undMov-dec').checked = false;
  }
});

document.getElementById('filter-undMov-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-undMov-inc').checked = false;
  }
});

document.getElementById('filter-optMov-inc').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-optMov-dec').checked = false;
  }
});

document.getElementById('filter-optMov-dec').addEventListener('change', function() {
  if(this.checked) {
      document.getElementById('filter-optMov-inc').checked = false;
  }
});

let filterParam;

document.getElementById('option-filter').addEventListener('submit', function(event) {
  event.preventDefault();

  var optType = 0;
  if (event.target['filter-call'].checked) {
    optType = 1;
  } else if (event.target['filter-put'].checked) {
    optType = -1;
  }

  var oiStat = event.target['filter-oi-inc'].checked ? 1 : 0;
  var mdStat = event.target['filter-md-inc'].checked ? 1 : event.target['filter-md-dec'].checked ? -1 : 0;
  var premStat = event.target['filter-prem-inc'].checked ? 1 : event.target['filter-prem-dec'].checked ? -1 : 0;
  var undLsStat = event.target['filter-undLs-inc'].checked ? 1 : event.target['filter-undLs-dec'].checked ? -1 : 0;
  var optLsStat = event.target['filter-optLs-inc'].checked ? 1 : event.target['filter-optLs-dec'].checked ? -1 : 0;
  var undMvStat = event.target['filter-undMov-inc'].checked ? 1 : event.target['filter-undMov-dec'].checked ? -1 : 0;
  var optMvStat = event.target['filter-optMov-inc'].checked ? 1 : event.target['filter-optMov-dec'].checked ? -1 : 0;

  var movUndLinkStat = event.target['filter-movUnd-link'].checked ? 1 : 0;
  var UndLsLinkStat = event.target['filter-UndLs-link'].checked ? 1 : 0;


  var pDelta = parseInt(event.target['filter-p-delta'].value);
  var mktVal = parseInt(event.target['filter-mkt-val'].value);
  var ratioL = parseFloat(event.target['filter-ratio-l'].value);
  var ratioH = parseFloat(event.target['filter-ratio-h'].value);
  var expL = parseInt(event.target['filter-exp-l'].value);
  var expH = parseInt(event.target['filter-exp-h'].value);

  filterParam = JSON.stringify({
    'optType': optType,
    'oiStat': oiStat,
    'mdStat': mdStat,
    'premStat': premStat,
    'undLsStat': undLsStat,
    'optLsStat': optLsStat,
    'undMvStat': undMvStat,
    'optMvStat': optMvStat,
    'pDelta': pDelta,
    'mktVal': mktVal,
    'ratioL': ratioL,
    'ratioH': ratioH,
    'expL': expL,
    'expH': expH,
    'movUndLinkStat': movUndLinkStat,
    'UndLsLinkStat': UndLsLinkStat
  });
});


function updateOptionFilterTable() {
  if (filterParam) {
    fetch((root_url + '/filter_options'), {
      method: 'POST', 
      headers: {
          'Content-Type': 'application/json',
      },
      body: filterParam, 
    })
    .then(response => response.json())
    .then(data => {
      oiTargetList = data.map(t => t['contract'].split('$')[0]);
      updateTable(data, 'option-filter-table');
    });
  }
}

function removeOptionFilterTable(event) {
  event.preventDefault();

  filterParam = null;
  const optFilterTable = document.getElementById('option-filter-table');
  clearTable(optFilterTable);
  setTimeout(() => {
    clearTable(optFilterTable);
  }, updateFastTimeInterval);
}

setInterval(updateOptionFilterTable, updateFastTimeInterval);


// async function getExtremePriceCntData() {
//   const resp = await fetch(root_url + '/extreme_price_cnt');
//   const data = await resp.json();
//   return data;
// }

// let lastRecordedValues = {};

// async function alertExtremePriceCntData() {
//   parsedData = await getExtremePriceCntData();

//   const filtLs1 = (optionOnly) && (optionSymbols != null) ? optionSymbols : [];
//   const filtLs2 = (OiIncOnly) && (OiIncSymbols != null) ? OiIncSymbols : [];
//   const filtLs = [...new Set([...filtLs1, ...filtLs2])];

//   if (filtLs.length !== 0) {
//     parsedData = {
//       columns: parsedData.columns.filter(col => filtLs.includes(col)), // 筛选列名
//       index: parsedData.index, // 保持索引不变
//       data: parsedData.data.map(row => row.filter((_, index) => filtLs.includes(parsedData.columns[index]))), // 筛选数据
//     };
//   }

//   const extremePNoteContainer = document.querySelector('.extreme-price-note-container');

//   parsedData.columns.forEach((column, index) => {
//     const data = parsedData.data.map(row => row[index]);
//     var latestValue = data[data.length - 1];
    
//     if (lastRecordedValues[column] == null) {
//       lastRecordedValues[column] = latestValue;
//     } else {
//       const latestValueStr = latestValue < 0 ? `低` : `高`;
//       // const secondLastValueStr = lastRecordedValues[column] < 0 ? `负 ${Math.abs(lastRecordedValues[column])}` : lastRecordedValues[column].toString();
  
//       if (Math.abs(latestValue) > Math.abs(lastRecordedValues[column])) {
//         const message = document.createElement('p');
//         const msg_txt_html = `${currentTimeString} ${column} 价格新${latestValueStr} ${lastRecordedValues[column]} => ${latestValue}。`;
//         message.textContent = msg_txt_html;
//         extremePNoteContainer.appendChild(message);
//         extremePNoteContainer.scrollTop = extremePNoteContainer.scrollHeight;

//         addMessage('ExPrice', column, lastRecordedValues[column], latestValue);
//       }

//       lastRecordedValues[column] = latestValue;
//     }
//   });
// }

// setInterval(alertExtremePriceCntData, updateSlowTimeInterval);



// async function getExtremePositionCntData() {
//   const resp = await fetch(root_url + '/extreme_position_cnt');
//   const data = await resp.json();
//   return data;
// }

// let lastRecordedPositionCntValues = {};

// async function alertExtremePositionCntData() {
//   parsedData = await getExtremePositionCntData();
  
//   const filtLs1 = (optionOnly) && (optionSymbols != null) ? optionSymbols : [];
//   const filtLs2 = (OiIncOnly) && (OiIncSymbols != null) ? OiIncSymbols : [];
//   const filtLs = [...new Set([...filtLs1, ...filtLs2])];

//   if (filtLs.length !== 0) {
//     parsedData = {
//       columns: parsedData.columns.filter(col => filtLs.includes(col)), // 筛选列名
//       index: parsedData.index, // 保持索引不变
//       data: parsedData.data.map(row => row.filter((_, index) => filtLs.includes(parsedData.columns[index]))), // 筛选数据
//     };
//   }

//   const extremePNoteContainer = document.querySelector('.extreme-position-note-container');

//   parsedData.columns.forEach((column, index) => {
//     const data = parsedData.data.map(row => row[index]);
//     var latestValue = data[data.length - 1];
    
//     if (lastRecordedPositionCntValues[column] == null) {
//       lastRecordedPositionCntValues[column] = latestValue;
//     } else {
//       // const latestValueStr = latestValue < 0 ? `负 ${Math.abs(latestValue)}` : latestValue.toString();
//       // const secondLastValueStr = lastRecordedPositionCntValues[column] < 0 ? `负 ${Math.abs(lastRecordedPositionCntValues[column])}` : lastRecordedPositionCntValues[column].toString();
  
//       if (latestValue > lastRecordedPositionCntValues[column]) {
//         const message = document.createElement('p');
//         const msg_txt_html = `${currentTimeString} ${column} 持仓新高 ${lastRecordedPositionCntValues[column]} => ${latestValue}。`;
//         message.textContent = msg_txt_html;
//         extremePNoteContainer.appendChild(message);
//         extremePNoteContainer.scrollTop = extremePNoteContainer.scrollHeight;

//         addMessage('ExPosition', column, lastRecordedPositionCntValues[column], latestValue);
//       }

//       lastRecordedPositionCntValues[column] = latestValue;
//     }
//   });
// }

// setInterval(alertExtremePositionCntData, updateSlowTimeInterval);


const optSymMktSizeButton = document.getElementById('openOptSymMktSizePage');

optSymMktSizeButton.onclick = () => {
  var page_url = chart_url + '/optSymMktSize';
  window.open(page_url, '_blank');
}


const sentimentButton = document.getElementById('sentimentPage');

sentimentButton.onclick = () => {
  var page_url = chart_url + '/mktSentiment';
  window.open(page_url, '_blank');
}