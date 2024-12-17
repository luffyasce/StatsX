var storedData;

async function __single_calc__(contract, costRangeLow, costRangeHigh, aim, volatility, timeToExpiration, riskFreeRate) {
    var resp = await fetch('/target-watch-list/subscribe_target', {
        method: 'POST', 
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            'contract': contract,
            'costRangeLow': costRangeLow,
            'costRangeHigh': costRangeHigh,
            'aim': aim,
            'rate': riskFreeRate,
            'vol': volatility,
            'exp': timeToExpiration
        }), 
    });
    var data = await resp.json();
    return data
}

async function __calculate__(calculator, contract, current, costRangeLow, costRangeHigh, aim, volatility, timeToExpiration, riskFreeRate, ratio) {
    var otm_raw = (aim / current) - 1;

    var iv_implied_range = volatility / 15;
    var otmOverIVImpRange = (otm_raw / iv_implied_range).toFixed(2);

    var otm = (otm_raw * 100).toFixed(2);

    var outputArea = calculator.querySelector('.output');
    var subOutputs = outputArea.querySelectorAll('.sub-output');
    subOutputs.forEach(subOutput => {
        outputArea.removeChild(subOutput);
    });

    calculator.id = contract;

    var tte_ls = Array.from({length: 5}, (_, i) => timeToExpiration - i).filter(x => x > 0);
    for (var tte of tte_ls) {
        var data = await __single_calc__(contract, costRangeLow, costRangeHigh, aim, volatility, tte, riskFreeRate);
        var tLow = data['optLow']
        var tHigh = data['optHigh']
        var tTarget = data['optAim'];
    
        if (ratio) {
            var ratioThreshold = `(${(((tTarget - tLow) / (ratio + 1)) + tLow).toFixed(2)})`;
        } else {
            var ratioThreshold = '';
        }
        var subDiv = document.createElement('div');
        subDiv.className = 'sub-output';
        subDiv.innerHTML = `<p> ${contract.toUpperCase()} </p>` + 
        `<p> Expiry: ${tte} </p>` +
        `<p> OTM needed: ${otm}%  (~${otmOverIVImpRange}x IV indication)</p>` +
        `<p> Cost Range: ${tLow.toFixed(2)} ~ ${tHigh.toFixed(2)}${ratioThreshold} </p>` + 
        `<p> Target Price: ${tTarget.toFixed(2)} </p>`;
        outputArea.appendChild(subDiv);
    }
}

function calculate(event) {
    var calculator = event.target.parentElement;
    var contract = calculator.querySelector('[name="target-contract"]').value;
    var currentPrice = calculator.querySelector('[name="current-price"]').value;
    var costRangeLow = parseFloat(calculator.querySelector('[name="cost-range-low"]').value);
    var costRangeHigh = parseFloat(calculator.querySelector('[name="cost-range-high"]').value);
    var aim = parseFloat(calculator.querySelector('[name="aim-price"]').value);
    var volatility = parseFloat(calculator.querySelector('[name="volatility"]').value);
    var timeToExpiration = parseFloat(calculator.querySelector('[name="time-to-expiration"]').value);
    var riskFreeRate = parseFloat(calculator.querySelector('[name="risk-free-rate"]').value);
    var ratio = parseFloat(calculator.querySelector('[name="ratio"]').value);

    __calculate__(calculator, contract, currentPrice, costRangeLow, costRangeHigh, aim, volatility, timeToExpiration, riskFreeRate, ratio);

    params = {
        'currentPrice': currentPrice,
        'costRangeLow': costRangeLow,
        'costRangeHigh': costRangeHigh, 
        'aim': aim, 'volatility': volatility, 
        'timeToExpiration': timeToExpiration, 
        'riskFreeRate': riskFreeRate,
        'ratio': ratio
    }
    storedData[contract] = params;
    sessionStorage.setItem('watchListParams', JSON.stringify(storedData));
}


function addCalculator() {
    var existingCalculators = document.querySelectorAll('.calculator');
    var existingCalculator = existingCalculators[existingCalculators.length - 1];
    if (existingCalculator.id !== 'blank') {
        var newCalculator = existingCalculator.cloneNode(true);
        newCalculator.querySelector('.output').textContent = '';
        newCalculator.id = 'blank';
        newCalculator.querySelectorAll('input:not([name="risk-free-rate"])').forEach(input => input.value = '');
        document.getElementById('calculators').appendChild(newCalculator);
        return newCalculator;
    } else {
        return existingCalculator;
    }
}

function removeCalculator(event) {
    var existingCalculators = document.querySelectorAll('.calculator');
    var calculator = event.target.parentElement;
    var contract = calculator.id;

    if (existingCalculators.length > 1) {
        calculator.remove();
        if (storedData.hasOwnProperty(contract)) {
            delete storedData[contract];
            sessionStorage.setItem('watchListParams', JSON.stringify(storedData));
        }
    } else {
        calculator.querySelector('.output').textContent = '';
        calculator.id = 'blank';
        calculator.querySelectorAll('input:not([name="risk-free-rate"])').forEach(input => input.value = '');
        if (storedData.hasOwnProperty(contract)) {
            delete storedData[contract];
            sessionStorage.setItem('watchListParams', JSON.stringify(storedData));
        }
    }

    
}


window.addEventListener('load', function() {
    storedData = sessionStorage.getItem('watchListParams');
    if (storedData) {
        storedData = JSON.parse(storedData);
    } else {
        storedData = {};
    }

    for (var contract in storedData) {
        var calcArea = addCalculator();
        var params = storedData[contract];

        calcArea.querySelector('[name="target-contract"]').value = contract;
        calcArea.querySelector('[name="current-price"]').value = params['currentPrice'];
        calcArea.querySelector('[name="cost-range-low"]').value = params['costRangeLow'];
        calcArea.querySelector('[name="cost-range-high"]').value = params['costRangeHigh'];
        calcArea.querySelector('[name="aim-price"]').value = params['aim'];
        calcArea.querySelector('[name="volatility"]').value = params['volatility'];
        calcArea.querySelector('[name="time-to-expiration"]').value = params['timeToExpiration'];
        calcArea.querySelector('[name="risk-free-rate"]').value = params['riskFreeRate'];
        calcArea.querySelector('[name="ratio"]').value = params['ratio'];

        __calculate__(calcArea, contract, params['currentPrice'], params['costRangeLow'], params['costRangeHigh'], params['aim'], params['volatility'], params['timeToExpiration'], params['riskFreeRate'], params['ratio']);
    }
});