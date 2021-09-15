// Source: https://dev.to/nenadra/how-to-make-a-search-box-with-a-dropdown-in-javascript-4j9p

var ebq_searchbox = document.createElement("input");
ebq_searchbox.id = "input_searchbox";
ebq_searchbox.autocomplete = "off";
ebq_searchbox.setAttribute("onkeyup","searchEBQ(this);");
div_searchbox = document.getElementById('div_searchbox');
div_searchbox.appendChild(ebq_searchbox);

// Symbol exchange dict
var symExch = {};

async function readSymbolExchange() {
    endpoint = `http://${window.location.host}/api/symbol-exchange`;
    fetch(endpoint)
        .then(response => response.json())
        .then(resp_data => makeSymExchMap(resp_data));
}

function makeSymExchMap(data) {
    for (let i = 0; i < data.length; i++) {
        let exchange = data[i].exchange;
        let base_id = data[i].base_id;
        let quote_id = data[i].quote_id;
        let key = exchange + "::" + base_id + quote_id;
        symExch[key] = data[i];
    }
}

function searchEBQ(elem) {
    let ebq_selector = document.getElementById("ebq_selector");
    
    // Check if input is empty
    if (elem.value.trim() !== "") {
        elem.classList.add("dropdown"); // Add dropdown class (for the CSS border-radius)
        // If the selector div element does not exist, create it
        if (ebq_selector === null) {
            ebq_selector = document.createElement("div");
            ebq_selector.id = "ebq_selector";
            elem.parentNode.appendChild(ebq_selector);
            // Position it below the input element
            ebq_selector.style.left = elem.getBoundingClientRect().left + "px";
            ebq_selector.style.top = elem.getBoundingClientRect().bottom + "px";
            ebq_selector.style.width = elem.getBoundingClientRect().width + "px";
        }
        
        // Clear everything before new search
        ebq_selector.innerHTML = "";
        
        // Variable if result is empty
        let empty = true;
        for (let key in symExch) {
            let str_key = key.toLowerCase(); // Lowercase inputs and key
            let str_elem_value = elem.value.toLowerCase();
            if (str_key.indexOf(str_elem_value) !== -1) {
                let opt = document.createElement("button");
                opt.setAttribute("onclick","insertValue(this);")
                opt.innerHTML = key;
                ebq_selector.appendChild(opt);
                empty = false;
            }

        }
        // If result is empty, display a disabled button with text
        if (empty == true) {
            let opt = document.createElement("button");
            opt.disabled = true;
            opt.innerHTML = "No results";
            ebq_selector.appendChild(opt);
        }
    }

    // Remove selector element if input is empty
    else {
        if (ebq_selector !== null) {
            ebq_selector.parentNode.removeChild(ebq_selector);
            elem.classList.remove("dropdown");
        }
    }
}

function insertValue(elem) {
    let key = elem.innerHTML;
    ebq_searchbox.value = key;
    let ebq = symExch[key];
    ebq_searchbox.classList.remove("dropdown");
    elem.parentNode.parentNode.removeChild(elem.parentNode);
    resetAll();
    current_exchange = ebq.exchange;
    current_base_id = ebq.base_id;
    current_quote_id = ebq.quote_id;
    let time_switcher = document.getElementById('timerange_switcher');
    let first_time_switch = time_switcher.firstChild;
    syncToInterval(periods[0]);
    first_time_switch.click();
}

readSymbolExchange();
