"use strict";

function load_status_main(ev) {
    const ts1 = Date.now();
    const dom_ev = ev;
    const b = ev.target;
    b.classList.remove("loaded");
    b.classList.add("loading");
    b.value = b.dataset.btnLabel + "";
    const query = {
        hosts: "",
    };
    const fetch_init = {
        method: "get",
        /*headers: {
          retrieval_instance: document.getElementById("retrieval_instance").value,
        },
        body: JSON.stringify(query),*/
    };
    fetch(g_config.api_base + "node_status", fetch_init)
        .then(x => Promise.all([x.json(), Date.now()]))
        .then(g_config.ui_delay_test)
        .then(g_config.ui_delay_blink)
        .then(kk => {
            const js = kk[0];
            const ts2 = kk[1];
            {
                let b = document.getElementById("load_status");
                b.innerHTML = "Loaded (" + (ts2 - ts1) + " ms)";
            }
            {
                let b = dom_ev.target;
                b.classList.remove("loading");
                b.classList.add("loaded");
                b.setAttribute("value", b.dataset.btnLabel);
            }
        });
}

function clear_element() {
    // different methods:
    response.querySelectorAll("*").forEach(n => n.remove());
    response.innerHTML = "";
    response.replaceChildren();
    response.replaceChild();
}

function sort_default(hosts) {
    hosts.sort((a, b) => {
        if (a.inst < b.inst) return -1;
        if (a.inst > b.inst) return +1;
        if (a.host < b.host) return -1;
        if (a.host > b.host) return +1;
    });
}

function show_json_response(js) {
    const response = document.getElementById("response");
    response.textContent = "";
    while (response.firstChild) {
        response.removeChild(response.lastChild);
        //response.lastChild.remove();
    }
    //JSON.stringify(js, null, 2);
    //for (let machine of js) {
    //  console.log(typeof(machine));
    //}
    //const dat2 = js.hosts;
    //sort_default(dat2);
    response.textContent = JSON.stringify(js, null, 2);
}

function clear_cache_all(ev) {
    const ts1 = Date.now();
    const dom_ev = ev;
    const b = ev.target;
    b.classList.remove("loaded");
    b.classList.add("loading");
    b.value = b.dataset.btnLabel + " (loading)";
    const body = {
        hosts: "",
    };
    const fetch_init = {
        method: "get",
        /*headers: {
          retrieval_instance: document.getElementById("retrieval_instance").value,
        },*/
        /*body: JSON.stringify(body),*/
    };
    fetch(g_config.api_base + "gather/clear_cache", fetch_init)
        .then(x => Promise.all([x.json(), Date.now()]))
        .then(g_config.ui_delay_test)
        .then(g_config.ui_delay_blink)
        .then(kk => {
            const js = kk[0];
            show_json_response(js);
            const ts2 = kk[1];
            {
                let b = document.getElementById("load_status");
                b.innerHTML = "Loaded (" + (ts2 - ts1) + " ms)";
            }
            {
                let b = dom_ev.target;
                b.classList.remove("loading");
                b.classList.add("loaded");
                b.setAttribute("value", b.dataset.btnLabel);
            }
        });
}

const g_config = {
    api_base: "/api/4/",
    ui_delay_test: x => x,
    ui_delay_blink: x => new Promise(resolve => setTimeout(() => resolve(x), 50)),
    //ui_delay_blink: x => x,
};

function init_for_dev() {
    g_config.api_base = "http://dev-retrieval-rs:8059/api/4/";
}

function init() {
    {
        const u = document.location.href;
        const a = u.match("^(.+)/[^/]+/[^/]+$");
        if (a.length === 2) {
            g_config.api_base = a[1] + "/";
        }
    }
    // keydown event..
    document.getElementById("btn_clear_cache").addEventListener("click", clear_cache_all)
    if (document.location.href.includes("dev-retrieval-rs")) {
        init_for_dev();
    }
    console.info("api_base: ", g_config.api_base);
}

window.addEventListener("load", ev => {
    init();
    const init_load_ele = document.getElementById("none");
    if (init_load_ele != null) {
        init_load_ele.click();
    }
});
