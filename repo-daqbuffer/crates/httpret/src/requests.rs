use httpclient::http::header;
use httpclient::http::header::HeaderMap;
use netpod::ACCEPT_ALL;
use netpod::APP_CBOR_FRAMED;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::APP_OCTET;

pub fn accepts_json_or_all(headers: &HeaderMap) -> bool {
    let h = get_accept_or(APP_JSON, headers);
    h.contains(APP_JSON) || h.contains(ACCEPT_ALL)
}

pub fn accepts_octets(headers: &HeaderMap) -> bool {
    get_accept_or("", headers).contains(APP_OCTET)
}

pub fn accepts_cbor_framed(headers: &HeaderMap) -> bool {
    get_accept_or("", headers).contains(APP_CBOR_FRAMED)
}

pub fn accepts_json_framed(headers: &HeaderMap) -> bool {
    get_accept_or("", headers).contains(APP_JSON_FRAMED)
}

fn get_accept_or<'a>(def: &'a str, headers: &'a HeaderMap) -> &'a str {
    headers.get(header::ACCEPT).map_or(def, |k| k.to_str().unwrap_or(def))
}
