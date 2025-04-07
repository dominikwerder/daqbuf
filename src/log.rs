use proc_macro2::TokenStream;
use std::fs::File;
use std::io::Write;
use std::sync::Mutex;

static FILE: Mutex<Option<File>> = Mutex::new(None);

pub fn log(s: &str) {
    if true {
        return;
    }
    let mut mg = FILE.lock().unwrap();
    let mut fout = if let Some(x) = mg.take() {
        x
    } else {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("tmp-log-out.txt")
            .unwrap()
    };
    let mut buf = s.as_bytes().to_vec();
    buf.extend_from_slice(b"\n");
    fout.write(&buf).unwrap();
    *mg = Some(fout);
}

pub fn log_ts(ts: &TokenStream) {
    let fmtd = prettyplease::unparse(&syn::parse2::<syn::File>(ts.clone()).unwrap());
    log(&fmtd);
}
