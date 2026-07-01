fn main() {
    let names = ["DEBUG", "OPT_LEVEL", "TARGET", "CARGO_ENCODED_RUSTFLAGS"];
    for name in names {
        let val = std::env::var(name).unwrap();
        println!("cargo:rustc-env=DAQBUF_{}={}", name, val);
    }
}
