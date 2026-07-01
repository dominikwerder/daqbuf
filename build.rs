fn main() {
    cc::Build::new()
        .file("src/bitshuffle.c")
        .file("src/bitshuffle_core.c")
        .file("src/iochain.c")
        .file("src/lz4.c")
        .include("src")
        .warnings(false)
        .compile("bitshufbundled");
}
