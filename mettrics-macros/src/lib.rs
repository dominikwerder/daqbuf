use proc_macro::TokenStream;

mod gen1;
mod log;
mod make_metrics;
mod parse_decl;

#[proc_macro]
pub fn stats_struct(ts: TokenStream) -> TokenStream {
    gen1::stats_struct(ts)
}

#[proc_macro]
pub fn make_metrics(ts: TokenStream) -> TokenStream {
    make_metrics::make_metrics(ts)
}
