use proc_macro::TokenStream;

mod gen1;

#[proc_macro]
pub fn stats_struct(ts: TokenStream) -> TokenStream {
    gen1::stats_struct(ts)
}
