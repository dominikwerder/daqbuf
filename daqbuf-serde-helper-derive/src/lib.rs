//! Derive macro for `daqbuf-serde-helper`'s `ToSerde` trait.
//!
//! Generates a serializable mirror type for a component whose real type can not be
//! serialized (futures, wakers, channel handles), plus the `ToSerde` impl that builds it.
//! Derived values which exist in no field are declared with `extra(name: Type = expr)` and
//! are evaluated inside `to_serde`, so a costly one is only paid for when a snapshot is taken.
//!
//! ```ignore
//! #[derive(Debug, ToSerde)]
//! #[to_serde(vis = "pub", serde(tag = "ty", content = "co"))]
//! enum State {
//!     Init(#[to_serde(elapsed)] Instant, #[to_serde(skip)] Init),
//!     Creating(#[to_serde(elapsed)] Instant, #[to_serde(nest)] Creating),
//!     CreateChanSend(
//!         #[to_serde(elapsed)] Instant,
//!         #[to_serde(len)] VecDeque<CaMsg>,
//!         #[to_serde(skip)] FutDbg<()>,
//!     ),
//! }
//! ```

mod attrs;
mod expand;

use proc_macro::TokenStream;

#[proc_macro_derive(ToSerde, attributes(to_serde))]
pub fn to_serde(ts: TokenStream) -> TokenStream {
    let inp = syn::parse_macro_input!(ts as syn::DeriveInput);
    match expand::expand(inp) {
        Ok(x) => x.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
