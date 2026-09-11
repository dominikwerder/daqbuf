use proc_macro2::TokenStream as TokenStream2;
use syn::Token;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

/// A field which does not exist on the source type and is computed on snapshot.
///
/// Syntax: `name: Type = expr`
pub struct ExtraField {
    pub name: syn::Ident,
    pub ty: syn::Type,
    pub expr: syn::Expr,
}

impl Parse for ExtraField {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let name: syn::Ident = inp.parse()?;
        inp.parse::<Token![:]>()?;
        let ty: syn::Type = inp.parse()?;
        inp.parse::<Token![=]>()?;
        let expr: syn::Expr = inp.parse()?;
        Ok(Self { name, ty, expr })
    }
}

/// How long a state machine is typically expected to dwell in one state, used to derive a
/// `elapsed / typical` warn score sibling to an `elapsed`/`elapsed_ms` field.
///
/// `Ms(expr)` is a convenience form for a millisecond literal; `Expr(expr)` is interpreted
/// differently depending on where it appears (see `dwell_variant_arm` and
/// `dwell_field_score_block` in `expand.rs`): on a variant it must yield a `Duration`, on a
/// field it must already yield an `Option<Duration>` (typically a call into another type's
/// derive-generated `dwell_typical`).
pub enum DwellSpec {
    Ms(syn::Expr),
    Expr(syn::Expr),
}

fn parse_dwell_opt(inp: ParseStream, key: &syn::Ident) -> syn::Result<DwellSpec> {
    inp.parse::<Token![=]>()?;
    match key.to_string().as_str() {
        "dwell_ms" => Ok(DwellSpec::Ms(inp.parse()?)),
        "dwell" => Ok(DwellSpec::Expr(inp.parse()?)),
        _ => unreachable!(),
    }
}

#[derive(Default)]
pub struct ContainerAttrs {
    pub name: Option<syn::Ident>,
    pub vis: Option<syn::Visibility>,
    pub crate_path: Option<syn::Path>,
    pub serde: Vec<TokenStream2>,
    pub derives: Vec<syn::Path>,
    pub extra: Vec<ExtraField>,
    /// Enum-only: the context type threaded into the generated `dwell_typical(&self, ctx: &_)`
    /// when a variant's dwell time is not a compile-time constant (see `DwellSpec`).
    pub dwell_ctx: Option<syn::Type>,
}

impl ContainerAttrs {
    pub fn crate_path(&self) -> syn::Path {
        self.crate_path
            .clone()
            .unwrap_or_else(|| syn::parse_quote!(serde_helper))
    }
}

pub fn parse_container_attrs(attrs: &[syn::Attribute]) -> syn::Result<ContainerAttrs> {
    let mut out = ContainerAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("to_serde") {
            continue;
        }
        attr.parse_args_with(|inp: ParseStream| {
            while !inp.is_empty() {
                parse_container_opt(inp, &mut out)?;
                if inp.is_empty() {
                    break;
                }
                inp.parse::<Token![,]>()?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}

fn parse_container_opt(inp: ParseStream, out: &mut ContainerAttrs) -> syn::Result<()> {
    if inp.peek(Token![crate]) {
        inp.parse::<Token![crate]>()?;
        inp.parse::<Token![=]>()?;
        out.crate_path = Some(inp.parse()?);
        return Ok(());
    }
    let key: syn::Ident = inp.parse()?;
    match key.to_string().as_str() {
        "name" => {
            inp.parse::<Token![=]>()?;
            out.name = Some(inp.parse()?);
        }
        "vis" => {
            inp.parse::<Token![=]>()?;
            let s: syn::LitStr = inp.parse()?;
            out.vis = Some(s.parse()?);
        }
        "serde" => {
            let content;
            syn::parenthesized!(content in inp);
            out.serde.push(content.parse()?);
        }
        "derive" => {
            let content;
            syn::parenthesized!(content in inp);
            let p = Punctuated::<syn::Path, Token![,]>::parse_terminated(&content)?;
            out.derives.extend(p);
        }
        "extra" => {
            let content;
            syn::parenthesized!(content in inp);
            out.extra.push(content.parse()?);
        }
        "dwell_ctx" => {
            inp.parse::<Token![=]>()?;
            out.dwell_ctx = Some(inp.parse()?);
        }
        _ => {
            return Err(syn::Error::new(
                key.span(),
                "unknown to_serde option, expected one of: \
                 name, vis, crate, serde, derive, extra, dwell_ctx",
            ));
        }
    }
    Ok(())
}

pub enum FieldMode {
    /// Clone the field into the snapshot.
    Clone,
    Skip,
    /// `Instant` -> `Duration` since entering the state.
    Elapsed,
    /// `Instant` -> milliseconds since entering the state.
    ElapsedMs,
    /// Recurse into a child component via `ToSerde`.
    Nest,
    /// Container -> `LenCap`.
    Len,
    /// Arbitrary `fn(&T) -> U`; requires `ty = U`.
    With(syn::Path),
}

pub struct FieldAttrs {
    pub mode: FieldMode,
    pub ty: Option<syn::Type>,
    pub serde: Vec<TokenStream2>,
    /// Forwarded verbatim as `#[schema(..)]` on the generated field, for utoipa's `ToSchema`
    /// derive when the field's mirror type needs an explicit `value_type` override.
    pub schema: Vec<TokenStream2>,
    pub span: proc_macro2::Span,
    /// Only valid on `elapsed`/`elapsed_ms` fields. Produces an `Option<u32>` sibling
    /// (`elapsed / typical` in per-mille, `1000` == a ratio of `1.0`) right after this field.
    /// `Ms` is a self-contained constant;
    /// `Expr` must already evaluate to `Option<Duration>` (typically a call into a nested
    /// component's derive-generated `dwell_typical`, e.g. `self.state.dwell_typical(&self.interval)`).
    pub dwell: Option<DwellSpec>,
}

pub fn parse_field_attrs(field: &syn::Field) -> syn::Result<FieldAttrs> {
    let mut mode = None;
    let mut out = FieldAttrs {
        mode: FieldMode::Clone,
        ty: None,
        serde: Vec::new(),
        schema: Vec::new(),
        span: field.span(),
        dwell: None,
    };
    for attr in &field.attrs {
        if !attr.path().is_ident("to_serde") {
            continue;
        }
        attr.parse_args_with(|inp: ParseStream| {
            while !inp.is_empty() {
                let key: syn::Ident = inp.parse()?;
                let mut set = |m: FieldMode| -> syn::Result<()> {
                    if mode.is_some() {
                        return Err(syn::Error::new(key.span(), "conflicting to_serde field mode"));
                    }
                    mode = Some(m);
                    Ok(())
                };
                match key.to_string().as_str() {
                    "skip" => set(FieldMode::Skip)?,
                    "elapsed" => set(FieldMode::Elapsed)?,
                    "elapsed_ms" => set(FieldMode::ElapsedMs)?,
                    "nest" => set(FieldMode::Nest)?,
                    "len" => set(FieldMode::Len)?,
                    "with" => {
                        inp.parse::<Token![=]>()?;
                        let p: syn::Path = inp.parse()?;
                        set(FieldMode::With(p))?
                    }
                    "ty" => {
                        inp.parse::<Token![=]>()?;
                        out.ty = Some(inp.parse()?);
                    }
                    "serde" => {
                        let content;
                        syn::parenthesized!(content in inp);
                        out.serde.push(content.parse()?);
                    }
                    "schema" => {
                        let content;
                        syn::parenthesized!(content in inp);
                        out.schema.push(content.parse()?);
                    }
                    "dwell_ms" | "dwell" => {
                        if out.dwell.is_some() {
                            return Err(syn::Error::new(key.span(), "conflicting to_serde dwell spec"));
                        }
                        out.dwell = Some(parse_dwell_opt(inp, &key)?);
                    }
                    _ => {
                        return Err(syn::Error::new(
                            key.span(),
                            "unknown to_serde field option, expected one of: \
                             skip, elapsed, elapsed_ms, nest, len, with, ty, serde, schema, dwell_ms, dwell",
                        ));
                    }
                }
                if inp.is_empty() {
                    break;
                }
                inp.parse::<Token![,]>()?;
            }
            Ok(())
        })?;
    }
    if let Some(m) = mode {
        out.mode = m;
    }
    if matches!(&out.mode, FieldMode::With(_)) && out.ty.is_none() {
        return Err(syn::Error::new(
            out.span,
            "to_serde(with = ..) also needs ty = <MirrorType>",
        ));
    }
    if out.dwell.is_some() && !matches!(&out.mode, FieldMode::Elapsed | FieldMode::ElapsedMs) {
        return Err(syn::Error::new(
            out.span,
            "to_serde(dwell_ms/dwell = ..) is only valid alongside elapsed or elapsed_ms",
        ));
    }
    Ok(out)
}

#[derive(Default)]
pub struct VariantAttrs {
    pub serde: Vec<TokenStream2>,
    /// Forwarded verbatim as `#[schema(..)]` on the generated variant. utoipa only accepts a
    /// `value_type` override for a single-field tuple variant here, at the variant level, not
    /// on the field itself.
    pub schema: Vec<TokenStream2>,
    pub extra: Vec<ExtraField>,
    /// This variant's typical time-in-state, used to build the enum's `dwell_typical`.
    /// Absent means "no dwell expectation for this variant" (`dwell_typical` returns `None`).
    pub dwell: Option<DwellSpec>,
}

pub fn parse_variant_attrs(attrs: &[syn::Attribute]) -> syn::Result<VariantAttrs> {
    let mut out = VariantAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("to_serde") {
            continue;
        }
        attr.parse_args_with(|inp: ParseStream| {
            while !inp.is_empty() {
                let key: syn::Ident = inp.parse()?;
                match key.to_string().as_str() {
                    "serde" => {
                        let content;
                        syn::parenthesized!(content in inp);
                        out.serde.push(content.parse()?);
                    }
                    "schema" => {
                        let content;
                        syn::parenthesized!(content in inp);
                        out.schema.push(content.parse()?);
                    }
                    "extra" => {
                        let content;
                        syn::parenthesized!(content in inp);
                        out.extra.push(content.parse()?);
                    }
                    "dwell_ms" | "dwell" => {
                        if out.dwell.is_some() {
                            return Err(syn::Error::new(key.span(), "conflicting to_serde dwell spec"));
                        }
                        out.dwell = Some(parse_dwell_opt(inp, &key)?);
                    }
                    _ => {
                        return Err(syn::Error::new(
                            key.span(),
                            "unknown to_serde variant option, expected one of: \
                             serde, schema, extra, dwell_ms, dwell",
                        ));
                    }
                }
                if inp.is_empty() {
                    break;
                }
                inp.parse::<Token![,]>()?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}
