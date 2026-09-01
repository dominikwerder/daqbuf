use crate::attrs::ContainerAttrs;
use crate::attrs::ExtraField;
use crate::attrs::FieldAttrs;
use crate::attrs::FieldMode;
use crate::attrs::parse_container_attrs;
use crate::attrs::parse_field_attrs;
use crate::attrs::parse_variant_attrs;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use quote::format_ident;
use quote::quote;
use syn::spanned::Spanned;

pub fn expand(inp: syn::DeriveInput) -> syn::Result<TokenStream2> {
    let ca = parse_container_attrs(&inp.attrs)?;
    match &inp.data {
        syn::Data::Enum(x) => expand_enum(&inp, &ca, x),
        syn::Data::Struct(x) => expand_struct(&inp, &ca, x),
        syn::Data::Union(_) => Err(syn::Error::new(inp.span(), "ToSerde can not be derived for unions")),
    }
}

/// How one source field shows up in the generated snapshot type.
struct Mapped {
    ty: TokenStream2,
    expr: TokenStream2,
    serde_attrs: Vec<TokenStream2>,
    bound: Option<TokenStream2>,
}

/// `access` must evaluate to `&FieldTy`.
fn map_field(f: &syn::Field, fa: &FieldAttrs, access: &TokenStream2, cp: &syn::Path) -> Mapped {
    let fty = &f.ty;
    let mut serde_attrs: Vec<TokenStream2> = fa.serde.iter().map(|x| quote!(#x)).collect();
    let user_gave_serde = !serde_attrs.is_empty();
    let (ty, expr, bound) = match &fa.mode {
        FieldMode::Skip => unreachable!("skipped fields are filtered before map_field"),
        FieldMode::Clone => (
            quote!(#fty),
            quote!(::core::clone::Clone::clone(#access)),
            Some(quote!(#fty: ::core::clone::Clone + ::serde::Serialize)),
        ),
        FieldMode::Elapsed => {
            if !user_gave_serde {
                let p = path_str(&quote!(#cp::serde_Duration_human::serialize));
                serde_attrs.push(quote!(serialize_with = #p));
            }
            (quote!(::core::time::Duration), quote!((#access).elapsed()), None)
        }
        FieldMode::ElapsedMs => (
            quote!(u64),
            quote!({
                let d = (#access).elapsed();
                d.as_secs() * 1000 + d.subsec_millis() as u64
            }),
            None,
        ),
        FieldMode::Nest => (
            quote!(<#fty as #cp::to_serde::ToSerde>::Serde),
            quote!(#cp::to_serde::ToSerde::to_serde(#access)),
            Some(quote!(#fty: #cp::to_serde::ToSerde)),
        ),
        FieldMode::Len => (
            quote!(#cp::to_serde::LenCap),
            quote!(#cp::to_serde::HasLenCap::len_cap(#access)),
            Some(quote!(#fty: #cp::to_serde::HasLenCap)),
        ),
        FieldMode::With(p) => {
            let ty = fa.ty.as_ref().expect("checked in parse_field_attrs");
            (quote!(#ty), quote!(#p(#access)), None)
        }
    };
    // An explicit `ty = ..` always wins for the declared mirror type.
    let ty = match (&fa.mode, &fa.ty) {
        (FieldMode::With(_), _) => ty,
        (_, Some(t)) => quote!(#t),
        (_, None) => ty,
    };
    Mapped {
        ty,
        expr,
        serde_attrs,
        bound,
    }
}

fn path_str(ts: &TokenStream2) -> String {
    ts.to_string().replace(' ', "")
}

fn serde_attr(attrs: &[TokenStream2]) -> TokenStream2 {
    if attrs.is_empty() {
        quote!()
    } else {
        quote!(#[serde( #(#attrs),* )])
    }
}

/// `#[derive(Debug, Serialize, ..)]` plus any forwarded `#[serde(..)]` for the generated type.
fn head(ca: &ContainerAttrs) -> TokenStream2 {
    let extra_derives = &ca.derives;
    let serde = serde_attr(&ca.serde);
    quote! {
        #[derive(Debug, ::serde::Serialize #(, #extra_derives)*)]
        #[allow(dead_code)]
        #serde
    }
}

fn dst_ident(inp: &syn::DeriveInput, ca: &ContainerAttrs) -> syn::Ident {
    ca.name.clone().unwrap_or_else(|| format_ident!("{}Serde", inp.ident))
}

fn dst_vis(inp: &syn::DeriveInput, ca: &ContainerAttrs) -> syn::Visibility {
    ca.vis.clone().unwrap_or_else(|| inp.vis.clone())
}

/// Bounds are only useful when the input is generic; for concrete field types they would
/// just produce confusing diagnostics.
fn where_clause(inp: &syn::DeriveInput, bounds: Vec<TokenStream2>) -> TokenStream2 {
    let existing = inp.generics.where_clause.as_ref();
    if inp.generics.type_params().next().is_none() {
        return quote!(#existing);
    }
    match existing {
        Some(w) => {
            let preds = &w.predicates;
            quote!(where #preds #(, #bounds)*)
        }
        None => {
            if bounds.is_empty() {
                quote!()
            } else {
                quote!(where #(#bounds),*)
            }
        }
    }
}

fn expand_enum(inp: &syn::DeriveInput, ca: &ContainerAttrs, data: &syn::DataEnum) -> syn::Result<TokenStream2> {
    if let Some(e) = ca.extra.first() {
        return Err(syn::Error::new(
            e.name.span(),
            "container-level extra(..) is only supported on structs; \
             put extra(..) on the individual variants instead",
        ));
    }
    let cp = ca.crate_path();
    let src = &inp.ident;
    let dst = dst_ident(inp, ca);
    let vis = dst_vis(inp, ca);
    let head = head(ca);
    let (impl_g, ty_g, _) = inp.generics.split_for_impl();
    let generics = &inp.generics;

    let mut bounds = Vec::new();
    let mut var_defs = Vec::new();
    let mut arms = Vec::new();

    for v in &data.variants {
        let va = parse_variant_attrs(&v.attrs)?;
        let vname = &v.ident;
        let vserde = serde_attr(&va.serde);
        let extras = &va.extra;

        match &v.fields {
            syn::Fields::Named(fs) => {
                let binds: Vec<&syn::Ident> = fs
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().expect("named field"))
                    .collect();
                let mut defs = Vec::new();
                let mut inits = Vec::new();
                for f in &fs.named {
                    let fa = parse_field_attrs(f)?;
                    if let FieldMode::Skip = fa.mode {
                        continue;
                    }
                    let name = f.ident.as_ref().expect("named field");
                    let m = map_field(f, &fa, &quote!(#name), &cp);
                    collect_bound(&mut bounds, m.bound);
                    let (ty, expr, sa) = (m.ty, m.expr, serde_attr(&m.serde_attrs));
                    defs.push(quote!(#sa #name: #ty));
                    inits.push(quote!(#name: #expr));
                }
                for ExtraField { name, ty, expr } in extras {
                    defs.push(quote!(#name: #ty));
                    inits.push(quote!(#name: #expr));
                }
                var_defs.push(quote!(#vserde #vname { #(#defs),* }));
                arms.push(quote!(#src::#vname { #(#binds),* } => #dst::#vname { #(#inits),* }));
            }
            syn::Fields::Unnamed(fs) => {
                let binds: Vec<syn::Ident> = (0..fs.unnamed.len()).map(|i| format_ident!("f{}", i)).collect();
                let mut defs = Vec::new();
                let mut inits = Vec::new();
                for (f, b) in fs.unnamed.iter().zip(binds.iter()) {
                    let fa = parse_field_attrs(f)?;
                    if let FieldMode::Skip = fa.mode {
                        continue;
                    }
                    let m = map_field(f, &fa, &quote!(#b), &cp);
                    collect_bound(&mut bounds, m.bound);
                    let (ty, expr, sa) = (m.ty, m.expr, serde_attr(&m.serde_attrs));
                    defs.push(quote!(#sa #ty));
                    inits.push(expr);
                }
                for ExtraField { ty, expr, .. } in extras {
                    defs.push(quote!(#ty));
                    inits.push(quote!(#expr));
                }
                if defs.is_empty() {
                    var_defs.push(quote!(#vserde #vname));
                    arms.push(quote!(#src::#vname( #(#binds),* ) => #dst::#vname));
                } else {
                    var_defs.push(quote!(#vserde #vname( #(#defs),* )));
                    arms.push(quote!(#src::#vname( #(#binds),* ) => #dst::#vname( #(#inits),* )));
                }
            }
            syn::Fields::Unit => {
                if extras.is_empty() {
                    var_defs.push(quote!(#vserde #vname));
                    arms.push(quote!(#src::#vname => #dst::#vname));
                } else {
                    let defs: Vec<_> = extras.iter().map(|x| &x.ty).collect();
                    let inits: Vec<_> = extras.iter().map(|x| &x.expr).collect();
                    var_defs.push(quote!(#vserde #vname( #(#defs),* )));
                    arms.push(quote!(#src::#vname => #dst::#vname( #(#inits),* )));
                }
            }
        }
    }

    let where_c = where_clause(inp, bounds);
    let ts = quote! {
        #head
        #vis enum #dst #generics #where_c {
            #(#var_defs),*
        }

        impl #impl_g #cp::to_serde::ToSerde for #src #ty_g #where_c {
            type Serde = #dst #ty_g;

            #[allow(unused_variables)]
            fn to_serde(&self) -> Self::Serde {
                match self {
                    #(#arms),*
                }
            }
        }
    };
    Ok(ts)
}

fn expand_struct(inp: &syn::DeriveInput, ca: &ContainerAttrs, data: &syn::DataStruct) -> syn::Result<TokenStream2> {
    let cp = ca.crate_path();
    let src = &inp.ident;
    let dst = dst_ident(inp, ca);
    let vis = dst_vis(inp, ca);
    let head = head(ca);
    let (impl_g, ty_g, _) = inp.generics.split_for_impl();
    let generics = &inp.generics;

    let mut bounds = Vec::new();
    let mut defs = Vec::new();
    let mut inits = Vec::new();

    match &data.fields {
        syn::Fields::Named(fs) => {
            for f in &fs.named {
                let fa = parse_field_attrs(f)?;
                if let FieldMode::Skip = fa.mode {
                    continue;
                }
                let name = f.ident.as_ref().expect("named field");
                let m = map_field(f, &fa, &quote!(&self.#name), &cp);
                collect_bound(&mut bounds, m.bound);
                let (ty, expr, sa) = (m.ty, m.expr, serde_attr(&m.serde_attrs));
                defs.push(quote!(#sa pub #name: #ty));
                inits.push(quote!(#name: #expr));
            }
            for ExtraField { name, ty, expr } in &ca.extra {
                defs.push(quote!(pub #name: #ty));
                inits.push(quote!(#name: #expr));
            }
            let where_c = where_clause(inp, bounds);
            Ok(quote! {
                #head
                #vis struct #dst #generics #where_c {
                    #(#defs),*
                }

                impl #impl_g #cp::to_serde::ToSerde for #src #ty_g #where_c {
                    type Serde = #dst #ty_g;

                    fn to_serde(&self) -> Self::Serde {
                        #dst { #(#inits),* }
                    }
                }
            })
        }
        syn::Fields::Unnamed(fs) => {
            for (i, f) in fs.unnamed.iter().enumerate() {
                let fa = parse_field_attrs(f)?;
                if let FieldMode::Skip = fa.mode {
                    continue;
                }
                let idx = syn::Index::from(i);
                let m = map_field(f, &fa, &quote!(&self.#idx), &cp);
                collect_bound(&mut bounds, m.bound);
                let (ty, expr, sa) = (m.ty, m.expr, serde_attr(&m.serde_attrs));
                defs.push(quote!(#sa pub #ty));
                inits.push(expr);
            }
            for ExtraField { ty, expr, .. } in &ca.extra {
                defs.push(quote!(pub #ty));
                inits.push(quote!(#expr));
            }
            let where_c = where_clause(inp, bounds);
            Ok(quote! {
                #head
                #vis struct #dst #generics ( #(#defs),* ) #where_c;

                impl #impl_g #cp::to_serde::ToSerde for #src #ty_g #where_c {
                    type Serde = #dst #ty_g;

                    fn to_serde(&self) -> Self::Serde {
                        #dst( #(#inits),* )
                    }
                }
            })
        }
        syn::Fields::Unit => {
            for ExtraField { name, ty, expr } in &ca.extra {
                defs.push(quote!(pub #name: #ty));
                inits.push(quote!(#name: #expr));
            }
            let where_c = where_clause(inp, bounds);
            Ok(quote! {
                #head
                #vis struct #dst #generics #where_c {
                    #(#defs),*
                }

                impl #impl_g #cp::to_serde::ToSerde for #src #ty_g #where_c {
                    type Serde = #dst #ty_g;

                    fn to_serde(&self) -> Self::Serde {
                        #dst { #(#inits),* }
                    }
                }
            })
        }
    }
}

fn collect_bound(bounds: &mut Vec<TokenStream2>, b: Option<TokenStream2>) {
    if let Some(b) = b {
        let s = b.to_string();
        if !bounds.iter().any(|x| x.to_string() == s) {
            bounds.push(b);
        }
    }
}

#[allow(unused)]
fn dbg_span() -> Span {
    Span::call_site()
}

#[allow(unused)]
fn dbg_tokens(x: &impl ToTokens) -> String {
    x.to_token_stream().to_string()
}
