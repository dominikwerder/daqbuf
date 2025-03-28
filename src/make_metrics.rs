use crate::log::log;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use syn::ext::IdentExt;
use syn::parse::ParseStream;

#[allow(unused)]
fn discard_rest(inp: ParseStream) {
    inp.step(|c| {
        let mut c = *c;
        loop {
            if let Some((x, c2)) = c.token_tree() {
                log(&format!("cursor token tree returned: {:?}", x));
                c = c2;
            } else {
                break;
            }
        }
        Ok(((), c))
    })
    .unwrap();
}

struct MetricsStructNameItem {}

impl syn::parse::Parse for MetricsStructNameItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let metric_name_item = inp.parse::<syn::ItemType>();
        log(&format!(
            "TRIED TO PARSE AN ITEM {:?}",
            metric_name_item.is_ok()
        ));
        let metric_name_item = metric_name_item?;
        match metric_name_item.ty.as_ref() {
            syn::Type::Path(x) => {
                log(&format!("Type Path {:?}", x.path.get_ident()));
                if let Some(id) = x.path.get_ident() {
                    id.to_string();
                }
            }
            _ => {
                log(&format!("ItemType unknown kind"));
            }
        }
        let ret = MetricsStructNameItem {};
        Ok(ret)
    }
}

struct MetricsModItem {}

impl syn::parse::Parse for MetricsModItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        log(&format!("MetricsModItem inp 1  {:?}", inp));
        let item = inp.parse::<syn::ItemMod>()?;
        log(&format!("MetricsModItem inp 2 {:?}", inp));
        if let Some(c) = item.content {
            for item in c.1 {
                match item {
                    syn::Item::Type(item) => {}
                    syn::Item::Mod(item) => {}
                    syn::Item::Enum(item) => {}
                    _ => todo!(),
                }
            }
        }
        let ret = MetricsModItem {};
        Ok(ret)
    }
}

#[derive(Debug)]
struct MetricsDecl {
    struct_name: String,
    value_names: Vec<String>,
    counter_names: Vec<String>,
}

impl syn::parse::Parse for MetricsDecl {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        log(&format!("INP IN PARSE {:?}", inp));
        let mut struct_name = Some(String::new());
        let mut value_names = Vec::new();
        let mut counter_names = Vec::new();
        let mut i = 0;
        loop {
            i += 1;
            if inp.is_empty() {
                break;
            }
            if i > 1000 {
                let e = inp.error("recursion limit");
                return Err(e);
            }

            let la1 = inp.lookahead1();
            if la1.peek(syn::Ident) {
                log("Lookahead was plain Ident");
            } else if la1.peek(syn::token::Enum) {
                let item = inp.parse::<syn::ItemEnum>()?;
                let s1 = item.ident.to_string();
                let vars = item.variants;
                for var in vars.iter() {
                    let s = var.ident.to_string();
                    log(&format!("have {:?} {:?}", s1, s));
                }
                if s1 == "values" {
                    for var in vars {
                        let s = var.ident.to_string();
                        value_names.push(s);
                    }
                } else if s1 == "counters" {
                    for var in vars {
                        let s = var.ident.to_string();
                        counter_names.push(s);
                    }
                } else {
                    let e = inp.error("unexpected enum kind");
                    return Err(e);
                }
                // discard_rest(inp);
            } else if true && la1.peek(syn::Token!(type)) {
                // alternative way of doing this
                let item = inp.parse::<syn::ItemType>()?;
                match item.ty.as_ref() {
                    syn::Type::Path(tp) => {
                        let s1 = item.ident.to_string();
                        let s2 = tp.path.get_ident().unwrap().to_string();
                        if s1 == "StructName" {
                            log(&format!("have {:?} {:?}", s1, s2));
                            struct_name = Some(s2);
                        } else {
                            let e = inp.error("unexpected type decl");
                            return Err(e);
                        }
                    }
                    _ => {
                        let e = inp.error("unexpected type decl");
                        return Err(e);
                    }
                }
            } else if la1.peek(syn::token::Type) {
                // could also peek on syn::Token!(type)
                log("Lookahead was Token type");
                let inp2 = inp.fork();
                let _ = inp2.parse::<syn::token::Type>()?;
                match inp2.parse::<syn::Ident>() {
                    Ok(x) => {
                        let s = x.to_string();
                        if s == "StructName" {
                            log("have StructName");
                            // discard_rest(inp);
                        } else {
                            let e = inp.error("unexpected type decl");
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                // let ty: syn::ItemType = inp.parse()?;
            } else if la1.peek(syn::token::Mod) {
                log("Lookahead was token Mod");
                inp.parse::<MetricsModItem>()?;
                // discard_rest(inp);
                log("==============   DONE  MetricsModItem");
            } else if la1.peek(syn::Ident::peek_any) {
                log("Lookahead was Any Ident");
                let inp2 = inp.fork();
                let ident: syn::Ident = inp2.parse()?;
                let ident_s = ident.to_string();
                if ident_s == "type" {
                    log("GOT A TYPE IDENT");
                } else {
                    log("GOT SOME OTHER IDENT");
                }
            } else {
                log("Lookahead was something else");
            }

            if false {
                if inp.fork().parse::<MetricsStructNameItem>().is_ok() {}
                let inp2 = inp.fork();
                let _ = inp2;
                let metric_name_item = inp.parse::<syn::ItemType>();
                log(&format!(
                    "TRIED TO PARSE AN ITEM {:?}",
                    metric_name_item.is_ok()
                ));
                let metric_name_item = metric_name_item?;
                match metric_name_item.ty.as_ref() {
                    syn::Type::Path(x) => {
                        log(&format!("Type Path {:?}", x.path.get_ident()));
                        if let Some(id) = x.path.get_ident() {
                            id.to_string();
                        }
                    }
                    _ => {
                        log(&format!("ItemType unknown kind"));
                    }
                }
            }
            if false {
                let cur1 = inp.cursor();
                if let Some((_tt, _cur2)) = cur1.token_tree() {
                } else {
                }
            }
        }
        let struct_name = struct_name.ok_or_else(|| inp.error("could not find StructName"))?;
        let ret = MetricsDecl {
            struct_name,
            value_names,
            counter_names,
        };
        Ok(ret)
    }
}

pub(super) fn make_metrics(ts: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ts2: TokenStream = ts.clone().into();
    // ParseBuffer::
    // TokenStream::parse(input)
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let ts3 = ts.clone();
    let rel_path = syn::parse_macro_input!(ts3 as syn::LitStr);
    let full_path = std::path::Path::new(&manifest_dir).join(rel_path.value());
    log("---------------------------------------------------------");
    log(&format!("read from {}", full_path.display()));
    let buf = std::fs::read(full_path).unwrap();
    let s = String::from_utf8_lossy(&buf);
    let ts4: TokenStream = syn::parse_str(&s).unwrap();
    log(&format!("ts4: {:?}", ts4));
    log(&format!("ts2: {:?}", ts2));
    log(&format!("ts: {:?}", ts));
    log(&format!("call_site {:?}", Span::call_site()));
    log(&manifest_dir);
    let ts5 = proc_macro::TokenStream::from(ts4);
    let _decl_file = syn::parse_macro_input!(ts5 as MetricsDecl);
    quote::quote! {}.into()
}
