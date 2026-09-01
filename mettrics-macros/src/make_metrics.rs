mod codegen;
mod resolve;

use crate::log::log;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use std::fmt;
use syn::ext::IdentExt;
use syn::parse::ParseStream;

#[allow(unused)]
#[derive(Debug)]
pub enum Error {
    Deser,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

#[allow(non_snake_case)]
mod serde_syn_Path {
    use std::fmt;

    pub fn serialize<S>(val: &syn::Path, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = quote::quote! { #val }.to_string();
        ser.serialize_str(&s)
    }

    struct Vis;

    impl<'a> serde::de::Visitor<'a> for Vis {
        type Value = syn::Path;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "expect a path")
        }

        fn visit_str<E>(self, inp: &str) -> Result<Self::Value, E> {
            let val = syn::parse_str::<syn::Path>(inp).expect("a syn parseable path");
            Ok(val)
        }
    }

    pub fn deserialize<'a, D>(de: D) -> Result<syn::Path, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        de.deserialize_str(Vis)
    }
}

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
        log(&format!("TRIED TO PARSE AN ITEM {:?}", metric_name_item.is_ok()));
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ComposeAggModItem {
    input: String,
    aggtor: String,
    name: String,
}

impl syn::parse::Parse for ComposeAggModItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let mut input = None;
        let mut aggtor = None;
        let mut name = None;
        let item = inp.parse::<syn::ItemMod>()?;
        if let Some(c) = item.content {
            for item in c.1 {
                match item {
                    syn::Item::Type(item) => {
                        if item.ident.to_string() == "Input" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    input = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else if item.ident.to_string() == "Aggtor" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    aggtor = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else if item.ident.to_string() == "Name" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    name = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else {
                            let e = inp.error(format!("expect a type `Input`, `Aggtor` or `Name`"));
                            return Err(e);
                        }
                    }
                    _ => {
                        let e = inp.error(format!("expect a type"));
                        return Err(e);
                    }
                }
            }
        }
        let ret = Self {
            input: input.expect("type Input"),
            aggtor: aggtor.expect("type Aggtor"),
            name: name.expect("type Name"),
        };
        Ok(ret)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AggregationModItem {
    struct_name: String,
    input: String,
    compose_agg_mods: Vec<ComposeAggModItem>,
}

impl syn::parse::Parse for AggregationModItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let mut struct_name = None;
        let mut input = None;
        let mut compose_agg_mods = Vec::new();
        log(&format!("AggregationModItem inp 1  {:?}", inp));
        let item = inp.parse::<syn::ItemMod>()?;
        log(&format!("AggregationModItem inp 2 {:?}", inp));
        if let Some(c) = item.content {
            for item in c.1 {
                match item {
                    syn::Item::Type(item) => {
                        if item.ident.to_string() == "StructName" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    struct_name = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else if item.ident.to_string() == "Input" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    input = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else {
                            let e = inp.error(format!("expect a type `StructName`"));
                            return Err(e);
                        }
                    }
                    syn::Item::Mod(im) => {
                        let x = syn::Item::Mod(im);
                        let ts3 = quote::quote! { #x };
                        let x = syn::parse::Parser::parse(|inp: ParseStream| inp.parse(), ts3.into())?;
                        compose_agg_mods.push(x);
                    }
                    _ => {
                        let e = inp.error(format!("expect a type"));
                        return Err(e);
                    }
                }
            }
        }
        let ret = Self {
            struct_name: struct_name.expect("type StructName"),
            input: input.expect("type Input"),
            compose_agg_mods,
        };
        Ok(ret)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ComposeModItem {
    #[serde(with = "serde_syn_Path")]
    input: syn::Path,
    name: String,
}

impl fmt::Debug for ComposeModItem {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let input = &self.input;
        let input = quote::quote! { #input }.to_string();
        fmt.debug_struct("ComposeModItem")
            .field("input", &input)
            .field("name", &self.name)
            .finish()
    }
}

impl syn::parse::Parse for ComposeModItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let mut input = None;
        let mut name = None;
        log(&format!("ComposeModItem inp 1  {:?}", inp));
        let item = inp.parse::<syn::ItemMod>()?;
        log(&format!("ComposeModItem inp 2 {:?}", inp));
        if let Some(c) = item.content {
            for item in c.1 {
                match item {
                    syn::Item::Type(item) => {
                        if item.ident.to_string() == "Input" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    input = Some(tp.path.clone());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else if item.ident.to_string() == "Name" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    name = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else {
                            let e = inp.error(format!("expect a type `StructName`"));
                            return Err(e);
                        }
                    }
                    _ => {
                        let e = inp.error(format!("expect a type"));
                        return Err(e);
                    }
                }
            }
        }
        let ret = Self {
            input: input.expect("type Input"),
            name: name.expect("type Name"),
        };
        Ok(ret)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MetricsModItem {
    struct_name: String,
    counter_names: Vec<String>,
    value_names: Vec<String>,
    histolog2_names: Vec<String>,
    compose_mods: Vec<ComposeModItem>,
}

impl MetricsModItem {}

impl syn::parse::Parse for MetricsModItem {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let mut struct_name = None;
        let mut counter_names = Vec::new();
        let mut value_names = Vec::new();
        let mut histolog2_names = Vec::new();
        let mut compose_mods = Vec::new();
        log(&format!("MetricsModItem inp 1  {:?}", inp));
        let item = inp.parse::<syn::ItemMod>()?;
        log(&format!("MetricsModItem inp 2 {:?}", inp));
        if let Some(c) = item.content {
            for item in c.1 {
                match item {
                    syn::Item::Type(item) => {
                        if item.ident.to_string() == "StructName" {
                            match item.ty.as_ref() {
                                syn::Type::Path(tp) => {
                                    struct_name = Some(tp.path.get_ident().expect("path-ident").to_string());
                                }
                                _ => {
                                    let e = inp.error(format!("expect a path type"));
                                    return Err(e);
                                }
                            }
                        } else {
                            let e = inp.error(format!("expect a type `StructName`"));
                            return Err(e);
                        }
                    }
                    syn::Item::Mod(item) => {
                        let idn = item.ident.to_string();
                        if idn == "Compose" {
                            let item = syn::Item::Mod(item);
                            let ts3 = quote::quote! { #item };
                            let x = syn::parse::Parser::parse(|inp: ParseStream| inp.parse(), ts3.into())?;
                            log("==============   DONE  ComposeModItem");
                            compose_mods.push(x);
                        } else {
                            let e = inp.error(format!("expect mod `Compose`"));
                            return Err(e);
                        }
                    }
                    syn::Item::Enum(item) => {
                        let idn = item.ident.to_string();
                        let vars = item.variants;
                        if idn == "counters" {
                            for var in vars {
                                let s = var.ident.to_string();
                                counter_names.push(s);
                            }
                        } else if idn == "values" {
                            for var in vars {
                                let s = var.ident.to_string();
                                value_names.push(s);
                            }
                        } else if idn == "histolog2s" {
                            for var in vars {
                                let s = var.ident.to_string();
                                histolog2_names.push(s);
                            }
                        } else {
                            let e = inp.error(format!("expect enum `counters`, `values` or `histolog2s`"));
                            return Err(e);
                        }
                    }
                    _ => {
                        let e = inp.error(format!("expect a type, mod or enum"));
                        return Err(e);
                    }
                }
            }
        }
        let ret = Self {
            struct_name: struct_name.expect("type StructName"),
            counter_names,
            value_names,
            histolog2_names,
            compose_mods,
        };
        Ok(ret)
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MetricsDecl {
    metrics_mods: Vec<MetricsModItem>,
    agg_mods: Vec<AggregationModItem>,
}

impl MetricsDecl {
    fn to_inspect(&self) -> String {
        format!("{:?}", self)
    }
}

impl syn::parse::Parse for MetricsDecl {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        log(&format!("INP IN PARSE {:?}", inp));
        let mut metrics_mods = Vec::new();
        let mut aggs_mods = Vec::new();
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
                if s1 == "counters" {
                    for var in vars {
                        let _s = var.ident.to_string();
                        // counter_names.push(s);
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
                        let s2 = tp.path.get_ident().expect("path-ident").to_string();
                        if s1 == "StructName" {
                            log(&format!("have {:?} {:?}", s1, s2));
                            // struct_name = Some(s2);
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
            } else if la1.peek(syn::token::Mod) {
                log("Lookahead was token Mod");
                let inp2 = inp.fork();
                let _ = inp2.parse::<syn::token::Mod>()?;
                let x = inp2.parse::<syn::Ident>()?;
                let s1 = x.to_string();
                if s1 == "Metrics" {
                    let x = inp.parse::<MetricsModItem>()?;
                    log("==============   DONE  MetricsModItem");
                    metrics_mods.push(x);
                } else if s1 == "Aggregation" {
                    let x = inp.parse::<AggregationModItem>()?;
                    log("==============   DONE  AggregationModItem");
                    aggs_mods.push(x);
                } else {
                    let e = inp.error("expect mod `Metrics` or `Aggregation`");
                    return Err(e);
                }
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
                log(&format!("TRIED TO PARSE AN ITEM {:?}", metric_name_item.is_ok()));
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
        let ret = Self {
            metrics_mods,
            agg_mods: aggs_mods,
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
    let mut decls_file = syn::parse_macro_input!(ts5 as MetricsDecl);
    decls_file.resolve();
    let decls_file = decls_file;
    let s1 = decls_file.to_inspect();
    let ts_out = decls_file.to_code().unwrap();
    let fmtd = prettyplease::unparse(&syn::parse2::<syn::File>(ts_out.clone()).unwrap());
    let s2 = fmtd;
    quote::quote! {
        pub fn tmp_metrics_s1() -> String {
            let ret = #s1;
            ret.into()
        }
        pub fn tmp_metrics_s2() -> String {
            let ret = #s2;
            ret.into()
        }
        #ts_out
    }
    .into()
}
