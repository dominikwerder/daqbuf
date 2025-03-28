use proc_macro::TokenStream;
use quote::quote;
// use syn::Ident;
use syn::parse::ParseStream;
use syn::parse_macro_input;
use syn::spanned::Spanned;

type PunctExpr = syn::punctuated::Punctuated<syn::Expr, syn::token::Comma>;

fn ident_from_expr(inp: syn::Expr) -> syn::Result<syn::Ident> {
    use syn::spanned::Spanned;
    match inp {
        syn::Expr::Path(k) => {
            if k.path.segments.len() == 1 {
                Ok(k.path.segments[0].ident.clone())
            } else {
                Err(syn::Error::new(k.span(), "Expect identifier"))
            }
        }
        _ => Err(syn::Error::new(inp.span(), "Expect identifier")),
    }
}

fn idents_from_exprs(inp: PunctExpr) -> syn::Result<Vec<syn::Ident>> {
    let mut ret = Vec::new();
    for k in inp {
        let g = ident_from_expr(k)?;
        ret.push(g);
    }
    Ok(ret)
}

struct FuncCallWithArgs {
    name: syn::Ident,
    args: PunctExpr,
}

fn func_name_from_expr(inp: syn::Expr) -> syn::Result<syn::Ident> {
    use syn::Error;
    use syn::Expr;
    use syn::spanned::Spanned;
    match inp {
        Expr::Path(k) => {
            if k.path.segments.len() != 1 {
                return Err(Error::new(k.span(), "Expect function name"));
            }
            let res = k.path.segments[0].ident.clone();
            Ok(res)
        }
        _ => {
            return Err(Error::new(inp.span(), "Expect function name"));
        }
    }
}

impl FuncCallWithArgs {
    fn from_expr(inp: syn::Expr) -> Result<Self, syn::Error> {
        use syn::Error;
        use syn::Expr;
        use syn::spanned::Spanned;
        let span_all = inp.span();
        match inp {
            Expr::Call(k) => {
                let name = func_name_from_expr(*k.func)?;
                let args = k.args;
                let ret = FuncCallWithArgs { name, args };
                Ok(ret)
            }
            _ => {
                // TODO
                // return Err(Error::new(span_all, format!("BAD  {:?}", inp)));
                return Err(Error::new(span_all, format!("BAD  {:?}", "inp")));
            }
        }
    }
}

#[derive(Clone, Debug)]
struct StatsStructDef {
    name: syn::Ident,
    prefix: Option<syn::Ident>,
    counters: Vec<syn::Ident>,
    values: Vec<syn::Ident>,
    histolog2s: Vec<syn::Ident>,
}

impl StatsStructDef {
    fn empty() -> Self {
        Self {
            name: syn::parse_str("__empty").unwrap(),
            prefix: syn::parse_str("__empty").unwrap(),
            counters: Vec::new(),
            values: Vec::new(),
            histolog2s: Vec::new(),
        }
    }

    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut prefix = None;
        let mut counters = None;
        let mut values = None;
        let mut histolog2s = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            } else if fa.name == "prefix" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                prefix = Some(ident);
            } else if fa.name == "counters" {
                let idents = idents_from_exprs(fa.args)?;
                counters = Some(idents);
            } else if fa.name == "values" {
                let idents = idents_from_exprs(fa.args)?;
                values = Some(idents);
            } else if fa.name == "histolog2s" {
                let idents = idents_from_exprs(fa.args)?;
                histolog2s = Some(idents);
            } else {
                panic!("fa.name: {:?}", fa.name);
            }
        }
        let ret = StatsStructDef {
            name: name.expect("Expect name for StatsStructDef"),
            prefix,
            counters: counters.unwrap_or(Vec::new()),
            values: values.unwrap_or(Vec::new()),
            histolog2s: histolog2s.unwrap_or(Vec::new()),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct AggStructDef {
    name: syn::Ident,
    parent: syn::Ident,
    // TODO this currently describes our input (especially the input's name):
    stats: StatsStructDef,
}

impl AggStructDef {
    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut parent = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            }
            if fa.name == "parent" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                parent = Some(ident);
            }
        }
        let ret = AggStructDef {
            name: name.expect("Expect name for AggStructDef"),
            // Will get resolved later:
            stats: StatsStructDef::empty(),
            parent: parent.expect("Expect parent"),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct DiffStructDef {
    name: syn::Ident,
    input: syn::Ident,
}

impl DiffStructDef {
    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut input = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            }
            if fa.name == "input" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                input = Some(ident);
            }
        }
        let ret = DiffStructDef {
            name: name.expect("Expect name for DiffStructDef"),
            input: input.expect("Expect input for DiffStructDef"),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct StatsTreeDef {
    stats_struct_defs: Vec<StatsStructDef>,
    agg_defs: Vec<AggStructDef>,
    diff_defs: Vec<DiffStructDef>,
}

impl syn::parse::Parse for StatsTreeDef {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let expr = inp.parse::<syn::Expr>()?;
        let tuple = match expr {
            syn::Expr::Tuple(tuple) => tuple,
            _ => return Err(syn::Error::new(expr.span(), "Expected a tuple expression")),
        };
        let mut a = Vec::new();
        let mut agg_defs = Vec::new();
        let mut diff_defs = Vec::new();
        for k in tuple.elems {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "stats_struct" {
                let stats_struct_def = StatsStructDef::from_args(fa.args)?;
                a.push(stats_struct_def);
            } else if fa.name == "agg" {
                let agg_def = AggStructDef::from_args(fa.args)?;
                agg_defs.push(agg_def);
            } else if fa.name == "diff" {
                let diff_def = DiffStructDef::from_args(fa.args)?;
                diff_defs.push(diff_def);
            } else {
                return Err(syn::Error::new(fa.name.span(), "Unexpected"));
            }
        }
        let ret = StatsTreeDef {
            stats_struct_defs: a,
            agg_defs,
            diff_defs,
        };
        Ok(ret)
    }
}

fn stats_struct_decl_impl(st: &StatsStructDef) -> String {
    todo!()
}

fn agg_decl_impl(st: &StatsStructDef, ag: &AggStructDef) -> String {
    todo!()
}

fn diff_decl_impl(st: &DiffStructDef, inp: &StatsStructDef) -> String {
    todo!()
}

pub fn stats_struct(ts: TokenStream) -> TokenStream {
    let mut def: StatsTreeDef = parse_macro_input!(ts);
    for h in &mut def.agg_defs {
        for k in &def.stats_struct_defs {
            if k.name == h.parent {
                // TODO factor this out..
                h.stats = k.clone();
            }
        }
    }
    if false {
        for j in &def.agg_defs {
            let h = StatsStructDef {
                name: j.name.clone(),
                prefix: None,
                counters: j.stats.counters.clone(),
                values: Vec::new(),
                histolog2s: Vec::new(),
            };
            def.stats_struct_defs.push(h);
        }
    }
    let mut code = String::new();
    let mut ts1 = TokenStream::new();
    for k in &def.stats_struct_defs {
        let s = stats_struct_decl_impl(k);
        code.push_str(&s);
        let ts2: TokenStream = s.parse().unwrap();
        ts1.extend(ts2);
    }
    for k in &def.agg_defs {
        for st in &def.stats_struct_defs {
            if st.name == k.parent {
                let s = agg_decl_impl(st, k);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
    }
    for k in &def.diff_defs {
        for j in &def.agg_defs {
            if j.name == k.input {
                // TODO currently, "j.stats" describes the input to the "agg", so that contains the wrong name.
                let p = StatsStructDef {
                    name: k.input.clone(),
                    // TODO refactor
                    prefix: None,
                    counters: j.stats.counters.clone(),
                    // TODO compute values
                    values: Vec::new(),
                    // TODO not supported yet
                    histolog2s: Vec::new(),
                };
                let s = diff_decl_impl(k, &p);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
        for j in &def.stats_struct_defs {
            if j.name == k.input {
                let s = diff_decl_impl(k, j);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
    }
    //panic!("CODE: {}", code);
    let _ts3 = TokenStream::from(quote!(
        mod asd {}
    ));
    ts1
}
