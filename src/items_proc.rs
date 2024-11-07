use proc_macro::{TokenStream, TokenTree};

const TYS: [&str; 10] = ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64"];
const IDS: [&str; 10] = ["U8", "U16", "U32", "U64", "I8", "I16", "I32", "I64", "F32", "F64"];

#[proc_macro]
pub fn make_answer(_item: TokenStream) -> TokenStream {
    "fn answer() -> u32 { 42 }".parse().unwrap()
}

#[proc_macro]
pub fn tycases1(ts: TokenStream) -> TokenStream {
    for tt in ts.clone() {
        match tt {
            TokenTree::Group(..) => (),
            TokenTree::Ident(..) => (),
            TokenTree::Punct(..) => (),
            TokenTree::Literal(..) => (),
        }
    }
    let tokens: Vec<_> = ts.clone().into_iter().collect();
    let match_val = if let TokenTree::Ident(x) = tokens[0].clone() {
        //panic!("GOT {}", x.to_string());
        x.to_string()
    } else {
        panic!("match_val")
    };
    let enum_1_pre = if let TokenTree::Ident(x) = tokens[2].clone() {
        //panic!("GOT {}", x.to_string());
        x.to_string()
    } else {
        panic!("enum_1_pre")
    };
    let enum_1_suff = tokens[4].to_string();
    let rhs = if let TokenTree::Group(x) = tokens[6].clone() {
        //panic!("GOT {}", x.to_string());
        x.to_string()
    } else {
        panic!("RHS mismatch {:?}", tokens[6])
    };
    //panic!("{:?}", tokens[0]);
    let tys = ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64"];
    let ids = ["U8", "U16", "U32", "U64", "I8", "I16", "I32", "I64", "F32", "F64"];
    let mut arms = vec![];
    for (id, ty) in ids.iter().zip(&tys) {
        let rhs = rhs.replace("$id", id);
        let rhs = rhs.replace("$ty", ty);
        let s = format!("{}::{}{} => {},", enum_1_pre, id, enum_1_suff, rhs);
        arms.push(s);
    }
    arms.push(format!("{}::{}{} => {}", enum_1_pre, "String", enum_1_suff, "todo!()"));
    let gen = format!("match {} {{\n{}\n}}", match_val, arms.join("\n"));
    //panic!("GENERATED: {}", gen);
    gen.parse().unwrap()
}

#[proc_macro]
pub fn enumvars(ts: TokenStream) -> TokenStream {
    let tokens: Vec<_> = ts.clone().into_iter().collect();
    let name = if let TokenTree::Ident(x) = tokens[0].clone() {
        x.to_string()
    } else {
        panic!("name")
    };
    let rhe = if let TokenTree::Ident(x) = tokens[2].clone() {
        x.to_string()
    } else {
        panic!("rhe")
    };
    let mut cases = vec![];
    for (id, ty) in IDS.iter().zip(&TYS) {
        let s = format!("{}({}<{}>),", id, rhe, ty);
        cases.push(s);
    }
    let gen = format!(
        "#[derive(Debug, Serialize, Deserialize)]\npub enum {} {{\n{}\n}}\n",
        name,
        cases.join("\n")
    );
    //panic!("GENERATED: {}", gen);
    gen.parse().unwrap()
}

#[proc_macro]
pub fn enumvariants(ts: TokenStream) -> TokenStream {
    //panic!("yoooo");
    //syn::parse_macro_input!(ts as syn::DeriveInput);
    //let tokens: Vec<_> = ts.into_iter().collect();
    //let parsed: syn::DeriveInput = syn::parse_macro_input!(ts as syn::DeriveInput);
    //let s = ts.to_string();
    let parsed = syn::parse::<syn::Item>(ts);
    //panic!("{:?}", parsed);
    match parsed {
        Ok(_ast) => {}
        Err(e) => panic!("Parse error {e:?}"),
    }
    TokenStream::new()
}
