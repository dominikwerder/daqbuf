use super::AggregationModItem;
use super::MetricsDecl;
use super::MetricsModItem;
use crate::log::log;
use crate::log::log_ts;
use proc_macro2::Span;
use proc_macro2::TokenStream;

impl MetricsDecl {
    fn agg_from_metrics_token_stream(
        &self,
        agg: &AggregationModItem,
        inp: &MetricsModItem,
    ) -> syn::Result<TokenStream> {
        let struct_name = syn::Ident::new(&agg.struct_name, Span::call_site());
        let inp_struct_name = syn::Ident::new(&inp.struct_name, Span::call_site());
        let (fields_counters_decl, fields_counters_init, ingest_counters) = {
            let fields_decl = inp
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32, });
            let fields_init = inp
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32::new(), });
            let ingest_counters = inp
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { self.#x.ingest(inp.#x); });
            (fields_decl, fields_init, ingest_counters)
        };

        let (fields_compose_decl, fields_compose_init, ingest_compose) = {
            let mut fields_decl = Vec::new();
            let mut fields_init = Vec::new();
            let mut ingest = Vec::new();
            for cm1 in inp.compose_mods.iter() {
                log(&format!(
                    "agg_from_metrics_token_stream  mod Compose  cm {:?}",
                    cm1
                ));
                let mut found = false;
                for cm2 in agg.compose_agg_mods.iter() {
                    if cm2.input == cm1.name {
                        if found {
                            let e = syn::Error::new(
                                Span::call_site(),
                                format!(
                                    "found composition method for input Compose again  {:?}  {:?}",
                                    cm1, cm2
                                ),
                            );
                            return Err(e);
                        }
                        log(&format!(
                            "agg_from_metrics_token_stream  found pair  cm1 {:?}  cm2 {:?}",
                            cm1, cm2
                        ));
                        // TODO difference?
                        let _name1 = quote::format_ident!("{}", cm1.name);
                        let name1 = syn::Ident::new(&cm1.name, Span::call_site());
                        let name2 = syn::Ident::new(&cm2.name, Span::call_site());
                        let aggtor = syn::Ident::new(&cm2.aggtor, Span::call_site());
                        let ts = quote::quote! { #name2: #aggtor, };
                        fields_decl.push(ts);
                        let ts = quote::quote! { #name2: <#aggtor>::new(), };
                        fields_init.push(ts);
                        let ts = quote::quote! {
                            // let _ = &inp.#name1;
                            self.#name2.ingest(inp.#name1);
                        };
                        ingest.push(ts);
                        found = true;
                    }
                }
                if found == false {
                    let e = syn::Error::new(
                        Span::call_site(),
                        format!(
                            "did not find composition method for input Compose  {:?}",
                            cm1
                        ),
                    );
                    return Err(e);
                }
            }
            (fields_decl, fields_init, ingest)
        };

        let ret = quote::quote! {
            #[derive(Debug, serde::Serialize)]
            pub struct #struct_name {
                #(#fields_counters_decl)*
                #(#fields_compose_decl)*
            }

            impl #struct_name {
                pub fn new() -> Self {
                    Self {
                        #(#fields_counters_init)*
                        #(#fields_compose_init)*
                    }
                }

                pub fn ingest(&mut self, inp: #inp_struct_name) {
                    #(#ingest_counters)*
                    #(#ingest_compose)*
                }
            }
        };
        Ok(ret)
    }

    fn agg_from_agg_token_stream(
        &self,
        agg: &AggregationModItem,
        inp: &AggregationModItem,
    ) -> syn::Result<TokenStream> {
        // recursion
        // input can be a Metrics or a Aggregation type.
        if let Some(inp) = self
            .metrics_mods
            .iter()
            .filter(|&x| x.struct_name == inp.input)
            .next()
        {
            self.agg_from_metrics_token_stream(agg, inp)
        } else if let Some(inp) = self
            .agg_mods
            .iter()
            .filter(|&x| x.struct_name == inp.input)
            .next()
        {
            self.agg_from_agg_token_stream(agg, inp)
        } else {
            let e = syn::Error::new(
                Span::call_site(),
                format!(
                    "can not find input decl to aggregation: {}",
                    agg.input.to_string()
                ),
            );
            Err(e)
        }
    }

    fn agg_token_stream(&self, agg: &AggregationModItem) -> syn::Result<TokenStream> {
        let ts1 = if let Some(inp) = self
            .metrics_mods
            .iter()
            .filter(|&x| x.struct_name == agg.input)
            .next()
        {
            self.agg_from_metrics_token_stream(agg, inp)
        } else if let Some(inp) = self
            .agg_mods
            .iter()
            .filter(|&x| x.struct_name == agg.input)
            .next()
        {
            self.agg_from_agg_token_stream(agg, inp)
        } else {
            let e = syn::Error::new(
                Span::call_site(),
                format!(
                    "can not find input decl to aggregation: {}",
                    agg.input.to_string()
                ),
            );
            Err(e)
        };
        let ts1 = ts1?;
        let ret = quote::quote! {
            #ts1
        };
        log_ts(&ret);
        Ok(ret)
    }

    fn agg_all_token_stream(&self) -> syn::Result<TokenStream> {
        let mut tsv1 = Vec::new();
        for m in self.agg_mods.iter() {
            // TODO preserve span information instead of Span::call_site
            tsv1.push(self.agg_token_stream(&m)?);
        }
        let ret = quote::quote! {
            #(#tsv1)*
        };
        Ok(ret)
    }

    fn metrics_token_stream(&self, metrics: &MetricsModItem) -> syn::Result<TokenStream> {
        let mut tsv1 = Vec::new();
        {
            // struct type declaration
            // TODO preserve span information instead of Span::call_site
            let struct_name = syn::Ident::new(&metrics.struct_name, Span::call_site());
            let field_decl_counters = metrics
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32, });
            let field_decl_composes = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                let ct = syn::Ident::new(&m.input, Span::call_site());
                quote::quote! { #n: #ct, }
            });
            let q1 = quote::quote! {
                #[derive(Debug, serde::Serialize)]
                pub struct #struct_name {
                    #(#field_decl_counters)*
                    #(#field_decl_composes)*
                }
            };
            tsv1.push(q1);
            let field_init_counters = metrics
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32::new(), });
            let field_incs_counters = metrics
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| {
                    quote::quote! {
                        #[inline(always)]
                        pub fn #x(&mut self) -> &mut CounterU32 {
                            &mut self.#x
                        }
                    }
                });
            let field_init_composes = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                let ct = syn::Ident::new(&m.input, Span::call_site());
                quote::quote! { #n: #ct::new(), }
            });
            let field_composes_get_mut = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                let ct = syn::Ident::new(&m.input, Span::call_site());
                quote::quote! {
                    #[inline(always)]
                    pub fn #n(&mut self) -> &mut #ct {
                        &mut self.#n
                    }
                }
            });
            let impl_1 = quote::quote! {
                impl #struct_name {
                    pub fn new() -> Self {
                        Self {
                            #(#field_init_counters)*
                            #(#field_init_composes)*
                        }
                    }
                    pub fn take_and_reset(&mut self) -> Self {
                        std::mem::replace(self, Self::new())
                    }
                    #(#field_incs_counters)*
                    #(#field_composes_get_mut)*
                }
            };
            tsv1.push(impl_1);
        }
        let ret = quote::quote! {
            #(#tsv1)*
        };
        Ok(ret)
    }

    fn metrics_all_token_stream(&self) -> syn::Result<TokenStream> {
        let mut tsv1 = Vec::new();
        for m in self.metrics_mods.iter() {
            // TODO preserve span information instead of Span::call_site
            tsv1.push(self.metrics_token_stream(&m)?);
        }
        let ret = quote::quote! {
            #(#tsv1)*
        };
        Ok(ret)
    }

    pub(super) fn to_code(&self) -> syn::Result<TokenStream> {
        let mods1 = self.metrics_all_token_stream()?;
        let aggs = self.agg_all_token_stream()?;
        let ret = quote::quote! {
            #mods1
            #aggs
        };
        Ok(ret)
    }
}
