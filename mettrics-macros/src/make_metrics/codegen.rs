use super::AggregationModItem;
use super::MetricsDecl;
use super::MetricsModItem;
use crate::log::log_ts;
use proc_macro2::Span;
use proc_macro2::TokenStream;

impl MetricsDecl {
    fn agg_from_counter_names(
        &self,
        agg: &AggregationModItem,
        inp_struct_name: &str,
        counter_names: Vec<String>,
    ) -> syn::Result<TokenStream> {
        let struct_name = syn::Ident::new(&agg.struct_name, Span::call_site());
        let inp_struct_name = syn::Ident::new(inp_struct_name, Span::call_site());
        let (fields_counters_decl, fields_counters_init, ingest_counters) = {
            let fields_decl = counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32, });
            let fields_init = counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32::new(), });
            let ingest_counters = counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { self.#x.ingest(inp.#x); });
            (fields_decl, fields_init, ingest_counters)
        };
        let ts = quote::quote! {
            #[derive(Debug, serde::Serialize)]
            pub struct #struct_name {
                #(#fields_counters_decl)*
            }

            impl #struct_name {
                pub fn new() -> Self {
                    Self {
                        #(#fields_counters_init)*
                    }
                }

                pub fn ingest(&mut self, inp: #inp_struct_name) {
                    #(#ingest_counters)*
                }
            }
        };
        Ok(ts)
    }

    fn agg_find_counters_in_input_metrics(&self, inp: &MetricsModItem) -> syn::Result<Vec<String>> {
        let ret = inp.counter_names.iter().map(|x| x.into()).collect();
        Ok(ret)
    }

    fn agg_find_counters_in_input(&self, inp: &AggregationModItem) -> syn::Result<Vec<String>> {
        // recursion
        // input can be a Metrics or a Aggregation type.
        if let Some(inp) = self.metrics_mods.iter().filter(|&x| x.struct_name == inp.input).next() {
            self.agg_find_counters_in_input_metrics(inp)
        } else if let Some(inp) = self.agg_mods.iter().filter(|&x| x.struct_name == inp.input).next() {
            self.agg_find_counters_in_input(inp)
        } else {
            let e = syn::Error::new(Span::call_site(), format!("can not find input counters"));
            Err(e)
        }
    }

    fn agg_from_agg_token_stream(
        &self,
        agg: &AggregationModItem,
        inp: &AggregationModItem,
    ) -> syn::Result<TokenStream> {
        let inp_struct_name = &inp.struct_name;
        let counter_names = self.agg_find_counters_in_input(inp)?;
        let ts = self.agg_from_counter_names(agg, inp_struct_name, counter_names)?;
        Ok(ts)
    }

    fn agg_from_metrics_token_stream(
        &self,
        agg: &AggregationModItem,
        inp: &MetricsModItem,
    ) -> syn::Result<TokenStream> {
        let inp_struct_name = &inp.struct_name;
        let counter_names = self.agg_find_counters_in_input_metrics(inp)?;
        let ts = self.agg_from_counter_names(agg, inp_struct_name, counter_names)?;
        Ok(ts)
    }

    fn agg_token_stream(&self, agg: &AggregationModItem) -> syn::Result<TokenStream> {
        let ts1 = if let Some(inp) = self.metrics_mods.iter().filter(|&x| x.struct_name == agg.input).next() {
            self.agg_from_metrics_token_stream(agg, inp)
        } else if let Some(inp) = self.agg_mods.iter().filter(|&x| x.struct_name == agg.input).next() {
            self.agg_from_agg_token_stream(agg, inp)
        } else {
            let e = syn::Error::new(
                Span::call_site(),
                format!("can not find input decl to aggregation: {}", agg.input.to_string()),
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
            let field_decl_values = metrics
                .value_names
                .iter()
                .map(|x| syn::Ident::new(&x, Span::call_site()))
                .map(|x| quote::quote! { #x: ValueU32, });
            let field_decl_histolog2s = metrics
                .histolog2_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: HistoLog2, });
            let field_decl_composes = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                // let ct = syn::Ident::new(&m.input, Span::call_site());
                // let ct = syn::parse_str::<syn::Path>(&m.input).unwrap();
                let ct = &m.input;
                quote::quote! { #n: #ct, }
            });
            let q1 = quote::quote! {
                #[derive(Debug, serde::Serialize)]
                pub struct #struct_name {
                    #(#field_decl_counters)*
                    #(#field_decl_values)*
                    #(#field_decl_histolog2s)*
                    #(#field_decl_composes)*
                }
            };
            tsv1.push(q1);
            let field_init_counters = metrics
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: CounterU32::new(), });
            let field_init_values = metrics
                .value_names
                .iter()
                .map(|x| syn::Ident::new(&x, Span::call_site()))
                .map(|x| quote::quote! { #x: ValueU32::new(), });
            let field_init_histolog2s = metrics
                .histolog2_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| quote::quote! { #x: HistoLog2::new(16), });
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
            let fields_counters_ingest = metrics
                .counter_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| {
                    quote::quote! { self.#x.ingest(inp.#x); }
                });
            let ingest_values = metrics
                .value_names
                .iter()
                .map(|x| syn::Ident::new(&x, Span::call_site()))
                .map(|x| {
                    quote::quote! { let _ = "NOTE HERE"; self.#x.ingest(inp.#x); }
                });
            let fields_histlog2s_ingest = metrics
                .histolog2_names
                .iter()
                .map(|x| syn::Ident::new(x, Span::call_site()))
                .map(|x| {
                    quote::quote! { self.#x.ingest_upstream(inp.#x); }
                });
            let flatten_prom_counters = metrics.counter_names.iter().map(|n| {
                let id = syn::Ident::new(&n, Span::call_site());
                quote::quote! {
                    let n = format!("{}_{}", name, #n);
                    ret.push(format!("# TYPE {} counter", n));
                    ret.push(format!("{} {}", n, self.#id.to_u32()));
                }
            });
            let flatten_prom_histolog2s = metrics.histolog2_names.iter().map(|n| {
                let id = syn::Ident::new(n, Span::call_site());
                quote::quote! {
                    let n = format!("{}_{}", name, #n);
                    let v = self.#id.to_flatten_prometheus(&n);
                    ret.extend(v);
                }
            });
            let flatten_prom_values = metrics.value_names.iter().map(|n| {
                let id = syn::Ident::new(n, Span::call_site());
                quote::quote! {
                    let n = format!("{}_{}", name, #n);
                    let v = self.#id.to_flatten_prometheus(&n);
                    ret.extend(v);
                }
            });
            let field_init_composes = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                // let ct = syn::Ident::new(&m.input, Span::call_site());
                let ct = &m.input;
                quote::quote! { #n: #ct::new(), }
            });
            let field_histlog2s_get_mut = metrics.histolog2_names.iter().map(|x| {
                let n = syn::Ident::new(x, Span::call_site());
                quote::quote! {
                    #[inline(always)]
                    pub fn #n(&mut self) -> &mut HistoLog2 {
                        &mut self.#n
                    }
                }
            });
            let field_values_get_mut = metrics.value_names.iter().map(|x| {
                let n = syn::Ident::new(x, Span::call_site());
                quote::quote! {
                    #[inline(always)]
                    pub fn #n(&mut self) -> &mut ValueU32 {
                        &mut self.#n
                    }
                }
            });
            let field_composes_get_mut = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                // let ct = syn::Ident::new(&m.input, Span::call_site());
                let ct = &m.input;
                quote::quote! {
                    #[inline(always)]
                    pub fn #n(&mut self) -> &mut #ct {
                        &mut self.#n
                    }
                }
            });
            let fields_composes_ingest = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                quote::quote! {
                    self.#n.ingest(inp.#n);
                }
            });
            let flatten_prom_composes = metrics.compose_mods.iter().map(|m| {
                let n = &m.name;
                let id = syn::Ident::new(&n, Span::call_site());
                quote::quote! {
                    let n = format!("{}_{}", name, #n);
                    let v = self.#id.to_flatten_prometheus(&n);
                    ret.extend(v);

                }
            });
            let take_from_counters = metrics.counter_names.iter().map(|x| {
                let n = syn::Ident::new(x, Span::call_site());
                quote::quote! {
                    ret.#n.take_from(&mut self.#n);
                }
            });
            let take_from_values = metrics.value_names.iter().map(|x| {
                let n = syn::Ident::new(x, Span::call_site());
                quote::quote! {
                    ret.#n.take_from(&mut self.#n);
                }
            });
            let take_from_histolog2s = metrics.histolog2_names.iter().map(|x| {
                let n = syn::Ident::new(x, Span::call_site());
                quote::quote! {
                    ret.#n.take_from(&mut self.#n);
                }
            });
            let take_from_composes = metrics.compose_mods.iter().map(|m| {
                let n = syn::Ident::new(&m.name, Span::call_site());
                quote::quote! {
                    ret.#n = self.#n.take_and_reset();
                }
            });
            let impl_1 = quote::quote! {
                impl #struct_name {
                    pub fn new() -> Self {
                        Self {
                            #(#field_init_counters)*
                            #(#field_init_values)*
                            #(#field_init_histolog2s)*
                            #(#field_init_composes)*
                        }
                    }
                    pub fn take_and_reset(&mut self) -> Self {
                        let mut ret = Self::new();
                        #(#take_from_counters)*
                        #(#take_from_values)*
                        #(#take_from_histolog2s)*
                        #(#take_from_composes)*
                        ret
                    }
                    pub fn ingest(&mut self, inp: #struct_name) {
                        #(#fields_counters_ingest)*
                        #(#ingest_values)*
                        #(#fields_histlog2s_ingest)*
                        #(#fields_composes_ingest)*
                    }
                    pub fn to_flatten_prometheus(&self, name: &str) -> Vec<String> {
                        let mut ret = Vec::new();
                        #(#flatten_prom_composes)*
                        #(#flatten_prom_counters)*
                        #(#flatten_prom_values)*
                        #(#flatten_prom_histolog2s)*
                        ret

                    }
                    #(#field_incs_counters)*
                    #(#field_values_get_mut)*
                    #(#field_histlog2s_get_mut)*
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
        let decl_js = serde_json::to_string(self).unwrap();
        let ret = quote::quote! {
            #mods1
            #aggs
            pub fn decl_json() -> String {
                #decl_js.into()
            }
        };
        Ok(ret)
    }
}
