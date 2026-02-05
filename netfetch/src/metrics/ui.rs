use axum::Router;
use axum::extract;
use axum::http;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::future::ready;
use std::path::PathBuf;

pub fn make_routes_daqingest_ui_node() -> Router {
    use axum::routing::get;
    Router::new()
        .fallback(|| async { StatusCode::NOT_FOUND })
        .route(
            "/allpaths",
            get({ move || async move { format!("{:?}", daqingest_ui::assets::all_asset_paths()) } }),
        )
        .route("/a1", get(|| ready(format!("a1 without trailing"))))
        .route("/a1/", get(|| ready(format!("a1 with trailing"))))
        .route("/b1/", get(|| ready(format!("b1 with trailing"))))
        .route("/b1", get(|| ready(format!("b1 without trailing"))))
        .route("/c1", get(|| ready(format!("c1 without trailing"))))
        .route("/c1/", get(|| ready(format!("c1 with trailing"))))
        .route(
            "/c1/{*path}",
            get(|extract::Path(path): extract::Path<String>| ready(format!("c1 with wildcard  {path:?}"))),
        )
        .route("/d1/", get(|| ready(format!("d1 with trailing"))))
        .route("/e1", get(|| ready(format!("e1 without trailing"))))
        .route(
            "/e1/{*path}",
            get(|extract::Path(path): extract::Path<String>| ready(format!("e1 with wildcard  {path:?}"))),
        )
        .route("/f1/", get(|| ready(format!("f1 with trailing"))))
        .route(
            "/f1/{*path}",
            get(|extract::Path(path): extract::Path<String>| ready(format!("f1 with wildcard  {path:?}"))),
        )
        .route(
            "/g1{*path}",
            get(|extract::Path(path): extract::Path<String>| ready(format!("g1 with wildcard  {path:?}"))),
        )
        .route(
            "/ui1/_app/{*path}",
            get({
                let pre = "/ui1/client/daqingest/ui/ui1/_app";
                move |extract::Path(path): extract::Path<String>| async move {
                    let full = format!("{pre}/{path}");
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => ([(http::header::CONTENT_TYPE, mime)], bytes).into_response(),
                        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
                    }
                }
            }),
        )
        .route(
            "/ui1",
            get(|| ready((StatusCode::SEE_OTHER, [(http::header::LOCATION, "ui1/")]))),
        )
        .route(
            "/ui1/",
            get({
                let pre = "/ui1/prerendered/daqingest/ui/ui1";
                let path = "index.html";
                move || async move {
                    let full = format!("{pre}/{path}");
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => ([(http::header::CONTENT_TYPE, mime)], bytes).into_response(),
                        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
                    }
                }
            }),
        )
        .route(
            "/ui1/img/{*path}",
            get({
                let pre = "/ui1/client/daqingest/ui/ui1/img";
                move |extract::Path(path): extract::Path<String>| async move {
                    let full = format!("{pre}/{path}");
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => ([(http::header::CONTENT_TYPE, mime)], bytes).into_response(),
                        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
                    }
                }
            }),
        )
        .route(
            "/ui1/{*path}",
            get({
                let pre = "/ui1/prerendered/daqingest/ui/ui1";
                move |extract::Path(path): extract::Path<String>| async move {
                    let path2 = if path == "" {
                        format!("index.html")
                    } else {
                        let p2 = PathBuf::from(&path);
                        if p2.extension().is_some() {
                            format!("{path}")
                        } else {
                            format!("{path}.html")
                        }
                    };
                    let full = format!("{pre}/{path2}");
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => ([(http::header::CONTENT_TYPE, mime)], bytes).into_response(),
                        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
                    }
                }
            }),
        )
}
