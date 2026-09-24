//! The small static page the public trigger surface shows at its bare
//! root, so a person checking the address sees something deliberate
//! instead of a naked 404. The front door's public listener rewrites
//! `/`, `/index.html` and `/logo.png` onto these routes (see
//! `deploy/k8s/gateway.yaml`, the `weft-public-door` route); the files
//! are built into the binary, so editing them ships with the next
//! dispatcher image.

use axum::http::header;
use axum::response::IntoResponse;

/// Where the page's files are served. Reached from outside only through
/// the public door's rewrite.
// SYNC: PUBLIC_PAGE_PREFIX <-> deploy/k8s/gateway.yaml (weft-public-door's
//       URLRewrite targets)
pub const PUBLIC_PAGE_PREFIX: &str = "/public-page";

pub async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../public-page/index.html"),
    )
}

pub async fn logo() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], include_bytes!("../../public-page/logo.png").as_slice())
}
