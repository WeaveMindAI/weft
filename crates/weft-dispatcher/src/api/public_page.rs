//! The small static page the install shows at its bare root, so a person
//! checking the address sees something deliberate instead of a naked
//! 404. The files are built into the binary, so editing them ships with
//! the next runtime.

use axum::http::header;
use axum::response::IntoResponse;

pub async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../public-page/index.html"),
    )
}

pub async fn logo() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "image/png")], include_bytes!("../../public-page/logo.png").as_slice())
}
