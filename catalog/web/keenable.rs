//! Shared Keenable plumbing: one place owning the API base, the app
//! header Keenable asks every caller for, and the split between its
//! keyless and keyed routes, so each Keenable node states only its
//! request and its field extraction.
//!
//! With no account wired, a node calls the public routes, which take
//! no key and are rate limited per IP. With a KeenableAccess wired, the
//! connection's client signs the call with the key (the recipe's
//! `X-API-Key` header) and the node calls the keyed routes, whose
//! limits are the account's.

use serde_json::Value;

use weft::access::client::json_call;
use weft::reqwest_middleware::{ClientWithMiddleware, RequestBuilder};
use weft::{Access, RunContext, WeftResult};

pub const API: &str = "https://api.keenable.ai/v1";

/// The header naming the calling app. Keenable refuses a keyless call
/// without it, and counts keyed calls per app with it.
pub const TITLE_HEADER: &str = "X-Keenable-Title";
pub const TITLE: &str = "weft";

/// A client for Keenable and which of its routes it may call.
pub struct Keenable {
    http: ClientWithMiddleware,
    keyed: bool,
}

impl Keenable {
    /// `keyed` says the client signs its calls with a key, so they go
    /// to the keyed routes; a plain client goes to the public ones.
    pub fn new(http: ClientWithMiddleware, keyed: bool) -> Self {
        Self { http, keyed }
    }

    /// The wired account's signed-in client, or a plain one when the
    /// node's optional account is not wired.
    pub async fn open(ctx: &RunContext, account: Option<&Access>) -> WeftResult<Self> {
        Ok(Self::new(ctx.client(account).await?, account.is_some()))
    }

    /// The address of `route` (`search`, `fetch`) this client calls.
    pub fn url(&self, route: &str) -> String {
        if self.keyed {
            format!("{API}/{route}")
        } else {
            format!("{API}/{route}/public")
        }
    }

    /// POST `body` to `route`, refusing a failed status loudly with
    /// Keenable's own words, and hand back the parsed answer.
    pub async fn post(&self, route: &str, body: &Value, what: &str) -> WeftResult<Value> {
        self.call(self.http.post(self.url(route)).json(body), what).await
    }

    /// GET `route` with `query`, the GET twin of [`Self::post`].
    pub async fn get(&self, route: &str, query: &[(&str, &str)], what: &str) -> WeftResult<Value> {
        self.call(self.http.get(self.url(route)).query(query), what).await
    }

    async fn call(&self, req: RequestBuilder, what: &str) -> WeftResult<Value> {
        json_call(req.header(TITLE_HEADER, TITLE), what).await
    }
}
