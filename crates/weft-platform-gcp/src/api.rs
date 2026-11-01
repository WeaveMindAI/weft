//! Calling Google's REST APIs as the runtime's own service account.
//!
//! Every Google API weft drives (Cloud Run, Compute Engine, Cloud Build,
//! Cloud Tasks, Cloud Storage, IAM) is JSON over HTTPS with an access
//! token, and the slow ones answer with an operation to wait on. This is
//! that, once.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use serde_json::Value;

use crate::metadata::MetadataTokens;

/// How much of an upload goes in one request ([`Google::upload_streamed`]):
/// a multiple of 256 KiB, as Cloud Storage asks of every part of a
/// resumable upload but the last, and the 8 MiB it recommends.
pub const UPLOAD_PART: usize = 8 * 1024 * 1024;

/// What a producer hands [`Google::upload_streamed`].
pub enum Streamed {
    /// The next bytes of the object: [`UPLOAD_PART`] of them, unless it
    /// is the last part.
    Part(Vec<u8>),
    /// Nothing comes after: the object is complete.
    End,
}

/// The `Content-Range` of `len` bytes at `offset` of an upload: the total
/// is said only with the last part (`*` until then), and a last part with
/// no bytes says only the total.
fn content_range(offset: u64, len: u64, last: bool) -> String {
    match (len, last) {
        (0, true) => format!("bytes */{offset}"),
        (_, true) => format!("bytes {offset}-{}/{}", offset + len - 1, offset + len),
        (_, false) => format!("bytes {offset}-{}/*", offset + len - 1),
    }
}

/// How many bytes of an upload Cloud Storage holds, from the `Range` of
/// its 308 (`bytes=0-N`, or no header for none yet).
fn received_up_to(range: Option<&str>) -> anyhow::Result<u64> {
    let Some(range) = range else { return Ok(0) };
    let last = range
        .strip_prefix("bytes=0-")
        .and_then(|end| end.parse::<u64>().ok())
        .ok_or_else(|| anyhow::anyhow!("Cloud Storage said it holds `{range}` of an upload, which is no range from its start"))?;
    Ok(last + 1)
}

/// How many requests in a row may move no bytes of a part before the
/// upload gives up: enough to ride out a dropped connection or a 5xx
/// blip, few enough that an upload Cloud Storage keeps refusing to take
/// fails while somebody still waits on it.
const UPLOAD_TRIES: u32 = 3;

/// What Cloud Storage answered one request of an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    /// A 308: it holds the object up to this byte, and wants the rest.
    Holds(u64),
    /// A 200 or 201: the object is complete.
    Complete,
    /// No answer came back (the request failed on the way), or one Cloud
    /// Storage says to try again (`why`): what the session holds is
    /// unknown until it is asked.
    Lost(String),
}

/// What an answer to a request of an upload means: [`Answer::Holds`] for
/// a 308 (its `Range`, [`received_up_to`]), [`Answer::Complete`] for a
/// 200 or 201, [`Answer::Lost`] for the answers Cloud Storage says to try
/// again on (a 5xx, 408 and 429). Any other status is Google refusing the
/// upload, an [`ApiError`] with its `body`: sending again would be
/// refused again.
fn answer_of(status: u16, range: Option<&str>, body: String) -> anyhow::Result<Answer> {
    match status {
        308 => Ok(Answer::Holds(received_up_to(range)?)),
        200 | 201 => Ok(Answer::Complete),
        408 | 429 | 500..=599 => Ok(Answer::Lost(ApiError { status, body }.to_string())),
        _ => Err(ApiError { status, body }.into()),
    }
}

/// What the upload of a part sends next ([`PartUpload::read`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// The bytes of the part from [`PartUpload::held`] on.
    Send,
    /// Nothing: it asks the session what it holds ([`PartUpload::asking_range`]).
    Ask,
    /// Nothing more: Cloud Storage holds the whole part.
    Done,
}

/// Where the upload of one part stands, read off each answer Cloud
/// Storage gives it. The part runs to `end`; the session holds the object
/// up to `held`, which only moves forward.
#[derive(Debug)]
struct PartUpload {
    held: u64,
    end: u64,
    last: bool,
    /// Sends in a row that moved no bytes, and what the last one said.
    stalled: u32,
    stalled_on: String,
    /// The request out is a question of where the session stands, not a
    /// send: its answer moves no bytes by nature, so it is never a stall.
    asking: bool,
}

impl PartUpload {
    /// `len` bytes at `offset`, all of the session's bytes before them.
    fn new(offset: u64, len: u64, last: bool) -> Self {
        Self { held: offset, end: offset + len, last, stalled: 0, stalled_on: String::new(), asking: false }
    }

    /// The `Content-Range` of the bytes still to send.
    fn sending_range(&self) -> String {
        content_range(self.held, self.end - self.held, self.last)
    }

    /// The `Content-Range` of an empty request asking the session what it
    /// holds: the total once it is known (with the last part), `*` until
    /// then.
    fn asking_range(&self) -> String {
        match self.last {
            true => format!("bytes */{}", self.end),
            false => "bytes */*".to_string(),
        }
    }

    /// What `answer` means for this part. A 308 moves `held` to what it
    /// says and sends the rest, or ends a part that is all held (the last
    /// part is done only once Cloud Storage says the object is complete:
    /// a 308 holding all of it sends its total again, which completes
    /// it). A lost request asks where the session stands. A complete
    /// object before the last part, or a 308 holding less than it held
    /// before or more than was sent, is no upload this sent: it fails.
    /// [`UPLOAD_TRIES`] sends in a row that move no bytes fail it too,
    /// naming what the last one said, so a session that never takes
    /// anything ends instead of looping. The question asked after a lost
    /// send is no send: a 308 answering it counts nothing, and only the
    /// question itself being lost counts.
    fn read(&mut self, answer: Answer) -> anyhow::Result<Next> {
        let asked = std::mem::take(&mut self.asking);
        let (next, moved) = match answer {
            Answer::Complete if self.last => return Ok(Next::Done),
            Answer::Complete => anyhow::bail!("Cloud Storage completed the object before its last part was sent"),
            Answer::Holds(held) if held < self.held || held > self.end => {
                anyhow::bail!("Cloud Storage holds {held} bytes of an upload it held {} of and sent up to {}", self.held, self.end)
            }
            Answer::Holds(held) => {
                let moved = held > self.held;
                self.held = held;
                if held == self.end && !self.last {
                    return Ok(Next::Done);
                }
                self.stalled_on = format!("Cloud Storage held {held} of the {} bytes sent", self.end);
                (Next::Send, moved)
            }
            Answer::Lost(why) => {
                self.stalled_on = why;
                (Next::Ask, false)
            }
        };
        self.asking = next == Next::Ask;
        if moved {
            self.stalled = 0;
            return Ok(next);
        }
        if asked && next == Next::Send {
            return Ok(next);
        }
        self.stalled += 1;
        if self.stalled >= UPLOAD_TRIES {
            anyhow::bail!("{UPLOAD_TRIES} sends in a row moved no bytes of the upload past byte {}; the last: {}", self.held, self.stalled_on);
        }
        Ok(next)
    }
}

/// How often an operation still running is looked at again.
const OPERATION_POLL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Google {
    http: reqwest::Client,
    tokens: Arc<MetadataTokens>,
}

/// What an API answered when it answered with an error.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub body: String,
}

/// How much of an answer that is not Google's error shape (a proxy's
/// HTML page) an error shows.
const SHOWN_BODY: usize = 300;

/// An [`ApiError`] reads as one line, the way `weft status` and the alert
/// lines show it: the HTTP status, then Google's words ([`said`]) when the
/// body is Google's error shape, else the body itself on one line.
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = serde_json::from_str::<Value>(&self.body).ok().and_then(|body| body.get("error").and_then(said));
        match error {
            Some(said) => write!(f, "Google answered {}: {said}", self.status),
            None => write!(f, "Google answered {}: {}", self.status, one_line(&self.body, SHOWN_BODY)),
        }
    }
}

/// What Google said in `error` (an API answer's `error`, or a failed
/// operation's) as one line: its status word, its message, then what its
/// details add ([`added_by`]). The message is a `google.rpc.Status`'s
/// `message`, or the messages of Compute Engine's `errors` list. `None`
/// for a value with no message, which is no such error.
fn said(error: &Value) -> Option<String> {
    let text = |v: &Value| v.get("message").and_then(Value::as_str).map(|m| one_line(m, usize::MAX)).filter(|m| !m.is_empty());
    let message = match text(error) {
        Some(message) => message,
        None => {
            let listed: Vec<String> = error.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(text).collect();
            if listed.is_empty() {
                return None;
            }
            listed.join("; ")
        }
    };
    let mut said = match error.get("status").and_then(Value::as_str) {
        Some(status) => format!("{status}: {message}"),
        None => message.clone(),
    };
    let added: Vec<String> = error.get("details").and_then(Value::as_array).into_iter().flatten().flat_map(|detail| added_by(detail, &message)).collect();
    if !added.is_empty() {
        said.push_str(&format!(" ({})", added.join("; ")));
    }
    Some(said)
}

/// What one of an error's `details` adds to its `message`: the permission
/// an `ErrorInfo` names, each field a `BadRequest` refuses with why, and
/// each violation of a `QuotaFailure` or a `PreconditionFailure`. Nothing
/// for a detail of any other type (a `Help`'s links, a `RequestInfo`'s
/// id).
fn added_by(detail: &Value, message: &str) -> Vec<String> {
    let kind = detail.get("@type").and_then(Value::as_str).and_then(|t| t.rsplit('.').next()).unwrap_or_default();
    let str_at = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(|s| one_line(s, usize::MAX)).filter(|s| !s.is_empty());
    let each = |key: &str, say: &dyn Fn(&Value) -> Option<String>| -> Vec<String> {
        detail.get(key).and_then(Value::as_array).into_iter().flatten().filter_map(say).collect()
    };
    match kind {
        // A permission the message names is not said twice.
        "ErrorInfo" => detail
            .pointer("/metadata/permission")
            .and_then(Value::as_str)
            .filter(|p| !message.contains(p))
            .map(|p| format!("missing permission: {p}"))
            .into_iter()
            .collect(),
        "BadRequest" => each("fieldViolations", &|v| match (str_at(v, "field"), str_at(v, "description")) {
            (Some(field), Some(why)) => Some(format!("{field}: {why}")),
            (field, why) => field.or(why),
        }),
        "QuotaFailure" | "PreconditionFailure" => each("violations", &|v| str_at(v, "description").or_else(|| str_at(v, "subject"))),
        _ => Vec::new(),
    }
}

/// `text` with every run of whitespace one space, cut to `at_most`
/// characters.
fn one_line(text: &str, at_most: usize) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match line.char_indices().nth(at_most) {
        Some((cut, _)) => format!("{}...", &line[..cut]),
        None => line,
    }
}

impl std::error::Error for ApiError {}

/// Whether `e` is Google answering `status`.
pub fn is_status(e: &anyhow::Error, status: u16) -> bool {
    e.chain().any(|c| c.downcast_ref::<ApiError>().is_some_and(|a| a.status == status))
}

impl Google {
    pub fn new(tokens: Arc<MetadataTokens>) -> Self {
        Self {
            http: weft_core::net::http_client().timeout(Duration::from_secs(60)).build().expect("reqwest client"),
            tokens,
        }
    }

    pub fn tokens(&self) -> &Arc<MetadataTokens> {
        &self.tokens
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Send `req` as this process's account, answering Google's response
    /// whatever its status: for a caller that reads an answer that is not
    /// JSON (Cloud Storage's objects and XML) or treats a status itself.
    pub async fn send_raw(&self, req: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        let token = self.tokens.access_token().await?;
        Ok(req.bearer_auth(token).send().await?)
    }

    /// `resp` when it is a success; its status and Google's words, as an
    /// [`ApiError`], otherwise.
    pub async fn success(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        Err(ApiError { status, body: resp.text().await.unwrap_or_default() }.into())
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> anyhow::Result<Value> {
        let resp = Self::success(self.send_raw(req).await?).await?;
        let body = resp.text().await?;
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).with_context(|| format!("Google answered something that is not JSON: {body}"))
    }

    pub async fn get(&self, url: &str) -> anyhow::Result<Value> {
        self.send(self.http.get(url)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET` with `query` as the URL's query string, encoded.
    pub async fn get_query(&self, url: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        self.send(self.http.get(url).query(query)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET`, with a 404 read as "not there".
    pub async fn get_opt(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.get(url).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn post(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.post(url).json(body)).await.with_context(|| format!("POST {url}"))
    }

    pub async fn put(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.put(url).json(body)).await.with_context(|| format!("PUT {url}"))
    }

    pub async fn patch(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.patch(url).json(body)).await.with_context(|| format!("PATCH {url}"))
    }

    /// Upload what `parts` yields, in order, to Cloud Storage as one object,
    /// holding no more of it than the part in flight: a resumable upload
    /// started at `start_url` (the bucket's `uploadType=resumable` URL
    /// naming the object). Every part but the last is [`UPLOAD_PART`]
    /// long, as Cloud Storage asks; the object is completed only on
    /// [`Streamed::End`]. A failure from the producer, or parts that stop
    /// before their end, fail the upload and cancel it, so no cut-off
    /// object is ever left.
    pub async fn upload_streamed(
        &self,
        start_url: &str,
        content_type: &str,
        mut parts: tokio::sync::mpsc::Receiver<anyhow::Result<Streamed>>,
    ) -> anyhow::Result<()> {
        let started = Self::success(
            self.send_raw(self.http.post(start_url).header("x-upload-content-type", content_type).header("content-length", "0"))
                .await
                .with_context(|| format!("start an upload at {start_url}"))?,
        )
        .await
        .with_context(|| format!("start an upload at {start_url}"))?;
        let session = started
            .headers()
            .get("location")
            .and_then(|l| l.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("Cloud Storage started an upload at {start_url} without saying where to send it"))?
            .to_string();
        let sent = self.send_parts(&session, &mut parts).await;
        if sent.is_err() {
            // Cancelled, so the session holds nothing; one this cannot
            // cancel ends on its own a week on, with nothing in the bucket.
            if let Err(e) = self.send_raw(self.http.delete(&session)).await {
                tracing::warn!(target: "weft_platform_gcp::api", error = %format!("{e:#}"), "an upload that failed could not be cancelled");
            }
        }
        sent
    }

    /// The parts of [`Self::upload_streamed`], to its `session`.
    async fn send_parts(&self, session: &str, parts: &mut tokio::sync::mpsc::Receiver<anyhow::Result<Streamed>>) -> anyhow::Result<()> {
        // A part goes once the next is known: only the one before the
        // end is sent as the last.
        let mut offset: u64 = 0;
        let mut pending: Option<Vec<u8>> = None;
        loop {
            match parts.recv().await {
                Some(Ok(Streamed::Part(part))) => {
                    if let Some(ahead) = pending.replace(part) {
                        offset = self.send_part(session, offset, ahead, false).await?;
                    }
                }
                Some(Ok(Streamed::End)) => {
                    self.send_part(session, offset, pending.take().unwrap_or_default(), true).await?;
                    return Ok(());
                }
                Some(Err(e)) => return Err(e.context("make what is uploaded")),
                None => anyhow::bail!("what is uploaded stopped before its end"),
            }
        }
    }

    /// Send `part`, starting at `offset` of the object, until Cloud Storage
    /// holds all of it, answering the offset after it. What each answer
    /// means is [`PartUpload::read`]'s: the part stays in memory until it
    /// is all held, so a request lost on the way, or a 5xx, asks the
    /// session where it stands and sends again from there, waiting a
    /// second longer each time nothing moved.
    async fn send_part(&self, session: &str, offset: u64, part: Vec<u8>, last: bool) -> anyhow::Result<u64> {
        let part = bytes::Bytes::from(part);
        let mut upload = PartUpload::new(offset, part.len() as u64, last);
        let mut next = Next::Send;
        loop {
            let req = match next {
                Next::Done => return Ok(upload.end),
                Next::Send => self
                    .http
                    .put(session)
                    .header("content-range", upload.sending_range())
                    .body(part.slice((upload.held - offset) as usize..)),
                Next::Ask => self.http.put(session).header("content-range", upload.asking_range()).header("content-length", "0"),
            };
            if upload.stalled > 0 {
                tokio::time::sleep(Duration::from_secs(upload.stalled as u64)).await;
            }
            let answer = self.upload_answer(req).await.with_context(|| format!("upload the part at byte {offset}"))?;
            next = upload.read(answer).with_context(|| format!("upload the part at byte {offset}"))?;
        }
    }

    /// What Cloud Storage answered `req`, one request of an upload
    /// ([`answer_of`]): a request that got no answer is [`Answer::Lost`].
    /// The status and headers are the answer; a body that breaks off is
    /// said in its place.
    async fn upload_answer(&self, req: reqwest::RequestBuilder) -> anyhow::Result<Answer> {
        let resp = match self.send_raw(req).await {
            Ok(resp) => resp,
            Err(e) => return Ok(Answer::Lost(format!("{e:#}"))),
        };
        let status = resp.status().as_u16();
        let range = resp.headers().get("range").and_then(|r| r.to_str().ok()).map(str::to_string);
        let body = resp.text().await.unwrap_or_else(|e| format!("(the answer's body broke off: {e:#})"));
        answer_of(status, range.as_deref(), body)
    }

    /// `DELETE`, with a 404 read as "already gone".
    pub async fn delete(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.send(self.http.delete(url)).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e.context(format!("DELETE {url}"))),
        }
    }

    /// Wait for a long-running operation to end, failing with its error.
    /// `op` is what the call that started it answered; `base` is the
    /// API's root the operation's name is under.
    pub async fn wait(&self, base: &str, op: Value) -> anyhow::Result<Value> {
        let mut op = op;
        loop {
            let done = op.get("done").and_then(Value::as_bool).unwrap_or(false)
                || op.get("status").and_then(Value::as_str) == Some("DONE");
            if done {
                if let Some(err) = op.get("error").filter(|e| !e.is_null()) {
                    match said(err) {
                        Some(said) => anyhow::bail!("the operation failed: {said}"),
                        None => anyhow::bail!("the operation failed: {err}"),
                    }
                }
                return Ok(op);
            }
            let url = match (op.get("selfLink").and_then(Value::as_str), op.get("name").and_then(Value::as_str)) {
                (Some(link), _) => link.to_string(),
                (None, Some(name)) => format!("{}/{name}", base.trim_end_matches('/')),
                (None, None) => anyhow::bail!("an operation with no name to follow: {op}"),
            };
            tokio::time::sleep(OPERATION_POLL).await;
            op = self.get(&url).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each part says where it sits; the total is said with the last one
    /// only, and a last part with no bytes says only the total.
    #[test]
    fn a_part_says_where_it_sits_and_the_last_the_total() {
        assert_eq!(content_range(0, 8, false), "bytes 0-7/*");
        assert_eq!(content_range(8, 4, true), "bytes 8-11/12");
        assert_eq!(content_range(12, 0, true), "bytes */12");
        assert_eq!(content_range(0, 0, true), "bytes */0");
    }

    /// What Cloud Storage holds is read off its `Range`, none without one;
    /// anything else is refused rather than guessed at.
    #[test]
    fn what_cloud_storage_holds_is_read_off_its_range() {
        assert_eq!(received_up_to(None).unwrap(), 0);
        assert_eq!(received_up_to(Some("bytes=0-8388607")).unwrap(), 8_388_608);
        assert!(received_up_to(Some("bytes=5-9")).is_err());
        assert!(received_up_to(Some("bytes=0-")).is_err());
    }

    /// A 308 says what is held, a 200 or 201 a complete object, a 5xx a
    /// request to ask about again; anything else is Google refusing, with
    /// its words.
    #[test]
    fn an_answer_to_an_upload_is_read_off_its_status() {
        assert_eq!(answer_of(308, Some("bytes=0-99"), String::new()).unwrap(), Answer::Holds(100));
        assert_eq!(answer_of(308, None, String::new()).unwrap(), Answer::Holds(0));
        assert!(answer_of(308, Some("bytes=3-9"), String::new()).is_err());
        assert_eq!(answer_of(200, None, "{}".into()).unwrap(), Answer::Complete);
        assert_eq!(answer_of(201, None, "{}".into()).unwrap(), Answer::Complete);
        assert_eq!(answer_of(503, None, "busy".into()).unwrap(), Answer::Lost("Google answered 503: busy".into()));
        for again in [408, 429] {
            assert!(matches!(answer_of(again, None, "later".into()).unwrap(), Answer::Lost(_)), "{again} is tried again");
        }
        let refused = answer_of(403, None, "no".into()).unwrap_err();
        assert!(is_status(&refused, 403), "{refused:#}");
        assert_eq!(refused.to_string(), "Google answered 403: no");
        assert!(is_status(&answer_of(404, None, String::new()).unwrap_err(), 404), "a session gone is refused at once");
    }

    /// Every part but the last ends once all of it is held; a 308 holding
    /// only its start sends the rest from there.
    #[test]
    fn a_part_ends_once_all_of_it_is_held() {
        let mut part = PartUpload::new(8, 8, false);
        assert_eq!(part.sending_range(), "bytes 8-15/*");
        assert_eq!(part.read(Answer::Holds(12)).unwrap(), Next::Send);
        assert_eq!(part.sending_range(), "bytes 12-15/*", "only what it does not hold goes again");
        assert_eq!(part.read(Answer::Holds(16)).unwrap(), Next::Done);
    }

    /// The last part takes the same resend path: a 308 holding its start
    /// sends the rest, a 308 holding all of it sends the total again,
    /// and only a complete object ends it.
    #[test]
    fn the_last_part_is_resent_like_any_other_and_ends_complete() {
        let mut part = PartUpload::new(8, 4, true);
        assert_eq!(part.sending_range(), "bytes 8-11/12");
        assert_eq!(part.read(Answer::Holds(10)).unwrap(), Next::Send);
        assert_eq!(part.sending_range(), "bytes 10-11/12");
        assert_eq!(part.read(Answer::Holds(12)).unwrap(), Next::Send);
        assert_eq!(part.sending_range(), "bytes */12", "all held: the total alone completes it");
        assert_eq!(part.read(Answer::Complete).unwrap(), Next::Done);
        let mut empty = PartUpload::new(12, 0, true);
        assert_eq!(empty.sending_range(), "bytes */12");
        assert_eq!(empty.read(Answer::Complete).unwrap(), Next::Done);
    }

    /// A complete object before the last part, or a 308 holding less than
    /// was held or more than was sent, is no upload this sent.
    #[test]
    fn an_answer_no_upload_could_give_fails_it() {
        let e = PartUpload::new(0, 8, false).read(Answer::Complete).unwrap_err().to_string();
        assert!(e.contains("before its last part"), "{e}");
        let mut part = PartUpload::new(8, 8, false);
        assert!(part.read(Answer::Holds(4)).is_err(), "less than it held");
        assert!(PartUpload::new(8, 8, false).read(Answer::Holds(17)).is_err(), "more than was sent");
        let mut part = PartUpload::new(0, 8, false);
        part.read(Answer::Holds(4)).unwrap();
        assert!(part.read(Answer::Holds(2)).is_err(), "a held byte is never given back");
    }

    /// A request lost on the way asks the session where it stands, and
    /// the part goes on from what it says; the session's question takes
    /// the total once it is known.
    #[test]
    fn a_lost_request_asks_and_resumes_from_what_is_held() {
        let mut part = PartUpload::new(0, 8, false);
        assert_eq!(part.read(Answer::Lost("connection reset".into())).unwrap(), Next::Ask);
        assert_eq!(part.asking_range(), "bytes */*");
        assert_eq!(part.read(Answer::Holds(6)).unwrap(), Next::Send);
        assert_eq!(part.stalled, 0, "bytes moved: the count starts over");
        assert_eq!(part.sending_range(), "bytes 6-7/*");
        assert_eq!(part.read(Answer::Lost("Google answered 503: busy".into())).unwrap(), Next::Ask);
        assert_eq!(part.read(Answer::Holds(8)).unwrap(), Next::Done, "the lost request had landed whole");

        let mut last = PartUpload::new(8, 4, true);
        assert_eq!(last.read(Answer::Lost("timed out".into())).unwrap(), Next::Ask);
        assert_eq!(last.asking_range(), "bytes */12");
        assert_eq!(last.read(Answer::Complete).unwrap(), Next::Done, "the lost request completed the object");
    }

    /// Sends that move no bytes, lost ones and 308s alike, fail the part
    /// at the [`UPLOAD_TRIES`]th in a row, naming the last; the question
    /// asked after a lost send is no try of its own; one that moves bytes
    /// starts the count over.
    #[test]
    fn sends_that_move_nothing_fail_the_part_after_the_tries() {
        let mut part = PartUpload::new(0, 8, false);
        for _ in 0..2 {
            assert_eq!(part.read(Answer::Lost("reset".into())).unwrap(), Next::Ask);
            assert_eq!(part.read(Answer::Holds(0)).unwrap(), Next::Send, "the answer to the question counts nothing");
        }
        let e = part.read(Answer::Lost("Google answered 502: gateway".into())).unwrap_err().to_string();
        assert!(e.contains("3 sends in a row") && e.contains("502: gateway"), "{e}");

        let mut part = PartUpload::new(0, 8, false);
        assert_eq!(part.read(Answer::Lost("reset".into())).unwrap(), Next::Ask);
        assert_eq!(part.read(Answer::Lost("reset".into())).unwrap(), Next::Ask, "a lost question counts");
        assert_eq!(part.read(Answer::Holds(2)).unwrap(), Next::Send, "bytes moved");
        part.read(Answer::Holds(2)).unwrap();
        part.read(Answer::Holds(2)).unwrap();
        let e = part.read(Answer::Holds(2)).unwrap_err().to_string();
        assert!(e.contains("held 2 of the 8 bytes sent"), "a 308 that never moves cannot loop forever: {e}");

        let mut last = PartUpload::new(8, 4, true);
        last.read(Answer::Holds(12)).unwrap();
        last.read(Answer::Holds(12)).unwrap();
        last.read(Answer::Holds(12)).unwrap();
        assert!(last.read(Answer::Holds(12)).is_err(), "a last part held whole that never completes ends too");
    }

    /// Cloud Run refusing to read a worker pool, the way Google answers a
    /// permission not granted yet: the permission is in the message, and
    /// again in the details.
    const RUN_DENIED: &str = r#"{
      "error": {
        "code": 403,
        "message": "Permission 'run.workerpools.get' denied on resource 'projects/acme/locations/europe-west1/workerPools/weft-holder' (or resource may not exist).",
        "status": "PERMISSION_DENIED",
        "details": [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "IAM_PERMISSION_DENIED",
            "domain": "iam.googleapis.com",
            "metadata": { "permission": "run.workerpools.get", "resource": "projects/acme/locations/europe-west1/workerPools/weft-holder" }
          }
        ]
      }
    }"#;

    /// Cloud Tasks refusing to make a task, its message naming no
    /// permission and its details naming it.
    const TASKS_DENIED: &str = r#"{
      "error": {
        "code": 403,
        "message": "The principal (user or service account) lacks IAM permission for the resource \"projects/acme/locations/europe-west1/queues/weft-ticks\" (or the resource may not exist).",
        "status": "PERMISSION_DENIED",
        "details": [
          { "@type": "type.googleapis.com/google.rpc.Help", "links": [{ "url": "https://cloud.google.com/tasks/docs" }] },
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "IAM_PERMISSION_DENIED",
            "domain": "cloudtasks.googleapis.com",
            "metadata": { "permission": "cloudtasks.tasks.create" }
          }
        ]
      }
    }"#;

    fn shown(status: u16, body: &str) -> String {
        ApiError { status, body: body.into() }.to_string()
    }

    #[test]
    fn a_google_refusal_reads_as_one_line_of_its_words() {
        assert_eq!(
            shown(403, RUN_DENIED),
            "Google answered 403: PERMISSION_DENIED: Permission 'run.workerpools.get' denied on resource \
             'projects/acme/locations/europe-west1/workerPools/weft-holder' (or resource may not exist).",
            "the permission is said once"
        );
        assert_eq!(
            shown(403, TASKS_DENIED),
            "Google answered 403: PERMISSION_DENIED: The principal (user or service account) lacks IAM permission for the resource \
             \"projects/acme/locations/europe-west1/queues/weft-ticks\" (or the resource may not exist). \
             (missing permission: cloudtasks.tasks.create)",
            "a permission only the details name is added"
        );
    }

    /// A refusal whose message is generic says which field was wrong, and
    /// why, only in its details.
    #[test]
    fn a_bad_request_says_each_field_it_refused() {
        let body = r#"{"error": {"code": 400, "message": "Request contains an invalid argument.", "status": "INVALID_ARGUMENT", "details": [
            { "@type": "type.googleapis.com/google.rpc.BadRequest", "fieldViolations": [
                { "field": "spec.template.spec.containers[0].resources.limits.memory", "description": "Invalid value specified for memory." },
                { "description": "Only one container may be set." }
            ] },
            { "@type": "type.googleapis.com/google.rpc.QuotaFailure", "violations": [{ "subject": "project:acme", "description": "CPU quota exceeded in europe-west1." }] }
        ]}}"#;
        assert_eq!(
            shown(400, body),
            "Google answered 400: INVALID_ARGUMENT: Request contains an invalid argument. \
             (spec.template.spec.containers[0].resources.limits.memory: Invalid value specified for memory.; \
             Only one container may be set.; CPU quota exceeded in europe-west1.)"
        );
    }

    #[test]
    fn an_answer_that_is_not_googles_error_shape_reads_as_its_body_on_one_line() {
        assert_eq!(shown(502, "<html>\n  <body>Bad   gateway</body>\n</html>\n"), "Google answered 502: <html> <body>Bad gateway</body> </html>");
        assert_eq!(shown(404, ""), "Google answered 404: ");
        assert_eq!(shown(400, r#"{"error": {"code": 400}}"#), r#"Google answered 400: {"error": {"code": 400}}"#, "no message: the body as it is");
        assert_eq!(
            shown(404, r#"{"error": {"code": 404, "message": "  ", "status": "NOT_FOUND"}}"#),
            r#"Google answered 404: {"error": {"code": 404, "message": " ", "status": "NOT_FOUND"}}"#,
            "an empty message is no message"
        );
        let long = "x".repeat(SHOWN_BODY + 50);
        assert_eq!(shown(500, &long), format!("Google answered 500: {}...", "x".repeat(SHOWN_BODY)));
    }

    /// A failed operation's error is the same shape, usually with no
    /// status word.
    #[test]
    fn a_failed_operations_error_reads_as_its_message() {
        let error = serde_json::json!({ "code": 7, "message": "Permission denied on secret: projects/acme/secrets/s/versions/latest" });
        assert_eq!(said(&error).unwrap(), "Permission denied on secret: projects/acme/secrets/s/versions/latest");
        assert_eq!(said(&serde_json::json!({ "code": 13 })), None);
        assert_eq!(said(&serde_json::json!({ "code": 13, "message": "", "errors": [{ "message": "disk full" }] })).unwrap(), "disk full", "an empty message reads the list");
        let compute = serde_json::json!({ "errors": [
            { "code": "QUOTA_EXCEEDED", "message": "Quota 'GPUS_ALL_REGIONS' exceeded.  Limit: 0.0 globally." },
            { "code": "ZONE_RESOURCE_POOL_EXHAUSTED", "message": "The zone does not have enough resources." }
        ] });
        assert_eq!(
            said(&compute).unwrap(),
            "Quota 'GPUS_ALL_REGIONS' exceeded. Limit: 0.0 globally.; The zone does not have enough resources.",
            "a Compute Engine operation's errors, each message in turn"
        );
    }
}
