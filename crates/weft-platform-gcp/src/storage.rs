//! The object store on Cloud Storage, reached as this process's own
//! service account.
//!
//! No key is ever made for it: the process's calls carry its access
//! token, and the links it hands out are signed by Google for that same
//! account (IAM's `signBlob`, which the account may call on itself). An
//! organization that forbids service-account keys, the default for one
//! created since 2024, runs it as is.
//!
//! Reads, writes and multipart uploads go through Cloud Storage's XML
//! API, which has S3's shape; listing goes through its JSON API, which
//! answers in JSON. Links are V4 signed URLs (`GOOG4-RSA-SHA256`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use anyhow::{bail, Context as _, Result};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use weft_platform_traits::object_store::MAX_PRESIGN_TTL_SECS;
use weft_platform_traits::{ObjectEntry, ObjectStore, PresignAudience};

use crate::api::{is_status, Google};

const HOST: &str = "storage.googleapis.com";

pub struct GcsObjectStore {
    google: Google,
    bucket: String,
    /// The service account the process runs as: the one links are signed
    /// for.
    account: String,
    /// Whether Google has signed for this process yet. Until it has, a
    /// refusal may be the install's own grant to sign still taking effect
    /// (a fresh install signs its first link, the standard library
    /// preload, about a minute after granting it), so it is waited out;
    /// after that, a refusal means the grant is gone.
    signed: AtomicBool,
}

impl GcsObjectStore {
    /// The store on `bucket`, as the account the metadata server names.
    /// Fails at boot, naming the cause, when the bucket cannot be reached.
    pub async fn new(google: Google, bucket: String) -> Result<Self> {
        let account = google.tokens().account_email().await?;
        let store = Self { google, bucket, account, signed: AtomicBool::new(false) };
        let probe = store.request(Method::GET, &format!("https://{HOST}/storage/v1/b/{}", store.bucket), &[]).await;
        if let Err(e) = Self::ok("reach the object-store bucket", probe?).await {
            let advice = if is_status(&e, 403) {
                format!(
                    ": grant {} roles/storage.objectAdmin and roles/storage.legacyBucketReader on it",
                    store.account
                )
            } else {
                String::new()
            };
            bail!("the object-store bucket '{}' is not reachable as {}{advice}\n{e:#}", store.bucket, store.account);
        }
        Ok(store)
    }

    fn object_url(&self, key: &str) -> String {
        format!("https://{HOST}{}", object_path(&self.bucket, key))
    }

    async fn request(&self, method: Method, url: &str, query: &[(&str, String)]) -> Result<reqwest::Response> {
        self.google.send_raw(self.google.http().request(method, url).query(query)).await
    }

    /// A write: Google refuses one without `Content-Length` (411), and an
    /// empty body goes out without it unless it is set by hand.
    async fn write(&self, req: reqwest::RequestBuilder, body: Bytes) -> Result<reqwest::Response> {
        self.google.send_raw(req.header(reqwest::header::CONTENT_LENGTH, body.len()).body(body)).await
    }

    /// The answer, when it is a success; Google's words otherwise.
    async fn ok(what: &str, resp: reqwest::Response) -> Result<reqwest::Response> {
        Google::success(resp).await.with_context(|| what.to_string())
    }

    async fn head(&self, key: &str) -> Result<Option<u64>> {
        let resp = self.request(Method::HEAD, &self.object_url(key), &[]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = Self::ok(&format!("look at {key}"), resp).await?;
        let size = resp
            .headers()
            .get("x-goog-stored-content-length")
            .or_else(|| resp.headers().get(reqwest::header::CONTENT_LENGTH))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .with_context(|| format!("Cloud Storage named no size for {key}"))?;
        Ok(Some(size))
    }

    /// A link to `method` on `key` with `query`, signed for this account,
    /// the body's exact length signed in when there is one.
    async fn sign(&self, method: &str, key: &str, query: &[(&str, String)], length: Option<u64>, ttl_secs: u64) -> Result<String> {
        if ttl_secs == 0 || ttl_secs > MAX_PRESIGN_TTL_SECS {
            bail!("a link to {key} lives 1 to {MAX_PRESIGN_TTL_SECS} seconds (7 days), not {ttl_secs}");
        }
        let link = LinkToSign {
            method,
            path: object_path(&self.bucket, key),
            query: query.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            length,
            account: &self.account,
            at: SystemTime::now(),
            ttl_secs,
        };
        let (to_sign, query) = link.string_to_sign();
        let url = format!("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:signBlob", self.account);
        let body = serde_json::json!({ "payload": base64::engine::general_purpose::STANDARD.encode(to_sign.as_bytes()) });
        let not_yet_granted = |e: &anyhow::Error| is_status(e, 403) && !self.signed.load(Ordering::Relaxed);
        let signed = crate::accounts::until_settled_within(&crate::accounts::GRANT_APPLIES, not_yet_granted, || {
            self.google.post(&url, &body)
        })
        .await
        .map_err(|e| {
            let advice = if is_status(&e, 403) { ": the account needs roles/iam.serviceAccountTokenCreator on itself" } else { "" };
            e.context(format!("sign a link as {}{advice}", self.account))
        })?;
        self.signed.store(true, Ordering::Relaxed);
        let blob = signed.get("signedBlob").and_then(|v| v.as_str()).context("signBlob answered no signedBlob")?;
        let signature = base64::engine::general_purpose::STANDARD.decode(blob).context("signBlob's signature is not base64")?;
        Ok(format!("https://{HOST}{}?{query}&X-Goog-Signature={}", link.path, hex(&signature)))
    }
}

#[async_trait]
impl ObjectStore for GcsObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        let resp = self.write(self.google.http().put(self.object_url(key)), bytes).await?;
        Self::ok(&format!("write {key}"), resp).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let resp = self.request(Method::GET, &self.object_url(key), &[]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::ok(&format!("read {key}"), resp).await?.bytes().await?))
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Option<Bytes>> {
        if end <= start {
            return Ok(self.head(key).await?.map(|_| Bytes::new()));
        }
        let resp = self
            .google
            .send_raw(self.google.http().get(self.object_url(key)).header(reqwest::header::RANGE, format!("bytes={start}-{}", end - 1)))
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::ok(&format!("read bytes {start}..{end} of {key}"), resp).await?.bytes().await?))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.head(key).await?.is_some())
    }

    async fn size(&self, key: &str) -> Result<Option<u64>> {
        self.head(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let resp = self.request(Method::DELETE, &self.object_url(key), &[]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Self::ok(&format!("delete {key}"), resp).await?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        #[derive(serde::Deserialize)]
        struct Page {
            #[serde(default)]
            items: Vec<Item>,
            #[serde(default, rename = "nextPageToken")]
            next: Option<String>,
        }
        #[derive(serde::Deserialize)]
        struct Item {
            name: String,
            size: String,
        }
        let url = format!("https://{HOST}/storage/v1/b/{}/o", self.bucket);
        let mut entries = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut query = vec![("prefix", prefix.to_string()), ("fields", "items(name,size),nextPageToken".to_string())];
            if let Some(t) = &page_token {
                query.push(("pageToken", t.clone()));
            }
            let resp = Self::ok(&format!("list {prefix}"), self.request(Method::GET, &url, &query).await?).await?;
            let page: Page = resp.json().await.with_context(|| format!("read the listing of {prefix}"))?;
            for item in page.items {
                let size = item.size.parse().with_context(|| format!("the size of {} is not a number", item.name))?;
                entries.push(ObjectEntry { key: item.name, size });
            }
            match page.next {
                Some(t) => page_token = Some(t),
                None => break,
            }
        }
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    // Every caller reaches Cloud Storage at the same address, so the
    // audience changes nothing.
    async fn presign_get(&self, key: &str, _audience: PresignAudience, ttl_secs: u64) -> Result<String> {
        self.sign("GET", key, &[], None, ttl_secs).await
    }

    async fn presign_put(&self, key: &str, content_length: u64, _audience: PresignAudience, ttl_secs: u64) -> Result<String> {
        self.sign("PUT", key, &[], Some(content_length), ttl_secs).await
    }

    async fn create_multipart(&self, key: &str) -> Result<String> {
        let resp = self.write(self.google.http().post(format!("{}?uploads", self.object_url(key))), Bytes::new()).await?;
        let body = Self::ok(&format!("start an upload of {key}"), resp).await?.text().await?;
        xml_text(&body, "UploadId").with_context(|| format!("Cloud Storage started an upload of {key} with no id: {body}"))
    }

    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        _audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String> {
        let query = [("partNumber", part_number.to_string()), ("uploadId", upload_id.to_string())];
        self.sign("PUT", key, &query, Some(part_size), ttl_secs).await
    }

    async fn upload_part(&self, key: &str, upload_id: &str, part_number: i32, bytes: Bytes) -> Result<String> {
        let req = self
            .google
            .http()
            .put(self.object_url(key))
            .query(&[("partNumber", part_number.to_string()), ("uploadId", upload_id.to_string())]);
        let resp = Self::ok(&format!("upload part {part_number} of {key}"), self.write(req, bytes).await?).await?;
        resp.headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .with_context(|| format!("Cloud Storage answered part {part_number} of {key} with no etag"))
    }

    async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> Result<u64> {
        let req = self
            .google
            .http()
            .post(self.object_url(key))
            .query(&[("uploadId", upload_id)])
            .header(reqwest::header::CONTENT_TYPE, "application/xml");
        Self::ok(&format!("complete the upload of {key}"), self.write(req, Bytes::from(complete_body(parts))).await?).await?;
        self.head(key).await?.with_context(|| format!("the completed upload of {key} is not there"))
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let resp = self.request(Method::DELETE, &self.object_url(key), &[("uploadId", upload_id.to_string())]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Self::ok(&format!("abort the upload of {key}"), resp).await?;
        Ok(())
    }

    async fn multipart_exists(&self, key: &str, upload_id: &str) -> Result<bool> {
        let resp = self.request(Method::GET, &self.object_url(key), &[("uploadId", upload_id.to_string())]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        Self::ok(&format!("look at the upload of {key}"), resp).await?;
        Ok(true)
    }
}

/// `/<bucket>/<key>`, each segment of the key percent-encoded.
fn object_path(bucket: &str, key: &str) -> String {
    let key: Vec<String> = key.split('/').map(encode).collect();
    format!("/{bucket}/{}", key.join("/"))
}

/// RFC 3986 encoding: everything but the unreserved characters.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The text of the first `<tag>` in `xml`.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&format!("</{tag}>"))?;
    Some(xml[start..end].to_string())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn complete_body(parts: &[(i32, String)]) -> String {
    let mut body = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        body.push_str(&format!("<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>", xml_escape(etag)));
    }
    body.push_str("</CompleteMultipartUpload>");
    body
}

/// Everything a V4 signed URL signs, before Google signs it.
struct LinkToSign<'a> {
    method: &'a str,
    path: String,
    query: Vec<(String, String)>,
    length: Option<u64>,
    account: &'a str,
    at: SystemTime,
    ttl_secs: u64,
}

impl LinkToSign<'_> {
    /// What Google signs, and the link's query string without its
    /// signature.
    fn string_to_sign(&self) -> (String, String) {
        let at: chrono::DateTime<chrono::Utc> = self.at.into();
        let date = at.format("%Y%m%d").to_string();
        let stamp = at.format("%Y%m%dT%H%M%SZ").to_string();
        let scope = format!("{date}/auto/storage/goog4_request");
        let mut headers = vec![("host", HOST.to_string())];
        if let Some(length) = self.length {
            headers.insert(0, ("content-length", length.to_string()));
        }
        let signed_headers = headers.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
        let mut query = self.query.clone();
        query.extend([
            ("X-Goog-Algorithm".to_string(), "GOOG4-RSA-SHA256".to_string()),
            ("X-Goog-Credential".to_string(), format!("{}/{scope}", self.account)),
            ("X-Goog-Date".to_string(), stamp.clone()),
            ("X-Goog-Expires".to_string(), self.ttl_secs.to_string()),
            ("X-Goog-SignedHeaders".to_string(), signed_headers.clone()),
        ]);
        let mut encoded: Vec<(String, String)> = query.iter().map(|(k, v)| (encode(k), encode(v))).collect();
        encoded.sort();
        let query = encoded.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
        let canonical = [self.method, &self.path, &query, &canonical_headers, &signed_headers, "UNSIGNED-PAYLOAD"].join("\n");
        let to_sign = format!("GOOG4-RSA-SHA256\n{stamp}\n{scope}\n{}", hex(&Sha256::digest(canonical.as_bytes())));
        (to_sign, query)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn link(length: Option<u64>, query: Vec<(String, String)>) -> LinkToSign<'static> {
        LinkToSign {
            method: "PUT",
            path: object_path("files", "runtime/t 1/a.txt"),
            query,
            length,
            account: "core@p.iam.gserviceaccount.com",
            at: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            ttl_secs: 900,
        }
    }

    #[test]
    fn a_key_is_encoded_segment_by_segment() {
        assert_eq!(object_path("files", "runtime/t 1/a+b.txt"), "/files/runtime/t%201/a%2Bb.txt");
    }

    #[test]
    fn a_signed_upload_carries_its_length_and_the_query_in_order() {
        let (to_sign, query) = link(Some(42), vec![("uploadId".into(), "u/1".into()), ("partNumber".into(), "3".into())]).string_to_sign();
        assert!(to_sign.starts_with("GOOG4-RSA-SHA256\n20231114T221320Z\n20231114/auto/storage/goog4_request\n"), "{to_sign}");
        assert_eq!(
            query,
            "X-Goog-Algorithm=GOOG4-RSA-SHA256\
             &X-Goog-Credential=core%40p.iam.gserviceaccount.com%2F20231114%2Fauto%2Fstorage%2Fgoog4_request\
             &X-Goog-Date=20231114T221320Z&X-Goog-Expires=900&X-Goog-SignedHeaders=content-length%3Bhost\
             &partNumber=3&uploadId=u%2F1"
        );
    }

    #[test]
    fn a_signed_download_signs_only_the_host() {
        let (_, query) = link(None, Vec::new()).string_to_sign();
        assert!(query.ends_with("X-Goog-SignedHeaders=host"), "{query}");
    }

    #[test]
    fn the_length_changes_what_is_signed() {
        assert_ne!(link(Some(1), Vec::new()).string_to_sign().0, link(Some(2), Vec::new()).string_to_sign().0);
    }

    #[test]
    fn an_upload_id_is_read_and_a_completion_lists_every_part() {
        let xml = "<?xml version='1.0'?><InitiateMultipartUploadResult><Bucket>b</Bucket><UploadId>ABC-1</UploadId></InitiateMultipartUploadResult>";
        assert_eq!(xml_text(xml, "UploadId").as_deref(), Some("ABC-1"));
        assert_eq!(
            complete_body(&[(1, "\"e1\"".into()), (2, "\"e&2\"".into())]),
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"e&amp;2\"</ETag></Part></CompleteMultipartUpload>"
        );
    }
}
