//! Signal-token value generation + at-rest hashing.
//!
//! A signal token is a bearer credential an external system holds to reach a
//! project's signals. ONE generation path, fully machine-generated:
//!
//!   `wft-<w1>-<w2>-<w3>-<w4>-<w5>-<w6>`
//!
//! The fixed `wft-` prefix makes the string recognizable as a weft signal
//! token at a glance (the `sk-` idea: humans and secret scanners both spot
//! it); the six words are the secret, drawn UNBIASED (rejection sampling)
//! from the combined 203-word pool (95 adjectives + 108 nouns, all DISTINCT so
//! every index maps to a unique word): 203^6 ≈ 7.0e13 combinations (~46 bits).
//! It still reads like words (`wft-azure-otter-brave-summit-river-maple`),
//! never a scary base64/uuid blob. (A future wrong-guess rate limiter is the
//! defense-in-depth on top; see the weft TODO.)
//!
//! Show-once at rest: the server stores only `token_hash` (sha256 hex of the
//! full string) plus a display `recognizer` (`wft-<w1>-…`). The full value
//! exists exactly once, in the mint response; no endpoint can re-reveal it,
//! and a DB dump exposes no usable credential.
//!
//! The user-facing NAME is pure metadata (a DB column, editable anytime),
//! never part of the token string: embedding it would make a rename change
//! the credential.
//!
//! Randomness comes from `uuid::Uuid::new_v4()` (16 fresh random bytes per
//! call) to avoid pulling in the `rand` crate. Bytes that would bias the
//! word choice (a 256-wide byte reduced mod a 203-wide pool makes the low
//! indices ~2x likelier) are REJECTED and redrawn, so every word is uniform.

use sha2::{Digest, Sha256};

/// The fixed, recognizable prefix every signal token starts with.
pub const TOKEN_PREFIX: &str = "wft-";

/// How many secret words a token carries.
const TOKEN_WORDS: usize = 6;

const ADJECTIVES: &[&str] = &[
    "swift", "bright", "calm", "bold", "clever", "cosmic", "crystal", "dancing",
    "daring", "dreamy", "eager", "electric", "emerald", "endless", "epic", "eternal",
    "fierce", "flying", "frozen", "gentle", "glowing", "golden", "graceful", "happy",
    "hidden", "humble", "icy", "infinite", "jade", "jolly", "keen", "kind",
    "lively", "lucky", "lunar", "magic", "mellow", "mighty", "misty", "noble",
    "ocean", "peaceful", "playful", "polar", "proud", "quantum", "quiet", "radiant",
    "rapid", "rising", "roaming", "royal", "ruby", "rustic", "sacred", "serene",
    "shiny", "silent", "silver", "sleek", "smooth", "snowy", "solar", "sonic",
    "sparkling", "speedy", "stellar", "stormy", "sunny", "tender", "thunder",
    "tidal", "timeless", "tranquil", "twilight", "urban", "velvet", "vibrant", "vivid",
    "wandering", "warm", "wavy", "wild", "windy", "winter", "wise", "witty",
    "wonder", "wooden", "young", "zealous", "zen", "zesty", "zippy", "azure",
];

const NOUNS: &[&str] = &[
    "anchor", "arrow", "aurora", "beacon", "bear", "bird", "bloom", "breeze",
    "bridge", "brook", "canyon", "castle", "cedar", "cloud", "comet", "coral",
    "crane", "creek", "dawn", "delta", "dolphin", "dove", "dragon",
    "dream", "dune", "eagle", "echo", "ember", "falcon", "fern", "field",
    "flame", "flower", "forest", "fountain", "fox", "frost", "garden", "glacier",
    "grove", "harbor", "hawk", "heart", "hill", "horizon", "island",
    "jewel", "jungle", "lake", "leaf", "light", "lion", "lotus", "maple",
    "meadow", "meteor", "moon", "mountain", "nebula", "nest", "night", "oak",
    "orchid", "otter", "owl", "palm", "panda", "path", "peak",
    "pearl", "phoenix", "pine", "planet", "pond", "prism", "quartz", "rain",
    "rainbow", "raven", "reef", "ridge", "river", "robin", "rose", "sage",
    "salmon", "sand", "shadow", "shore", "sky", "snow", "spark", "spring",
    "star", "stone", "storm", "stream", "summit", "sun", "swan",
    "tiger", "trail", "tree", "valley", "wave", "willow", "wind", "wolf",
];

/// A word from the combined pool at index `i` (`i < pool_len()`).
fn word_at(i: usize) -> &'static str {
    if i < ADJECTIVES.len() {
        ADJECTIVES[i]
    } else {
        NOUNS[i - ADJECTIVES.len()]
    }
}

fn pool_len() -> usize {
    ADJECTIVES.len() + NOUNS.len()
}

/// Generate a fresh token: `wft-` + six UNIFORMLY drawn words. Rejection
/// sampling: a random byte is used only when it falls inside the largest
/// multiple of the pool size (bytes past it would over-represent the low
/// indices); rejected bytes are discarded and more randomness is drawn.
pub fn generate_token() -> String {
    let pool = pool_len();
    debug_assert!(pool <= 256, "byte-wide rejection sampling assumes pool <= 256");
    // Largest multiple of `pool` that fits in a byte range: accept b < limit.
    let limit = (256 / pool) * pool;
    let mut words = Vec::with_capacity(TOKEN_WORDS);
    while words.len() < TOKEN_WORDS {
        for b in uuid::Uuid::new_v4().into_bytes() {
            if (b as usize) < limit {
                words.push(word_at(b as usize % pool));
                if words.len() == TOKEN_WORDS {
                    break;
                }
            }
        }
    }
    format!("{TOKEN_PREFIX}{}", words.join("-"))
}

/// The at-rest form of a token: sha256 of the full string, lowercase hex.
/// The ONLY token-derived value the DB ever stores; lookups hash the
/// presented credential and match this (a fixed-width digest compare).
pub fn token_hash(token: &str) -> String {
    let digest = Sha256::new().chain_update(token.as_bytes()).finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push(char::from_digit((b >> 4) as u32, 16).expect("nibble < 16"));
        out.push(char::from_digit((b & 0xf) as u32, 16).expect("nibble < 16"));
    }
    out
}

/// The display recognizer for a token: the prefix plus its first word
/// (`wft-azure-…`). Enough for a user to tell tokens apart in a list, never
/// enough to reconstruct the secret (5 more uniform words remain).
pub fn recognizer(token: &str) -> String {
    let after_prefix = token.strip_prefix(TOKEN_PREFIX).unwrap_or(token);
    let first = after_prefix.split('-').next().unwrap_or("");
    format!("{TOKEN_PREFIX}{first}-…")
}

/// The longest a token with an expiry may live: a year.
pub const MAX_EXPIRY_SECS: u64 = 365 * 24 * 3600;

/// When a token minted at `now_unix` to live `expires_in_secs` stops
/// working, in unix seconds. Refuses 0 (a token that never works) and
/// anything over [`MAX_EXPIRY_SECS`]. The one check every mint (the
/// dispatcher's `weft token`, the broker's instance tokens) makes.
pub fn expiry_at(now_unix: u64, expires_in_secs: u64) -> Result<u64, String> {
    if expires_in_secs == 0 || expires_in_secs > MAX_EXPIRY_SECS {
        return Err(format!(
            "a token lives between 1 second and {MAX_EXPIRY_SECS} seconds (a year), not {expires_in_secs}"
        ));
    }
    now_unix
        .checked_add(expires_in_secs)
        .ok_or_else(|| format!("a clock at {now_unix} plus {expires_in_secs} seconds is past what a timestamp holds"))
}

crate::wire_enum! {
    /// What a token may do. One table, one hash, one mint/list/revoke
    /// surface for both, and the kind is checked at each door: a caller
    /// token never opens the admin surface, and an operator key is never
    /// taken on the outside-caller doors, so a frontend's leaked token can
    /// never administer the install and an admin key is never pasted into
    /// a frontend.
    /// SYNC: kind column values <-> the CHECK on signal_token.kind in crates/weft-dispatcher/src/journal/postgres.rs GROUP
    pub enum TokenKind {
        /// An outside caller's scoped credential: signals, displays.
        Caller = "caller",
        /// Full admin of the tenant: every CLI and editor verb.
        Operator = "operator",
    }
}

/// A listed token (`GET /signal-tokens`): metadata + recognizer only,
/// no secret. What the install answers and the CLI reads.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenSummary {
    pub id: uuid::Uuid,
    pub kind: TokenKind,
    pub recognizer: String,
    pub name: Option<String>,
    #[serde(rename = "createdAtUnix")]
    pub created_at_unix: u64,
    #[serde(rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
    #[serde(rename = "allowedDisplays")]
    pub allowed_displays: Vec<String>,
    #[serde(rename = "allDisplays")]
    pub all_displays: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    #[serde(rename = "expiresAtUnix", default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix: Option<u64>,
}

/// `POST /signal-tokens`: mint a token.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MintTokenRequest {
    /// The user-facing label (a display name), never part of the token value.
    #[serde(default)]
    pub name: Option<String>,
    /// Scope vectors. Empty = wildcard. Each non-empty vector narrows
    /// the signals this token can enumerate. Tags must pass the tag
    /// charset (`[A-Za-z0-9_-]{1,64}`); rejected at parse time.
    #[serde(default, rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(default, rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
    /// The display dimension, which does NOT take the wildcard-on-empty
    /// rule: a token says which node displays it may read, or reads
    /// none. `allDisplays` is the wildcard within the token's projects.
    #[serde(default, rename = "allowedDisplays")]
    pub allowed_displays: Vec<String>,
    #[serde(default, rename = "allDisplays")]
    pub all_displays: bool,
    /// An instance token: acts inside this one instance of its one project
    /// (`allowedProjects` names exactly that one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    /// How long the token works, in seconds from now. Required for an
    /// instance token (it lives in a browser); optional otherwise.
    #[serde(default, rename = "expiresInSecs", skip_serializing_if = "Option::is_none")]
    pub expires_in_secs: Option<u64>,
    /// What the token may do. Absent is a caller token, the scoped
    /// credential; `operator` is an admin key and takes no scope.
    #[serde(default = "caller_kind")]
    pub kind: TokenKind,
}

impl MintTokenRequest {
    /// A caller token named `name`, scoped to nothing yet.
    pub fn caller(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            allowed_projects: Vec::new(),
            allowed_tags: Vec::new(),
            allowed_displays: Vec::new(),
            all_displays: false,
            instance: None,
            expires_in_secs: None,
            kind: TokenKind::Caller,
        }
    }
}

fn caller_kind() -> TokenKind {
    TokenKind::Caller
}

/// The mint answer: the ONLY place the full token value ever appears.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MintedToken {
    pub id: uuid::Uuid,
    pub kind: TokenKind,
    /// The full secret, shown once. The client copies it now; the server
    /// keeps only its hash and can never show it again.
    pub token: String,
    pub recognizer: String,
    pub name: Option<String>,
    /// The paste-able connect string for clients that take one URL
    /// (`<public-base>/signal-token/<token>`): clients PARSE it into base +
    /// token and present the token via `Authorization: Bearer`; the wire
    /// requests never carry it in a path.
    pub url: String,
    #[serde(rename = "allowedProjects")]
    pub allowed_projects: Vec<uuid::Uuid>,
    #[serde(rename = "allowedTags")]
    pub allowed_tags: Vec<String>,
    #[serde(rename = "allowedDisplays")]
    pub allowed_displays: Vec<String>,
    #[serde(rename = "allDisplays")]
    pub all_displays: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    #[serde(rename = "expiresAtUnix", default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(TokenKind);

    #[test]
    fn an_expiry_is_between_a_second_and_a_year_from_now() {
        assert_eq!(expiry_at(100, 60), Ok(160));
        assert_eq!(expiry_at(100, MAX_EXPIRY_SECS), Ok(100 + MAX_EXPIRY_SECS));
        assert!(expiry_at(100, 0).unwrap_err().contains("between 1 second"));
        assert!(expiry_at(100, MAX_EXPIRY_SECS + 1).unwrap_err().contains("between 1 second"));
        assert!(expiry_at(u64::MAX, 1).unwrap_err().contains("past what a timestamp holds"));
    }

    #[test]
    fn generated_tokens_have_the_documented_shape() {
        for _ in 0..64 {
            let t = generate_token();
            assert!(t.starts_with(TOKEN_PREFIX));
            let words: Vec<&str> = t[TOKEN_PREFIX.len()..].split('-').collect();
            assert_eq!(words.len(), TOKEN_WORDS);
            for w in words {
                assert!(
                    ADJECTIVES.contains(&w) || NOUNS.contains(&w),
                    "unknown word {w} in {t}"
                );
            }
        }
    }

    #[test]
    fn tokens_are_distinct() {
        let a = generate_token();
        let b = generate_token();
        assert_ne!(a, b, "two fresh tokens must not collide");
    }

    #[test]
    fn hash_is_stable_and_hex() {
        let t = "wft-azure-otter-brave-summit-river-maple";
        let h = token_hash(t);
        assert_eq!(h.len(), 64);
        assert_eq!(h, token_hash(t), "same input, same digest");
        assert!(h.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(h, token_hash("wft-azure-otter-brave-summit-river-oak"));
    }

    #[test]
    fn recognizer_reveals_only_the_first_word() {
        let t = "wft-azure-otter-brave-summit-river-maple";
        assert_eq!(recognizer(t), "wft-azure-…");
        assert!(!recognizer(t).contains("otter"));
    }
}
