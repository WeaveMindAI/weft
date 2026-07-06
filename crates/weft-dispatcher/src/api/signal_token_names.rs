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

#[cfg(test)]
mod tests {
    use super::*;

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
