//! Memorable random names for browser-extension tokens.
//!
//! Pattern: `<adjective>-<noun>-<NN>` where `NN` is a 0-99 number,
//! e.g. `swift-falcon-23`. Pulled directly from v1's dashboard
//! token-creation page so users who used the v1 flow recognise the
//! shape. Output collisions are possible (~96 × 108 × 100 ≈ 1M, so
//! ~0.1% chance of a dup at 30 tokens by birthday paradox), which is
//! acceptable: tokens themselves are uuid-distinct, the name is
//! purely cosmetic.
//!
//! Randomness comes from `uuid::Uuid::new_v4()` to avoid pulling in
//! the `rand` crate just for this. We hex-decode 8 bytes of a fresh
//! v4 uuid and reduce mod len.

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
    "crane", "creek", "crystal", "dawn", "delta", "dolphin", "dove", "dragon",
    "dream", "dune", "eagle", "echo", "ember", "falcon", "fern", "field",
    "flame", "flower", "forest", "fountain", "fox", "frost", "garden", "glacier",
    "grove", "harbor", "hawk", "heart", "hill", "horizon", "island", "jade",
    "jewel", "jungle", "lake", "leaf", "light", "lion", "lotus", "maple",
    "meadow", "meteor", "moon", "mountain", "nebula", "nest", "night", "oak",
    "ocean", "orchid", "otter", "owl", "palm", "panda", "path", "peak",
    "pearl", "phoenix", "pine", "planet", "pond", "prism", "quartz", "rain",
    "rainbow", "raven", "reef", "ridge", "river", "robin", "rose", "sage",
    "salmon", "sand", "shadow", "shore", "sky", "snow", "spark", "spring",
    "star", "stone", "storm", "stream", "summit", "sun", "swan", "thunder",
    "tiger", "trail", "tree", "valley", "wave", "willow", "wind", "wolf",
];

/// Generate a memorable name like `swift-falcon-23`. Each call
/// pulls a fresh v4 uuid and reduces three of its bytes modulo
/// the wordlist sizes; collisions are possible but cosmetic.
pub fn random_name() -> String {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    let adj = ADJECTIVES[(bytes[0] as usize) % ADJECTIVES.len()];
    let noun = NOUNS[(bytes[1] as usize) % NOUNS.len()];
    let num = (bytes[2] as u16) % 100;
    format!("{adj}-{noun}-{num:02}")
}

/// Friendly extension token: `wm_tk_<random-name>`. The full
/// token IS the user-visible string they paste into the browser
/// extension, so we keep it readable. ~1M combinations across the
/// wordlist; the prefix lives outside that for routing clarity.
pub fn friendly_token() -> String {
    format!("wm_tk_{}", random_name())
}

/// Hard extension token: `wm_tk_<32-hex>`. High entropy. Use when
/// the dispatcher is reachable beyond localhost.
pub fn hard_token() -> String {
    format!("wm_tk_{}", uuid::Uuid::new_v4().simple())
}
