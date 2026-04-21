use serde::{Deserialize, Serialize};
use ts_rs::TS;

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Document
// Parameterized:  List[T], Dict[K, V]
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio | Document
// Type variables: T, T1, T2... : node-scoped, same T on input and output = same type
// MustOverride:   Node can't know the type, user/AI must declare it in Weft code
//
// Port types describe what the node sees post-operation:
//   Expand input (<): declared type is T (element). Compiler validates List[T] arrives.
//   Gather input (>): declared type is List[T] (collected). Compiler validates stack context.
//   Stack depth is tracked by the compiler, NOT in the type system.
//
// In backend.rs node definitions, types are string literals:
//   PortDef::new("name", "String", true)
//   PortDef::new("items", "List[String]", false)
//   PortDef::new("headers", "Dict[String, String]", false)
//   PortDef::new("value", "T", false)           : type variable
//   PortDef::new("value", "MustOverride", false) : user must declare type
// =============================================================================

macro_rules! define_primitives {
    ($($variant:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
        #[ts(export)]
        pub enum WeftPrimitive {
            $($variant),+
        }

        impl WeftPrimitive {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(WeftPrimitive::$variant => stringify!($variant)),+
                }
            }

            pub fn from_str(s: &str) -> Option<Self> {
                match s {
                    $(stringify!($variant) => Some(WeftPrimitive::$variant)),+,
                    _ => None,
                }
            }
        }
    };
}

define_primitives!(
    String,
    Number,
    Boolean,
    Null,
    Image,
    Video,
    Audio,
    Document,
    Empty,
);

impl std::fmt::Display for WeftPrimitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}


/// Recursive port type system.
#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(export)]
pub enum WeftType {
    /// Scalar: String, Number, Boolean, Image, Video, Audio, Document
    Primitive(WeftPrimitive),
    /// Homogeneous list: List[T]
    List(Box<WeftType>),
    /// Key-value dict: Dict[K, V]
    Dict(Box<WeftType>, Box<WeftType>),
    /// Union: String | Number
    Union(Vec<WeftType>),
    /// Opaque JSON dict: Dict[String, *] where value types are unchecked.
    /// Compatible with any Dict[String, V] in both directions.
    /// Use for raw API responses where the shape is unknown or too complex to declare.
    JsonDict,
    /// Node-scoped type variable: T, T1, T2, etc.
    /// Same name on different ports of the same node = same type.
    /// Resolved per-node when connections are made.
    TypeVar(std::string::String),
    /// Node cannot determine the type. User/AI must override in Weft code.
    /// Remaining MustOverride at compile time = error.
    MustOverride,
}

impl WeftType {
    // ── Constructors ────────────────────────────────────────────────────

    pub fn primitive(p: WeftPrimitive) -> Self {
        WeftType::Primitive(p)
    }
    pub fn list(inner: WeftType) -> Self {
        WeftType::List(Box::new(inner))
    }

    pub fn dict(key: WeftType, value: WeftType) -> Self {
        WeftType::Dict(Box::new(key), Box::new(value))
    }

    pub fn union(mut types: Vec<WeftType>) -> Self {
        let mut flat = Vec::new();
        for t in types.drain(..) {
            match t {
                WeftType::Union(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        // Remove duplicates (not just consecutive : handles T1=String, T2=String)
        let mut seen = Vec::new();
        for t in flat {
            if !seen.contains(&t) {
                seen.push(t);
            }
        }
        // Remove Empty (bottom type) from unions with other types.
        // Empty adds nothing to a union: `Number | Empty` = `Number`.
        if seen.len() > 1 {
            seen.retain(|t| !matches!(t, WeftType::Primitive(WeftPrimitive::Empty)));
        }
        if seen.len() == 1 {
            seen.into_iter().next().unwrap()
        } else {
            WeftType::Union(seen)
        }
    }

    pub fn union_primitives(prims: Vec<WeftPrimitive>) -> Self {
        Self::union(prims.into_iter().map(WeftType::Primitive).collect())
    }

    pub fn media() -> Self {
        Self::union(vec![
            WeftType::Primitive(WeftPrimitive::Image),
            WeftType::Primitive(WeftPrimitive::Video),
            WeftType::Primitive(WeftPrimitive::Audio),
            WeftType::Primitive(WeftPrimitive::Document),
        ])
    }

    pub fn type_var(name: &str) -> Self {
        WeftType::TypeVar(name.to_string())
    }

    pub fn json_dict() -> Self {
        WeftType::JsonDict
    }

    pub fn must_override() -> Self {
        WeftType::MustOverride
    }

    // ── Queries ─────────────────────────────────────────────────────────

    pub fn is_type_var(&self) -> bool {
        matches!(self, WeftType::TypeVar(_))
    }

    pub fn is_must_override(&self) -> bool {
        matches!(self, WeftType::MustOverride)
    }

    /// Returns true for TypeVar or MustOverride : types not yet resolved to concrete
    pub fn is_unresolved(&self) -> bool {
        matches!(self, WeftType::TypeVar(_) | WeftType::MustOverride)
    }

    /// Returns true if the type includes Null as a valid value.
    /// This means null is a legitimate data value, not a skip signal.
    /// Whether a port of this type should be configurable by default.
    /// Returns false only for Media primitives (Image/Audio/Video/Document,
    /// alone or in containers), TypeVar, and MustOverride. Everything else ,
    /// primitives, lists, dicts, JsonDict, and unions of the above, is
    /// configurable by default so users can paste literal JSON into the
    /// config field instead of wiring a separate Text node. Catalog authors
    /// override per port via `PortDef::wired_only(...)` when the node needs
    /// a runtime-only value (e.g. a streaming handle).
    pub fn is_default_configurable(&self) -> bool {
        match self {
            WeftType::Primitive(p) => !matches!(
                p,
                WeftPrimitive::Image
                    | WeftPrimitive::Video
                    | WeftPrimitive::Audio
                    | WeftPrimitive::Document
            ),
            WeftType::List(inner) => inner.is_default_configurable(),
            WeftType::Dict(_, v) => v.is_default_configurable(),
            WeftType::Union(types) => types.iter().all(|t| t.is_default_configurable()),
            WeftType::JsonDict => true,
            WeftType::TypeVar(_) => false,
            WeftType::MustOverride => false,
        }
    }

    pub fn contains_null(&self) -> bool {
        match self {
            WeftType::Primitive(WeftPrimitive::Null) => true,
            WeftType::Union(types) => types.iter().any(|t| t.contains_null()),
            _ => false,
        }
    }

    /// Remove Null from a type. `String | Null` becomes `String`.
    /// A bare `Null` becomes `Null` (cannot strip further).
    /// Non-union types without Null are returned unchanged.
    /// Note: only strips from flat unions (direct children). Unions are
    /// always flattened by `WeftType::union()`, so nested unions do not
    /// occur in practice. If that invariant ever changes, this method
    /// should be made recursive to match `contains_null()`.
    pub fn without_null(&self) -> WeftType {
        match self {
            WeftType::Union(types) => {
                let filtered: Vec<WeftType> = types.iter()
                    .filter(|t| !matches!(t, WeftType::Primitive(WeftPrimitive::Null)))
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    self.clone()
                } else if filtered.len() == 1 {
                    filtered.into_iter().next().unwrap()
                } else {
                    WeftType::Union(filtered)
                }
            }
            _ => self.clone(),
        }
    }

    /// For an expand port: extract the element type from List[T] → T.
    /// Returns None if the type is not a List (expand requires List input).
    pub fn expand_element_type(&self) -> Option<WeftType> {
        match self {
            WeftType::List(inner) => Some(*inner.clone()),
            WeftType::TypeVar(_) => Some(self.clone()),
            _ => None,
        }
    }

    // ── Compatibility ───────────────────────────────────────────────────

    /// Compile-time compatibility: can a value of type `source` flow into a port of type `target`?
    /// Stack depth is NOT checked here : it's handled by the expand/gather validation pass.
    pub fn is_compatible(source: &WeftType, target: &WeftType) -> bool {
        if source.is_unresolved() || target.is_unresolved() {
            return true;
        }
        // Empty (bottom type from empty containers) is compatible with anything as source
        if matches!(source, WeftType::Primitive(WeftPrimitive::Empty)) {
            return true;
        }

        match (source, target) {
            (WeftType::Primitive(a), WeftType::Primitive(b)) => a == b,
            (WeftType::List(a), WeftType::List(b)) => Self::is_compatible(a, b),
            (WeftType::Dict(ak, av), WeftType::Dict(bk, bv)) => {
                Self::is_compatible(ak, bk) && Self::is_compatible(av, bv)
            }
            // JsonDict: compatible with any Dict[String, V] in both directions
            (WeftType::JsonDict, WeftType::JsonDict) => true,
            (WeftType::JsonDict, WeftType::Dict(k, _)) => {
                matches!(k.as_ref(), WeftType::Primitive(WeftPrimitive::String))
            }
            (WeftType::Dict(k, _), WeftType::JsonDict) => {
                matches!(k.as_ref(), WeftType::Primitive(WeftPrimitive::String))
            }
            // Both unions: every source variant must match at least one target variant
            (WeftType::Union(sources), WeftType::Union(targets)) => {
                sources.iter().all(|s| targets.iter().any(|t| Self::is_compatible(s, t)))
            }
            // Single into union: must match at least one variant
            (src, WeftType::Union(targets)) => {
                targets.iter().any(|t| Self::is_compatible(src, t))
            }
            // Union into single: all variants must be compatible
            (WeftType::Union(sources), tgt) => {
                sources.iter().all(|s| Self::is_compatible(s, tgt))
            }
            _ => false,
        }
    }

    // ── Type inference from values ────────────────────────────────────────

    /// Infer a WeftType from a runtime JSON value.
    /// Produces the most specific type in our type system.
    /// Arrays are typed as List[T] where T is the union of all element types.
    /// Objects with url+mimeType are detected as Image/Video/Audio/Document.
    /// Other objects are typed as Dict[String, V] where V is the union of all value types.
    pub fn infer(value: &serde_json::Value) -> WeftType {
        match value {
            serde_json::Value::Null => WeftType::Primitive(WeftPrimitive::Null),
            serde_json::Value::Bool(_) => WeftType::Primitive(WeftPrimitive::Boolean),
            serde_json::Value::Number(_) => WeftType::Primitive(WeftPrimitive::Number),
            serde_json::Value::String(_) => WeftType::Primitive(WeftPrimitive::String),
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    return WeftType::List(Box::new(WeftType::Primitive(WeftPrimitive::Empty)));
                }
                let element_types: Vec<WeftType> = arr.iter().map(Self::infer).collect();
                let unified = Self::unify_types(&element_types);
                WeftType::List(Box::new(unified))
            }
            serde_json::Value::Object(obj) => {
                // Detect media objects: {url, mimeType, ...}
                if let Some(media_type) = Self::detect_media_type(obj) {
                    return media_type;
                }
                if obj.is_empty() {
                    return WeftType::Dict(
                        Box::new(WeftType::Primitive(WeftPrimitive::String)),
                        Box::new(WeftType::Primitive(WeftPrimitive::Empty)),
                    );
                }
                let value_types: Vec<WeftType> = obj.values().map(Self::infer).collect();
                let unified_value = Self::unify_types(&value_types);
                WeftType::Dict(
                    Box::new(WeftType::Primitive(WeftPrimitive::String)),
                    Box::new(unified_value),
                )
            }
        }
    }

    /// Detect if an object is a media type (Image, Video, Audio, Document).
    fn detect_media_type(obj: &serde_json::Map<String, serde_json::Value>) -> Option<WeftType> {
        let has_url = obj.contains_key("url") || obj.contains_key("data");
        if !has_url { return None; }
        let mime = obj.get("mimeType").or_else(|| obj.get("mimetype"))
            .and_then(|m| m.as_str())?;
        if mime.starts_with("image/") { Some(WeftType::Primitive(WeftPrimitive::Image)) }
        else if mime.starts_with("video/") { Some(WeftType::Primitive(WeftPrimitive::Video)) }
        else if mime.starts_with("audio/") { Some(WeftType::Primitive(WeftPrimitive::Audio)) }
        else { Some(WeftType::Primitive(WeftPrimitive::Document)) }
    }

    /// Unify a list of types into a single type.
    /// If all are identical, return that type. Otherwise, return a Union (deduplicated).
    fn unify_types(types: &[WeftType]) -> WeftType {
        if types.is_empty() {
            return WeftType::Primitive(WeftPrimitive::Empty);
        }
        let mut unique: Vec<WeftType> = Vec::new();
        for t in types {
            match t {
                WeftType::Union(variants) => {
                    for v in variants {
                        if !unique.iter().any(|u| Self::is_compatible(v, u) && Self::is_compatible(u, v)) {
                            unique.push(v.clone());
                        }
                    }
                }
                _ => {
                    if !unique.iter().any(|u| Self::is_compatible(t, u) && Self::is_compatible(u, t)) {
                        unique.push(t.clone());
                    }
                }
            }
        }
        if unique.len() == 1 {
            unique.pop().unwrap()
        } else {
            WeftType::Union(unique)
        }
    }

    // ── Parsing ─────────────────────────────────────────────────────────

    /// Parse a port type string. Strict : no bare List/Dict/Any.
    ///
    /// Valid: "String", "List[String]", "Dict[String, Number]",
    ///        "String | Number", "Media", "T", "T1", "T2", "MustOverride",
    ///        "List[T]", "Dict[String, T1 | T2]"
    /// Invalid: "Any", "List", "Dict", "Foo"
    pub fn parse(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return None;
        }

        let parts = split_top_level(trimmed, '|');
        if parts.len() > 1 {
            let types: Option<Vec<WeftType>> = parts.iter()
                .map(|p| parse_single_type(p.trim()))
                .collect();
            return Some(WeftType::union(types?));
        }

        parse_single_type(trimmed)
    }
}

fn parse_single_type(s: &str) -> Option<WeftType> {
    let s = s.trim();

    if s == "Media" {
        return Some(WeftType::media());
    }

    if s == "JsonDict" {
        return Some(WeftType::JsonDict);
    }

    if s == "MustOverride" {
        return Some(WeftType::MustOverride);
    }

    // Parameterized: List[T], Dict[K, V]
    if let Some(bracket_pos) = s.find('[') {
        if !s.ends_with(']') {
            return None;
        }
        let name = s[..bracket_pos].trim();
        let inner = &s[bracket_pos + 1..s.len() - 1];

        match name {
            "List" => {
                let inner_type = WeftType::parse(inner)?;
                Some(WeftType::List(Box::new(inner_type)))
            }
            "Dict" => {
                let parts = split_top_level(inner, ',');
                if parts.len() != 2 {
                    return None;
                }
                let key = WeftType::parse(parts[0].trim())?;
                let val = WeftType::parse(parts[1].trim())?;
                Some(WeftType::Dict(Box::new(key), Box::new(val)))
            }
            _ => None,
        }
    } else {
        // Try primitive first
        if let Some(p) = WeftPrimitive::from_str(s) {
            return Some(WeftType::Primitive(p));
        }
        // Type variable: T, T1, T2, ... (starts with uppercase T, optionally followed by digits)
        if is_type_var_name(s) {
            return Some(WeftType::TypeVar(s.to_string()));
        }
        None
    }
}

/// Type variable names users can write: T, T1, T2, ..., T99.
///
/// Also accepted (catalog-internal only, not user-facing):
///   - `T_Auto`: sentinel emitted by catalog helpers like `FormFieldPort::any`
///     to request a per-port-instance TypeVar. Replaced with `T__{key}` at
///     enrichment time.
///   - `T__scope` (e.g. `T__hook`): materialized form of a `T_Auto` marker,
///     scoped to a specific port instance. Must round-trip through the parser
///     because the frontend representation of port types is a string.
///
/// The internal forms exist so catalog authors can express "this port accepts
/// anything, independently from sibling ports" without forcing the same rule
/// on nodes that genuinely want shared `T` semantics (Gate, future Zip, etc.).
fn is_type_var_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Internal fresh-TypeVar marker. Never written by users, but must parse
    // so the frontend can serialize it through string round-trips.
    if s == "T_Auto" {
        return true;
    }
    if !s.starts_with('T') {
        return false;
    }
    if s.len() == 1 {
        return true; // just "T"
    }
    let rest = &s[1..];
    // T followed by digits only: T1, T99
    if rest.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // T__scope form: double underscore followed by identifier chars.
    // Generated internally by enrichment; users write T1/T2/etc instead.
    if let Some(scope) = rest.strip_prefix("__") {
        return !scope.is_empty()
            && scope.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    }
    false
}

fn split_top_level(s: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            c if c == delimiter && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

impl Default for WeftType {
    fn default() -> Self {
        WeftType::MustOverride
    }
}

impl std::fmt::Display for WeftType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WeftType::Primitive(p) => write!(f, "{}", p.as_str()),
            WeftType::List(inner) => write!(f, "List[{}]", inner),
            WeftType::Dict(k, v) => write!(f, "Dict[{}, {}]", k, v),
            WeftType::Union(types) => {
                let parts: Vec<std::string::String> = types.iter().map(|t| t.to_string()).collect();
                write!(f, "{}", parts.join(" | "))
            }
            WeftType::JsonDict => write!(f, "JsonDict"),
            WeftType::TypeVar(name) => write!(f, "{}", name),
            WeftType::MustOverride => write!(f, "MustOverride"),
        }
    }
}

impl Serialize for WeftType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for WeftType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = std::string::String::deserialize(deserializer)?;
        WeftType::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid port type: {}", s)))
    }
}

#[cfg(test)]
#[path = "tests/weft_type_tests.rs"]
mod tests;
