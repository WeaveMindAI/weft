use serde::{Deserialize, Serialize};

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Blob
// Parameterized:  List[T], Dict[K, V]
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio;  File = Media | Blob
// Type variables: T, T1, T2... : node-scoped, same T on input and output = same type
// MustOverride:   Node can't know the type, user/AI must declare it in Weft code
//
// Port types describe what the node sees: types flow verbatim along
// edges. A port wired to receive `List[T]` sees `List[T]`; element-by-
// element iteration is explicit via `Loop(over: [...])`, never inferred
// from a type difference across an edge.
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
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    // Blob is the catch-all stored-file primitive: any bytes whose mime is
    // not image/video/audio (a pdf, a zip, an unknown type). It never
    // claims a format, so future first-class file types (Document, Text,
    // Presentation, ...) peel OUT of Blob without renaming the fallback.
    Blob,
    Empty,
);

impl std::fmt::Display for WeftPrimitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The concrete kind of a stored-file value: the single concept that
/// ties together the on-wire marker key, the `WeftType` primitive, and
/// the mime classification. A stored value is ALWAYS exactly one kind
/// (never the `Media`/`File` unions, which are signature-only). Blob is
/// the catch-all for any mime that is not image/video/audio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Image,
    Video,
    Audio,
    Blob,
}

impl FileKind {
    /// The on-wire sentinel key that tags a value of this kind.
    pub fn marker_key(self) -> &'static str {
        match self {
            FileKind::Image => "__weft_image__",
            FileKind::Video => "__weft_video__",
            FileKind::Audio => "__weft_audio__",
            FileKind::Blob => "__weft_blob__",
        }
    }

    /// The type-system primitive this kind types as.
    pub fn primitive(self) -> WeftPrimitive {
        match self {
            FileKind::Image => WeftPrimitive::Image,
            FileKind::Video => WeftPrimitive::Video,
            FileKind::Audio => WeftPrimitive::Audio,
            FileKind::Blob => WeftPrimitive::Blob,
        }
    }

    /// Classify a mime type into a concrete kind. The ONE place the
    /// mime->kind mapping lives: image/* -> Image, video/* -> Video,
    /// audio/* -> Audio, everything else -> Blob (the catch-all). When a
    /// future first-class type (Document/Text/...) peels out of Blob,
    /// it gets a branch here and nowhere else.
    pub fn from_mime(mime: &str) -> Self {
        if mime.starts_with("image/") {
            FileKind::Image
        } else if mime.starts_with("video/") {
            FileKind::Video
        } else if mime.starts_with("audio/") {
            FileKind::Audio
        } else {
            FileKind::Blob
        }
    }

    /// Identify the kind of a marker object by which sentinel key it
    /// carries. None if the object carries no stored-file marker.
    pub fn from_marker_obj(obj: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        for kind in [FileKind::Image, FileKind::Video, FileKind::Audio, FileKind::Blob] {
            if obj.contains_key(kind.marker_key()) {
                return Some(kind);
            }
        }
        None
    }
}


/// Recursive port type system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WeftType {
    /// Scalar: String, Number, Boolean, Image, Video, Audio, Blob
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
    /// A message-bus handle: an in-process channel between co-alive
    /// nodes. A `Bus` output connects only to a `Bus` input; message
    /// payloads are not type-checked by the language (the envelope is the
    /// universal contract). Wired-only: the value is a live runtime
    /// handle, never a config literal a user types.
    Bus,
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

    /// Resolve a NAMED union alias to its concrete union type, the ONE
    /// place a union name expands. `Media`/`File` are language-builtin
    /// aliases; this is also the hook where user-defined unions
    /// (`X = A | B | C`, for dynamic typing) will register, so name
    /// resolution stays a single generic mechanism, never a hardcoded
    /// per-name branch in the parser. Returns None for a non-alias name.
    pub fn named_union(name: &str) -> Option<Self> {
        use WeftPrimitive::{Audio, Blob, Image, Video};
        let prims: &[WeftPrimitive] = match name {
            // Media-proper: a picture, a clip, or a sound. Never includes
            // Blob (a spreadsheet/zip is not "media").
            "Media" => &[Image, Video, Audio],
            // Any stored file: media plus the Blob catch-all (and, later,
            // Document/Text/Presentation as they become first-class).
            "File" => &[Image, Video, Audio, Blob],
            _ => return None,
        };
        Some(Self::union_primitives(prims.to_vec()))
    }

    /// The `Media` union (Image | Video | Audio). Convenience over
    /// `named_union("Media")`.
    pub fn media() -> Self {
        Self::named_union("Media").expect("Media is a builtin alias")
    }

    /// The `File` union (any stored-file primitive). Convenience over
    /// `named_union("File")`.
    pub fn file() -> Self {
        Self::named_union("File").expect("File is a builtin alias")
    }

    /// The primitive members of the `File` union, the single source of
    /// truth for "is this primitive a stored-file reference". Derived
    /// from the `File` alias so adding a file primitive in `named_union`
    /// updates every membership check (references_file, detection).
    pub fn file_primitives() -> Vec<WeftPrimitive> {
        match Self::file() {
            WeftType::Union(types) => types
                .into_iter()
                .filter_map(|t| match t {
                    WeftType::Primitive(p) => Some(p),
                    _ => None,
                })
                .collect(),
            WeftType::Primitive(p) => vec![p],
            _ => Vec::new(),
        }
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

    /// True if this type is or contains a stored-file primitive (any
    /// member of the `File` union: Image/Video/Audio/Blob) anywhere in
    /// its structure. A stored file is a `{key|url, mimeType}` reference,
    /// never inline bytes, so it can't be cast from a local file's text.
    /// Membership is derived from the `File` union (the generic source of
    /// truth), not a hand-listed match, so adding a file primitive in one
    /// place updates this too.
    pub fn references_file(&self) -> bool {
        match self {
            WeftType::Primitive(p) => Self::file_primitives().contains(p),
            WeftType::List(inner) => inner.references_file(),
            WeftType::Dict(_, v) => v.references_file(),
            WeftType::Union(types) => types.iter().any(|t| t.references_file()),
            WeftType::JsonDict | WeftType::Bus | WeftType::TypeVar(_) | WeftType::MustOverride => {
                false
            }
        }
    }

    /// Whether a port of this type should be configurable by default. False
    /// only for stored files (alone or in containers), TypeVar, and
    /// MustOverride. Everything else (primitives, lists, dicts, JsonDict,
    /// unions of the above) is configurable so users can paste literal JSON
    /// into the config field instead of wiring a separate Text node. Catalog
    /// authors override per port via `PortDef::wired_only(...)`.
    pub fn is_default_configurable(&self) -> bool {
        match self {
            WeftType::Primitive(_) => !self.references_file(),
            WeftType::List(inner) => inner.is_default_configurable(),
            WeftType::Dict(_, v) => v.is_default_configurable(),
            WeftType::Union(types) => types.iter().all(|t| t.is_default_configurable()),
            WeftType::JsonDict => true,
            // A bus is a live runtime handle: never configurable.
            WeftType::Bus => false,
            WeftType::TypeVar(_) => false,
            WeftType::MustOverride => false,
        }
    }

    /// True if the type includes Null as a valid value (null is legitimate
    /// data, not a skip signal).
    pub fn contains_null(&self) -> bool {
        match self {
            WeftType::Primitive(WeftPrimitive::Null) => true,
            WeftType::Union(types) => types.iter().any(|t| t.contains_null()),
            _ => false,
        }
    }

    /// The canonical zero value for this type: what an unwired,
    /// unseeded port of this type defaults to. Number -> 0, String ->
    /// "", Boolean -> false, List -> [], Dict/JsonDict -> {}. Types with
    /// no literal default (Null/Empty, Media, Bus, and unresolved
    /// TypeVar/MustOverride) default to JSON null, the universal
    /// "no value". For a union, the zero value of its first NON-null
    /// variant (so an optional `Number?` = `Number | Null` zeroes to 0,
    /// not null, regardless of how the optional marker orders the
    /// variants); a union that is only nullish zeroes to null.
    pub fn zero_value(&self) -> serde_json::Value {
        use serde_json::Value;
        match self {
            WeftType::Primitive(p) => match p {
                WeftPrimitive::Number => Value::from(0),
                WeftPrimitive::String => Value::from(""),
                WeftPrimitive::Boolean => Value::from(false),
                WeftPrimitive::Null
                | WeftPrimitive::Empty
                | WeftPrimitive::Image
                | WeftPrimitive::Video
                | WeftPrimitive::Audio
                | WeftPrimitive::Blob => Value::Null,
            },
            WeftType::List(_) => Value::Array(Vec::new()),
            WeftType::Dict(_, _) | WeftType::JsonDict => Value::Object(serde_json::Map::new()),
            WeftType::Union(types) => types
                .iter()
                .find(|t| !t.contains_null())
                .map_or(Value::Null, |t| t.zero_value()),
            WeftType::Bus | WeftType::TypeVar(_) | WeftType::MustOverride => Value::Null,
        }
    }


    // ── Compatibility ───────────────────────────────────────────────────

    /// Structural compatibility: can a value of type `source` flow
    /// into a port of type `target`? Depth-mismatched lists are
    /// rejected. Used by the runtime value checker (validates
    /// inferred JSON types against declared port types), by
    /// merge_ports where narrowing must preserve list depth, and by
    /// the compile-time edge validator (an edge from `List[T]` to `T`
    /// is a real type mismatch; to iterate a list, wrap the consumer
    /// in a `Loop(over: [...])`).
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
            // A bus connects only to a bus; payloads are not type-checked.
            (WeftType::Bus, WeftType::Bus) => true,
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
    /// Objects carrying a concrete stored-file marker (`__weft_image__` /
    /// video / audio / blob) are typed as that primitive.
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
                // Sentinel-tagged runtime types: `{"__weft_<typename>__": <payload>}`.
                // A single distinguishing key keeps the recognition unambiguous
                // (no plain user dict can collide accidentally) and gives every
                // runtime-only type the same shape.
                if let Some(file_type) = Self::detect_file_type(obj) {
                    return file_type;
                }
                if Self::detect_bus_type(obj).is_some() {
                    return WeftType::Bus;
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

    /// Detect a stored-file value by its CONCRETE marker key. A value
    /// carries its exact type as its sentinel: `__weft_image__`,
    /// `__weft_video__`, `__weft_audio__`, or `__weft_blob__` (the
    /// catch-all). The type is read directly from the marker, never
    /// re-derived by guessing from the mime string. There is NO
    /// `__weft_media__` umbrella on the wire: `Media`/`File` are
    /// type-system unions a value matches, never a tag a value carries.
    /// Each payload has a handle (`url`|`data`|`key`) + `mimeType`.
    fn detect_file_type(obj: &serde_json::Map<String, serde_json::Value>) -> Option<WeftType> {
        let kind = FileKind::from_marker_obj(obj)?;
        let payload = obj.get(kind.marker_key())?.as_object()?;
        let has_handle = payload.contains_key("url")
            || payload.contains_key("data")
            || payload.contains_key("key");
        if !has_handle {
            return None;
        }
        Some(WeftType::Primitive(kind.primitive()))
    }

    /// Detect a bus marker. The shape mirrors the stored-file markers:
    /// `{"__weft_bus__": {"id": "<uuid-string>", "mode": "journaled" | "ephemeral"}}`.
    /// Returns the inner payload object so callers can read both fields without
    /// re-parsing. We don't validate the UUID format here; `ctx.bus(...)` errors
    /// loudly on miss.
    fn detect_bus_type(
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        obj.get("__weft_bus__")?.as_object()
    }

    /// Public helper: extract the channel id from a Bus marker value. Returns
    /// `None` if the value is not a Bus marker. Used by `ctx.bus(...)` to
    /// resolve the handle from the registry.
    pub fn bus_marker_id(value: &serde_json::Value) -> Option<&str> {
        Self::detect_bus_type(value.as_object()?)?.get("id")?.as_str()
    }

    /// Public helper: extract the mode from a Bus marker. Returns
    /// `Some(BusMode)` on success, `None` if the value is not a Bus
    /// marker OR the mode field is missing OR the field carries an
    /// unrecognised string. Callers MUST treat `None` as "this is not
    /// a routable bus marker for THIS execution"; do NOT default to
    /// `Journaled`, since silently misclassifying ephemeral as
    /// journaled would route a frame-rate stream into permanent
    /// journal storage.
    #[cfg(feature = "runtime")]
    pub fn bus_marker_mode(value: &serde_json::Value) -> Option<crate::bus::BusMode> {
        let s = Self::detect_bus_type(value.as_object()?)?.get("mode")?.as_str()?;
        crate::bus::BusMode::from_wire_str(s)
    }

    /// Build a Bus marker JSON value from a channel id and a mode.
    /// Takes `BusMode` (not `&str`) so the wire-vocabulary invariant
    /// is enforced at the type system; a typo can't slip through.
    #[cfg(feature = "runtime")]
    pub fn bus_marker(id: &str, mode: crate::bus::BusMode) -> serde_json::Value {
        serde_json::json!({ "__weft_bus__": { "id": id, "mode": mode.as_wire_str() } })
    }

    /// Build a stored-file marker JSON value of the given concrete
    /// `kind`: `{ "<kind marker key>": <payload> }`. The producer picks
    /// the kind once (from the mime) at store time; the value then
    /// self-describes its exact type.
    pub fn file_marker(kind: FileKind, payload: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ kind.marker_key(): payload })
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

    // ── Casting external text ───────────────────────────────────────────

    /// Cast raw external text (a file's content) into a JSON value of this
    /// type. Used by `@file("path", Type)` to inject a file as a typed value
    /// at compile time.
    ///
    /// `String` is verbatim: the bytes become a JSON string, no parsing. This
    /// is the common case (a prompt, a document) and the reason `String` is
    /// the default. Every other concrete type parses the text and checks the
    /// result matches: `Number`/`Boolean` parse the trimmed scalar, everything
    /// structural (`JsonDict`, `List`, `Dict`, `Union`) parses JSON and
    /// validates the inferred type against `self` via `is_compatible`.
    ///
    /// Unresolved targets (`TypeVar`, `MustOverride`) are rejected: a `@file`
    /// cast must name a concrete type.
    pub fn cast_text(&self, text: &str) -> Result<serde_json::Value, std::string::String> {
        match self {
            WeftType::Primitive(WeftPrimitive::String) => {
                Ok(serde_json::Value::String(text.to_string()))
            }
            WeftType::Primitive(WeftPrimitive::Number) => {
                let n: f64 = text.trim().parse().map_err(|_| {
                    format!("expected Number, file content is not a number: {:?}", text.trim())
                })?;
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| format!("Number is not finite: {}", n))
            }
            WeftType::Primitive(WeftPrimitive::Boolean) => match text.trim() {
                "true" => Ok(serde_json::Value::Bool(true)),
                "false" => Ok(serde_json::Value::Bool(false)),
                other => Err(format!("expected Boolean (true/false), got {:?}", other)),
            },
            WeftType::TypeVar(_) | WeftType::MustOverride => {
                Err(format!("@file cannot cast to {}: name a concrete type", self))
            }
            // A stored file (anywhere in the type: bare, List[Image],
            // Image | String) is a {key|url, mimeType} reference, not inline
            // bytes. Loading a binary file's text and JSON-parsing it would
            // always fail with a confusing "not valid JSON"; reject loudly
            // with the real reason.
            _ if self.references_file() => Err(format!(
                "@file cannot cast to {}: a stored file is referenced by key/URL, not loaded inline from a file",
                self
            )),
            // Structural types: parse the file as JSON, then check the inferred
            // shape is compatible with the declared type.
            _ => {
                let value: serde_json::Value = serde_json::from_str(text.trim())
                    .map_err(|e| format!("expected {}, file is not valid JSON: {}", self, e))?;
                let inferred = WeftType::infer(&value);
                if WeftType::is_compatible(&inferred, self) {
                    Ok(value)
                } else {
                    Err(format!(
                        "file content has type {} but @file declares {}",
                        inferred, self
                    ))
                }
            }
        }
    }
}

fn parse_single_type(s: &str) -> Option<WeftType> {
    let s = s.trim();

    // Named union aliases (Media, File, and later user-defined unions)
    // all resolve through the one registry, never a per-name branch.
    if let Some(alias) = WeftType::named_union(s) {
        return Some(alias);
    }

    if s == "JsonDict" {
        return Some(WeftType::JsonDict);
    }

    if s == "Bus" {
        return Some(WeftType::Bus);
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
            WeftType::Bus => write!(f, "Bus"),
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
