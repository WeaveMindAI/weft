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
// In metadata.json input/output declarations, types are string literals:
//   { "name": "items", "type": "List[String]" }
//   { "name": "headers", "type": "Dict[String, String]" }
//   { "name": "value", "type": "T" }            : type variable
//   { "name": "value", "type": "MustOverride" } : user must declare type
// =============================================================================

macro_rules! define_primitives {
    ($($variant:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        // SYNC: WeftPrimitive <-> packages/weft-graph/src/protocol.ts WeftPrimitive
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
    // SYNC: FileKind::marker_key <-> packages/weft-graph/src/protocol.ts STORED_FILE_MARKER_TYPES
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
    /// The concrete kind for a FILE primitive (`Image`/`Video`/`Audio`/`Blob`),
    /// None for any non-file primitive. The inverse of [`Self::primitive`];
    /// used when a DECLARED type picks a value's marker (an asset `@file` ref
    /// typed `Image` produces an `__weft_image__` value regardless of what a
    /// mime guess would say).
    pub fn from_primitive(p: WeftPrimitive) -> Option<Self> {
        match p {
            WeftPrimitive::Image => Some(FileKind::Image),
            WeftPrimitive::Video => Some(FileKind::Video),
            WeftPrimitive::Audio => Some(FileKind::Audio),
            WeftPrimitive::Blob => Some(FileKind::Blob),
            _ => None,
        }
    }

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


/// Where a value may come from for an input, the one knob a node author
/// sets per input. Exposure governs LITERALS: whether a literal may sit
/// in the config braces (`M { x: 5 }`) and/or as an assignment statement
/// (`M.x = 5`). Wires (an edge, `M { x: other.y }`, an inline expression)
/// are a separate axis: every exposure is wireable EXCEPT `Config`, which
/// is a pure design-time setting the graph never drives.
// SYNC: Exposure <-> packages/weft-graph/src/protocol.ts Exposure
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exposure {
    /// Everything: braces literal, assignment literal, wires.
    #[default]
    All,
    /// A literal only via `node.input = ...`; a braces literal is
    /// refused. Wireable.
    Assignment,
    /// A literal only in the config braces. NOT wireable: the value is
    /// design-time configuration, never graph data.
    Config,
    /// No literal ever: the input is driven by wires alone.
    Wire,
}

impl Exposure {
    /// May a literal sit in the config braces?
    pub fn allows_braces_literal(self) -> bool {
        matches!(self, Exposure::All | Exposure::Config)
    }

    /// May a literal be written as an assignment statement?
    pub fn allows_assignment_literal(self) -> bool {
        matches!(self, Exposure::All | Exposure::Assignment)
    }

    /// May the input be driven by a wire (edge, braces endpoint,
    /// inline expression)?
    pub fn wireable(self) -> bool {
        !matches!(self, Exposure::Config)
    }

    /// May a literal be written at all (braces or assignment)?
    pub fn allows_literal(self) -> bool {
        !matches!(self, Exposure::Wire)
    }

    /// Stable lowercase tag, matching the serde/wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            Exposure::All => "all",
            Exposure::Assignment => "assignment",
            Exposure::Config => "config",
            Exposure::Wire => "wire",
        }
    }
}

/// The sentinel key of the generator handle value the engine places in
/// a consumer's input bag (`{"__weft_generator__": {"id": "<uuid>"}}`).
/// Same shape family as the bus / stored-file / access markers. Lives
/// here (not in the runtime-gated `generator` module) because the type
/// gate (`validate_value`) must recognise it in every build.
pub const GENERATOR_MARKER_KEY: &str = "__weft_generator__";

/// One resolved type declaration: the body a name expands to, and
/// whether the name is NOMINAL (user-declared: the name is the contract,
/// values wire only into the same name) or pure display sugar (the
/// builtin `Media`/`File` aliases: equality never sees the name).
#[derive(Debug, Clone, PartialEq)]
pub struct TypeDecl {
    pub body: WeftType,
    pub nominal: bool,
}

/// The named-type registry: every type NAME the parser may resolve
/// (builtin aliases plus user declarations from `types` keys in
/// `metadata.json`). Needed only at AUTHORING boundaries, where humans
/// write bare names: catalog metadata loading and weft-source compiling.
/// Serialized type strings are self-contained (`Name=Body`, see
/// [`WeftType::wire_string`]) precisely so that DESERIALIZING a stored
/// project or journal never needs a registry.
///
/// How a registry becomes visible to [`WeftType::parse`]:
/// - [`TypeRegistry::scoped`] activates one for a synchronous closure
///   (a catalog scan, a compile). The scope is thread-local and must
///   not span an `.await`; the closure is sync by construction. This
///   is the form for processes that handle many projects.
/// - [`TypeRegistry::install`] sets one for the whole process, once.
///   Only for binaries that live inside a single project (an emitted
///   project binary installs its own table at boot). A second install
///   with different content is a loud error.
/// - With neither, [`TypeRegistry::current`] answers the builtin table
///   (`Media`, `File`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypeRegistry {
    entries: std::collections::BTreeMap<std::string::String, TypeDecl>,
}

thread_local! {
    static REGISTRY_SCOPE: std::cell::RefCell<Vec<std::sync::Arc<TypeRegistry>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

static REGISTRY_INSTALLED: std::sync::OnceLock<std::sync::Arc<TypeRegistry>> =
    std::sync::OnceLock::new();

impl TypeRegistry {
    /// The builtin table: the union aliases (`Media`, `File`), as
    /// non-nominal display sugar. Seeded from [`WeftType::UNION_ALIASES`]
    /// so the alias set has one source of truth.
    pub fn builtin() -> Self {
        let mut entries = std::collections::BTreeMap::new();
        for (name, prims) in WeftType::UNION_ALIASES {
            entries.insert(
                (*name).to_string(),
                TypeDecl {
                    body: WeftType::union_primitives(prims.to_vec()),
                    nominal: false,
                },
            );
        }
        Self { entries }
    }

    /// Resolve a bare name to its type: a nominal entry answers
    /// `Named { name, body }`, a sugar entry answers its body directly.
    pub fn lookup(&self, name: &str) -> Option<WeftType> {
        self.entries.get(name).map(|decl| {
            if decl.nominal {
                WeftType::Named { name: name.to_string(), body: Box::new(decl.body.clone()) }
            } else {
                decl.body.clone()
            }
        })
    }


    /// Every user-declared (nominal) entry as `(name, authored body
    /// string)`, for shipping to surfaces that parse type strings
    /// themselves (the editor).
    pub fn nominal_entries(&self) -> Vec<(std::string::String, std::string::String)> {
        self.entries
            .iter()
            .filter(|(_, d)| d.nominal)
            .map(|(n, d)| (n.clone(), d.body.to_string()))
            .collect()
    }

    /// Build a registry from raw declarations (`name`, `type string`,
    /// `origin` for error messages). Declarations may reference each
    /// other in any order; resolution iterates to a fixed point.
    /// Duplicate names with structurally identical bodies are absorbed
    /// (two independent packages may declare the same shared type);
    /// different bodies are a loud error naming both origins. Fails
    /// loud on a reserved name, a cycle, or a reference to a name
    /// nobody declares.
    pub fn build(
        declarations: &[(std::string::String, std::string::String, std::string::String)],
    ) -> Result<Self, std::string::String> {
        for (name, _, origin) in declarations {
            Self::check_declarable_name(name)
                .map_err(|e| format!("{origin}: type `{name}`: {e}"))?;
        }

        let mut registry = Self::builtin();
        // Where each name was FIRST declared, so a clash names both
        // sides (blaming only the second sends the author to fix the
        // wrong file half the time).
        let mut first_origin: std::collections::BTreeMap<&str, &str> =
            std::collections::BTreeMap::new();
        // (name, body string, origin) still waiting for their references.
        let mut pending: Vec<&(std::string::String, std::string::String, std::string::String)> =
            declarations.iter().collect();

        loop {
            let mut progressed = false;
            let mut still_pending = Vec::new();
            for decl in pending {
                let (name, body_str, origin) = decl;
                let parsed =
                    std::sync::Arc::new(registry.clone()).scoped_ref(|| WeftType::parse(body_str));
                match parsed {
                    Some(body) => {
                        // A declared body must be CONCRETE: `T` is a
                        // node-scoped port variable, and the language
                        // has no parameterized nominal types, so a var
                        // in a declaration could only ever produce two
                        // same-named types with different bodies (which
                        // name-only nominal compatibility must never
                        // see).
                        if body.contains_unresolved_leaf() {
                            return Err(format!(
                                "{origin}: type `{name}`: a declared type body must be \
                                 concrete; `{body_str}` carries a type variable, which \
                                 is a node-scoped port concept and cannot appear in a \
                                 declaration"
                            ));
                        }
                        if let Some(existing) = registry.entries.get(name.as_str()) {
                            if existing.nominal && existing.body == body {
                                // Identical redeclaration: absorbed.
                            } else if existing.nominal {
                                let first = first_origin.get(name.as_str()).copied().unwrap_or("?");
                                return Err(format!(
                                    "type `{name}` is declared twice with different bodies \
                                     (first in {first}, then in {origin}: `{body_str}`); rename one"
                                ));
                            } else {
                                // check_declarable_name refuses builtin-alias
                                // names up front, so a non-nominal (builtin)
                                // collision here is an internal invariant break.
                                return Err(format!(
                                    "{origin}: type `{name}` is reserved by the type language"
                                ));
                            }
                        } else {
                            first_origin.insert(name.as_str(), origin.as_str());
                            registry.entries.insert(
                                name.clone(),
                                TypeDecl { body, nominal: true },
                            );
                        }
                        progressed = true;
                    }
                    None => still_pending.push(decl),
                }
            }
            if still_pending.is_empty() {
                return Ok(registry);
            }
            if !progressed {
                let names: Vec<std::string::String> = still_pending
                    .iter()
                    .map(|(n, b, o)| format!("`{n}` = `{b}` ({o})"))
                    .collect();
                return Err(format!(
                    "type declarations could not be resolved (an unknown referenced name, \
                     a cycle, or invalid syntax): {}",
                    names.join(", ")
                ));
            }
            pending = still_pending;
        }
    }

    /// A declarable name: an identifier starting with an uppercase
    /// letter, not colliding with anything the type language already
    /// means (a primitive, a container keyword, a special type, a
    /// TypeVar shape).
    // SYNC: named-name rule <-> packages/weft-graph/src/protocol.ts parseSingleType (Name=Body arm)
    fn check_declarable_name(name: &str) -> Result<(), std::string::String> {
        let mut chars = name.chars();
        let valid_ident = chars.next().is_some_and(|c| c.is_ascii_uppercase())
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid_ident {
            return Err("a type name starts with an uppercase letter and contains only \
                        letters, digits, and underscores"
                .into());
        }
        let reserved = WeftPrimitive::from_str(name).is_some()
            || matches!(
                name,
                "List" | "Dict" | "JsonDict" | "Bus" | "Access" | "Generator" | "MustOverride"
            )
            || WeftType::UNION_ALIASES.iter().any(|(alias, _)| *alias == name)
            || is_type_var_name(name);
        if reserved {
            return Err(format!("`{name}` is reserved by the type language"));
        }
        Ok(())
    }

    /// Run `f` with this registry active for bare-name resolution on the
    /// current thread. `f` is a sync closure ON PURPOSE: the scope is
    /// thread-local and must never span an `.await`.
    pub fn scoped<R>(self: std::sync::Arc<Self>, f: impl FnOnce() -> R) -> R {
        self.scoped_ref(f)
    }

    fn scoped_ref<R>(self: std::sync::Arc<Self>, f: impl FnOnce() -> R) -> R {
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                REGISTRY_SCOPE.with(|s| {
                    s.borrow_mut().pop();
                });
            }
        }
        REGISTRY_SCOPE.with(|s| s.borrow_mut().push(self));
        let _guard = Guard;
        f()
    }

    /// Install the process-wide registry. For single-project binaries
    /// only (an emitted project binary at boot). Idempotent for the
    /// same content; a different content is a loud error.
    pub fn install(registry: TypeRegistry) -> Result<(), std::string::String> {
        let arc = std::sync::Arc::new(registry);
        match REGISTRY_INSTALLED.set(arc.clone()) {
            Ok(()) => Ok(()),
            Err(_) => {
                let existing = REGISTRY_INSTALLED.get().expect("set just failed, so present");
                if **existing == *arc {
                    Ok(())
                } else {
                    Err("a different type registry is already installed in this process; \
                         a process serves one project's types"
                        .into())
                }
            }
        }
    }

    /// The registry bare-name resolution reads right now: the innermost
    /// active scope, else the installed one, else builtin.
    pub fn current() -> std::sync::Arc<TypeRegistry> {
        if let Some(scoped) = REGISTRY_SCOPE.with(|s| s.borrow().last().cloned()) {
            return scoped;
        }
        if let Some(installed) = REGISTRY_INSTALLED.get() {
            return installed.clone();
        }
        static BUILTIN: std::sync::OnceLock<std::sync::Arc<TypeRegistry>> =
            std::sync::OnceLock::new();
        BUILTIN.get_or_init(|| std::sync::Arc::new(TypeRegistry::builtin())).clone()
    }
}

/// One declared field of a [`WeftType::Record`]: a name, a type, and
/// whether the key may be absent (a present `null` counts as absent,
/// matching serde's skip-when-none world). Field ORDER is preserved for
/// display; equality and compatibility are order-insensitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordField {
    pub name: std::string::String,
    pub ty: WeftType,
    pub optional: bool,
}

/// Recursive port type system.
#[derive(Debug, Clone, Eq)]
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
    /// Authorized ability to call a third party: the value an access
    /// node emits (`{"__weft_access__": {...}}`, see
    /// `crate::access::Access`). One general type for both kinds
    /// (metered + personal); never parameterized per service (wrong-
    /// service wiring fails loud at runtime). Wired-only: the marker is
    /// minted by an access node, never typed as a literal.
    Access,
    /// A typed, one-directional, terminating stream: `Generator[T]`.
    /// The port itself accepts being emitted into multiple times; every
    /// item is checked against the element type `T`, the consumer pulls
    /// them one at a time, and the stream ends when the producer's body
    /// returns. Exactly one producer feeds exactly one consumer (a
    /// stream has one taker, compiler-enforced). Wired-only: the
    /// consumer's bag value is a live runtime handle, never a literal.
    // SYNC: WeftType::Generator <-> packages/weft-graph/src/protocol.ts WeftType 'generator'
    Generator(Box<WeftType>),
    /// Dict with KNOWN field names and per-field types:
    /// `{ role: String, name?: String }`. `?` marks an optional field.
    /// Validation is strict: a value carrying an undeclared key does not
    /// match. Structural (two records with the same fields are the same
    /// type); the nominal wrapper is [`WeftType::Named`].
    Record(Vec<RecordField>),
    /// A user-declared NOMINAL type: the name is the contract.
    /// Compatibility is by name (same-named `Named` only); the body is
    /// carried inline so validation, display, and file detection never
    /// need a registry after parse. Declared via the `types` key in
    /// `metadata.json` (see [`TypeRegistry`]); the serialized wire form
    /// is self-contained (`Name=Body`) so a stored type string is
    /// interpretable in any process with no out-of-band state.
    Named {
        name: std::string::String,
        body: Box<WeftType>,
    },
    /// Node-scoped type variable: T, T1, T2, etc.
    /// Same name on different ports of the same node = same type.
    /// Resolved per-node when connections are made.
    TypeVar(std::string::String),
    /// Node cannot determine the type. User/AI must override in Weft code.
    /// Remaining MustOverride at compile time = error.
    MustOverride,
}

/// Structural equality, with three deliberate departures from a
/// derive: record fields and union members compare as a SET
/// (declaration order is display-only, and `is_compatible` already
/// treats unions as sets), and a `Named` equals only a same-named
/// `Named`. The name IS the contract: same-named bodies are made to
/// agree by the compiler's `named-type-conflict` rule (the registry
/// refuses conflicting DECLARATIONS, and the compiler refuses a wire
/// restatement that contradicts one), never by the parser, so a
/// stored string can legitimately carry an out-of-date body and
/// name-only comparison is the deliberate nominal choice.
// SYNC: PartialEq for WeftType <-> packages/weft-graph/src/protocol.ts weftTypesEqual
impl PartialEq for WeftType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (WeftType::Primitive(a), WeftType::Primitive(b)) => a == b,
            (WeftType::List(a), WeftType::List(b)) => a == b,
            (WeftType::Dict(ak, av), WeftType::Dict(bk, bv)) => ak == bk && av == bv,
            (WeftType::Union(a), WeftType::Union(b)) => {
                a.len() == b.len() && a.iter().all(|m| b.contains(m))
            }
            (WeftType::JsonDict, WeftType::JsonDict) => true,
            (WeftType::Bus, WeftType::Bus) => true,
            (WeftType::Access, WeftType::Access) => true,
            (WeftType::Generator(a), WeftType::Generator(b)) => a == b,
            (WeftType::Record(a), WeftType::Record(b)) => {
                a.len() == b.len()
                    && a.iter().all(|fa| {
                        b.iter().any(|fb| {
                            fa.name == fb.name && fa.optional == fb.optional && fa.ty == fb.ty
                        })
                    })
            }
            (WeftType::Named { name: a, .. }, WeftType::Named { name: b, .. }) => a == b,
            (WeftType::TypeVar(a), WeftType::TypeVar(b)) => a == b,
            (WeftType::MustOverride, WeftType::MustOverride) => true,
            _ => false,
        }
    }
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

    // SYNC: WeftType::union <-> packages/weft-graph/src/protocol.ts union normalization (parseWeftType's union arm)
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

    /// The builtin union aliases: the ONE table both name resolution
    /// ([`Self::named_union`]) and rendering ([`std::fmt::Display`]) read,
    /// so `parse -> to_string` round-trips an alias to its NAME instead of
    /// leaking the structural expansion into user-facing surfaces (source
    /// lines, the editor palette, metadata re-serialization). The type
    /// itself stays STRUCTURAL (an alias is naming sugar, equality and
    /// unification never see the name); a hand-written `Image | Video |
    /// Audio` therefore renders under its canonical name `Media`.
    ///
    /// - `Media`: a picture, a clip, or a sound. Never includes Blob (a
    ///   spreadsheet/zip is not "media").
    /// - `File`: any stored file: media plus the Blob catch-all (and,
    ///   later, Document/Text/Presentation as they become first-class).
    // SYNC: WeftType::UNION_ALIASES <-> packages/weft-graph/src/protocol.ts NAMED_UNIONS
    const UNION_ALIASES: &'static [(&'static str, &'static [WeftPrimitive])] = &[
        ("Media", &[WeftPrimitive::Image, WeftPrimitive::Video, WeftPrimitive::Audio]),
        (
            "File",
            &[
                WeftPrimitive::Image,
                WeftPrimitive::Video,
                WeftPrimitive::Audio,
                WeftPrimitive::Blob,
            ],
        ),
    ];

    /// Resolve a NAMED union alias to its concrete union type, the ONE
    /// place a union name expands. `Media`/`File` are language-builtin
    /// aliases (see [`Self::UNION_ALIASES`]); this is also the hook where
    /// user-defined unions (`X = A | B | C`, for dynamic typing) will
    /// register, so name resolution stays a single generic mechanism, never
    /// a hardcoded per-name branch in the parser. Returns None for a
    /// non-alias name.
    pub fn named_union(name: &str) -> Option<Self> {
        Self::UNION_ALIASES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, prims)| Self::union_primitives(prims.to_vec()))
    }

    /// The alias NAME of a structural union, if its members are exactly one
    /// alias's primitive set (order-insensitive). The Display half of the
    /// alias round-trip.
    fn union_alias_name(types: &[WeftType]) -> Option<&'static str> {
        let prims: Vec<&WeftPrimitive> = types
            .iter()
            .map(|t| match t {
                WeftType::Primitive(p) => Some(p),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Self::UNION_ALIASES
            .iter()
            .find(|(_, members)| {
                members.len() == prims.len() && members.iter().all(|m| prims.contains(&m))
            })
            .map(|(name, _)| *name)
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
    /// The concrete file kind this type NAMES, when it is exactly one file
    /// primitive (`Image`/`Video`/`Audio`/`Blob`). None for the unions
    /// (`File`/`Media`), containers, and non-file types: those don't pin a
    /// marker kind by declaration (the value's mime decides instead).
    pub fn concrete_file_kind(&self) -> Option<FileKind> {
        match self {
            WeftType::Primitive(p) => FileKind::from_primitive(*p),
            WeftType::Named { body, .. } => body.concrete_file_kind(),
            _ => None,
        }
    }

    /// Can a value of this type be edited IN PLACE through a `@file` ref
    /// (the editor serializes the field's new value back to the referenced
    /// file as text)? Each type opts in here; a type without it is only
    /// legal in `@asset` (pull-only), and `@file` rejects it with a message
    /// pointing there. Today the line is "text-serializable": every
    /// non-file type. A future type can sit on either side (or both, by
    /// being text-serializable AND file-referencing, if such a shape ever
    /// exists).
    pub fn supports_bidirectional_edit(&self) -> bool {
        !self.references_file()
    }

    /// The generator behind this type, peeling nominal wrappers: a
    /// `Generator[T]` directly, or a `Named` whose body is one (a user
    /// alias like `type Rows = Generator[Number]`). Every "is this
    /// port a stream" decision (compiler wiring rules, engine routing,
    /// dispatch deferral) asks THIS, so an alias behaves identically
    /// to the spelled-out type everywhere.
    pub fn as_generator(&self) -> Option<&WeftType> {
        match self.structural() {
            WeftType::Generator(inner) => Some(inner),
            _ => None,
        }
    }

    /// The structural type behind any chain of nominal aliases: `self`
    /// unless it is `Named`, else the innermost non-`Named` body.
    /// Every structural pattern-match on a possibly-aliased type goes
    /// through this so an alias behaves identically to the spelled-out
    /// type.
    pub fn structural(&self) -> &WeftType {
        match self {
            WeftType::Named { body, .. } => body.structural(),
            other => other,
        }
    }

    /// The type a VALUE arriving on a port of this type is checked
    /// against: the element type for a stream (`Generator[T]` pulses
    /// carry items of `T`; the whole-port handle only exists in the
    /// consumer's bag), the type itself otherwise.
    pub fn port_value_type(&self) -> &WeftType {
        self.as_generator().unwrap_or(self)
    }

    // SYNC: references_file <-> packages/weft-graph/src/protocol.ts typeReferencesFile
    pub fn references_file(&self) -> bool {
        match self {
            WeftType::Primitive(p) => Self::file_primitives().contains(p),
            WeftType::List(inner) => inner.references_file(),
            WeftType::Dict(_, v) => v.references_file(),
            WeftType::Union(types) => types.iter().any(|t| t.references_file()),
            WeftType::Record(fields) => fields.iter().any(|f| f.ty.references_file()),
            WeftType::Named { body, .. } => body.references_file(),
            // A generator's port value is a live handle; its ITEMS may
            // reference files, but nothing about the port value itself
            // is a file to cast or pick, so it sits with Bus here.
            WeftType::JsonDict
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Generator(_)
            | WeftType::TypeVar(_)
            | WeftType::MustOverride => false,
        }
    }

    /// The default `Exposure` for an input of this type. Plain data
    /// (primitives, lists, dicts, JsonDict, unions of those) takes a
    /// literal anywhere and wires (`All`), so users can paste values into
    /// the config braces instead of wiring a separate Text node. Stored
    /// files (alone or in containers) take a literal only as an assignment
    /// (`n.p = @asset(..)`: nobody can hand-type a file marker into a
    /// config field), as do TypeVar and MustOverride inputs (their
    /// concrete type is unknown until overridden). A Bus is a live runtime
    /// handle: wires alone. Node authors override per input via
    /// `InputSpec::exposure` (including the `Config` mode, which no type
    /// defaults to).
    pub fn default_exposure(&self) -> Exposure {
        match self {
            WeftType::Primitive(_) => {
                if self.references_file() { Exposure::Assignment } else { Exposure::All }
            }
            WeftType::List(inner) => inner.default_exposure(),
            WeftType::Dict(_, v) => v.default_exposure(),
            // A union is as restrictive as its most restrictive member.
            // Only the three type-derived levels can occur here (`Config`
            // is author-only), so the fold is: any wires-only member makes
            // the union wires-only, else any assignment-only member makes
            // it assignment-only, else it stays open.
            WeftType::Union(types) => {
                let members: Vec<Exposure> =
                    types.iter().map(|t| t.default_exposure()).collect();
                if members.contains(&Exposure::Wire) {
                    Exposure::Wire
                } else if members.contains(&Exposure::Assignment) {
                    Exposure::Assignment
                } else {
                    Exposure::All
                }
            }
            WeftType::JsonDict => Exposure::All,
            // A record is as restrictive as its most restrictive field
            // (same fold as a union's members).
            WeftType::Record(fields) => {
                let members: Vec<Exposure> =
                    fields.iter().map(|f| f.ty.default_exposure()).collect();
                if members.contains(&Exposure::Wire) {
                    Exposure::Wire
                } else if members.contains(&Exposure::Assignment) {
                    Exposure::Assignment
                } else {
                    Exposure::All
                }
            }
            WeftType::Named { body, .. } => body.default_exposure(),
            WeftType::Bus => Exposure::Wire,
            WeftType::Access => Exposure::Wire,
            // A stream is a live edge between two running bodies:
            // wires alone, like a Bus.
            WeftType::Generator(_) => Exposure::Wire,
            WeftType::TypeVar(_) => Exposure::Assignment,
            WeftType::MustOverride => Exposure::Assignment,
        }
    }

    /// True when this type names a stored file as a WHOLE value: a bare
    /// file primitive, or a union whose every member is one (the
    /// `File`/`Media` aliases). Containers of files (`List[Image]`) are
    /// NOT file-valued: their literal is typed as JSON, not picked as one
    /// file. Drives the default widget (a file-valued input gets the
    /// drop/pick control).
    pub fn is_file_valued(&self) -> bool {
        match self {
            WeftType::Primitive(p) => Self::file_primitives().contains(p),
            WeftType::Union(types) => types.iter().all(|t| t.is_file_valued()),
            WeftType::Named { body, .. } => body.is_file_valued(),
            _ => false,
        }
    }

    /// True if the type includes Null as a valid value (null is legitimate
    /// data, not a skip signal).
    pub fn contains_null(&self) -> bool {
        match self {
            WeftType::Primitive(WeftPrimitive::Null) => true,
            WeftType::Union(types) => types.iter().any(|t| t.contains_null()),
            WeftType::Named { body, .. } => body.contains_null(),
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
            // An all-optional record zeroes to {}; one with required
            // fields has no honest zero (null, like Media).
            WeftType::Record(fields) => {
                if fields.iter().all(|f| f.optional) {
                    Value::Object(serde_json::Map::new())
                } else {
                    Value::Null
                }
            }
            WeftType::Named { body, .. } => body.zero_value(),
            WeftType::Bus
            | WeftType::Access
            | WeftType::Generator(_)
            | WeftType::TypeVar(_)
            | WeftType::MustOverride => Value::Null,
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
    // SYNC: is_compatible (Named/Record arms) <-> packages/weft-graph/src/webview/lib/types/index.ts isCompatible
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
            // An access connects only to an access; the KIND/service is
            // checked at runtime resolution, not by the type system.
            (WeftType::Access, WeftType::Access) => true,
            // A generator connects only to a same-element generator
            // (invariant in T, checked both ways): the stream contract
            // is one type end to end, so no variance-driven surprise
            // where an item flows into a widened consumer. A plain `T`
            // never accepts a `Generator[T]` and vice versa.
            // SYNC: generator compatibility <-> packages/weft-graph/src/webview/lib/types/index.ts isCompatible
            (WeftType::Generator(a), WeftType::Generator(b)) => {
                Self::is_compatible(a, b) && Self::is_compatible(b, a)
            }
            // A NAMED type is nominal: the name is the contract, so only
            // the same name flows in. Same-named bodies are made to
            // agree by the compiler's `named-type-conflict` rule (not
            // by the parser, which stays registry-independent), so
            // name-only comparison is the deliberate nominal choice.
            (WeftType::Named { name: a, .. }, WeftType::Named { name: b, .. }) => a == b,
            // Records: the target's contract decides. Every source field
            // must be declared on the target (strict unknown keys), every
            // required target field must be a required source field, and
            // field types must match pairwise.
            (WeftType::Record(sources), WeftType::Record(targets)) => {
                sources.iter().all(|sf| targets.iter().any(|tf| tf.name == sf.name))
                    && targets.iter().all(|tf| match sources.iter().find(|sf| sf.name == tf.name) {
                        Some(sf) => {
                            (!sf.optional || tf.optional) && Self::is_compatible(&sf.ty, &tf.ty)
                        }
                        None => tf.optional,
                    })
            }
            // A record forgets itself into the generic-object types, but
            // never the reverse: an unchecked object claiming a declared
            // shape is exactly what the record type exists to refuse
            // (the deliberate door is the Cast node).
            (WeftType::Record(_), WeftType::JsonDict) => true,
            (WeftType::Record(fields), WeftType::Dict(k, v)) => {
                matches!(k.as_ref(), WeftType::Primitive(WeftPrimitive::String))
                    && fields.iter().all(|f| Self::is_compatible(&f.ty, v))
            }
            (WeftType::JsonDict, WeftType::Record(_)) => false,
            (WeftType::Dict(_, _), WeftType::Record(_)) => false,
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
            // A named source DECAYS into a structural target (forgetting
            // the name is always safe); this arm sits after the union
            // arms so `Named -> Named | Null` matches through the union.
            (WeftType::Named { body, .. }, tgt) => Self::is_compatible(body, tgt),
            // Nothing unnamed flows into a named target (the arm above
            // the unions handled Named -> Named).
            (_, WeftType::Named { .. }) => false,
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
                if obj.contains_key(crate::access::ACCESS_MARKER_KEY) {
                    return WeftType::Access;
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
    /// `id` and `mode` are the marker's whole payload: every other bus
    /// parameter is read off the live handle a consumer resolves from
    /// the marker.
    #[cfg(feature = "runtime")]
    pub fn bus_marker(id: &str, mode: crate::bus::BusMode) -> serde_json::Value {
        serde_json::json!({ "__weft_bus__": {
            "id": id,
            "mode": mode.as_wire_str(),
        } })
    }

    /// Build a stored-file marker JSON value of the given concrete
    /// `kind`: `{ "<kind marker key>": <payload> }`. The producer picks
    /// the kind once (from the mime) at store time; the value then
    /// self-describes its exact type.
    pub fn file_marker(kind: FileKind, payload: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ kind.marker_key(): payload })
    }

    /// Cast a JSON value into this type, when the conversion is
    /// UNAMBIGUOUS. The literal-lenience rule: a value written in weft
    /// source (by a human or an AI) that plainly means a value of the
    /// declared type is converted at compile time instead of failing
    /// dumbly; anything genuinely ambiguous or lossy is refused and the
    /// caller reports the mismatch. The sibling of [`Self::cast_text`]
    /// (the `@file` cast), which handles TEXT; this handles an
    /// already-parsed JSON value.
    ///
    /// Rules (recursive):
    /// - a value already compatible with the type passes through as-is;
    /// - String -> Number parses the trimmed scalar;
    /// - String -> Boolean accepts true/false in any case ("True",
    ///   "FALSE") plus "1"/"0"; Number -> Boolean accepts exactly 1/0
    ///   (any other number is ambiguous and refused);
    /// - Boolean -> Number is 1/0; Number / Boolean -> String
    ///   stringifies the scalar;
    /// - String -> a structural type (List/Dict/JsonDict) parses the
    ///   string as JSON, then recurses;
    /// - List / Dict recurse into elements / values;
    /// - a union tries exact compatibility first; failing that, EVERY
    ///   member's cast is attempted: exactly one success (or several
    ///   agreeing on the same value) wins, and members disagreeing on
    ///   the result is an AMBIGUOUS cast, refused loudly naming the
    ///   readings (never silently picked by declaration order);
    /// - Null, Bus, TypeVar, MustOverride, and file types never cast
    ///   (a file value is a marker, not convertible data).
    pub fn cast_value(&self, value: &serde_json::Value) -> Result<serde_json::Value, std::string::String> {
        use serde_json::Value;
        // Exact fit: hand it back untouched (preserves the written form).
        if Self::is_compatible(&Self::infer(value), self) {
            return Ok(value.clone());
        }
        if value.is_null() {
            return Err("null has no cast".into());
        }
        // A file-valued type (a bare Image/Media/...) never casts: a
        // file value is a marker, not convertible data. Containers and
        // records HOLDING files recurse below; their file-typed leaves
        // only pass through when the value already is a marker (the
        // compatibility fast path), so nothing invents a file.
        if self.is_unresolved()
            || self.is_file_valued()
            || matches!(self, WeftType::Bus | WeftType::Access | WeftType::Generator(_))
        {
            return Err(format!("no cast into {self}"));
        }
        match self {
            WeftType::Primitive(WeftPrimitive::Number) => match value {
                Value::String(s) => {
                    let n: f64 = s.trim().parse().map_err(|_| {
                        format!("{s:?} is not a number")
                    })?;
                    serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .ok_or_else(|| format!("{n} is not finite"))
                }
                Value::Bool(b) => Ok(Value::from(if *b { 1 } else { 0 })),
                _ => Err(format!("no cast from {} into Number", Self::infer(value))),
            },
            WeftType::Primitive(WeftPrimitive::Boolean) => match value {
                Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "1" => Ok(Value::Bool(true)),
                    "false" | "0" => Ok(Value::Bool(false)),
                    other => Err(format!("{other:?} is not a boolean")),
                },
                // Exactly 1/0; any other number is ambiguous, refused.
                Value::Number(n) => match n.as_f64() {
                    Some(x) if x == 1.0 => Ok(Value::Bool(true)),
                    Some(x) if x == 0.0 => Ok(Value::Bool(false)),
                    _ => Err(format!("{n} is not a boolean (only 1/0 cast)")),
                },
                _ => Err(format!("no cast from {} into Boolean", Self::infer(value))),
            },
            WeftType::Primitive(WeftPrimitive::String) => match value {
                Value::Number(n) => Ok(Value::String(n.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                _ => Err(format!("no cast from {} into String", Self::infer(value))),
            },
            WeftType::List(inner) => match value {
                Value::Array(items) => items
                    .iter()
                    .map(|v| inner.cast_value(v))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array),
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    self.cast_value(&parsed)
                }
                _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
            },
            WeftType::Dict(_, v_ty) => match value {
                Value::Object(map) => {
                    let mut out = serde_json::Map::new();
                    for (k, v) in map {
                        out.insert(k.clone(), v_ty.cast_value(v)?);
                    }
                    Ok(Value::Object(out))
                }
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    self.cast_value(&parsed)
                }
                _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
            },
            WeftType::JsonDict => match value {
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    match parsed {
                        Value::Object(_) => Ok(parsed),
                        other => Err(format!("expected a JSON object, got {}", Self::infer(&other))),
                    }
                }
                _ => Err(format!("no cast from {} into JsonDict", Self::infer(value))),
            },
            // A record casts field by field under its declared contract:
            // undeclared keys refused, required fields demanded, an
            // optional field's null kept as written. A string parses as
            // JSON first.
            WeftType::Record(fields) => match value {
                Value::Object(map) => {
                    if let Some(unknown) =
                        map.keys().find(|k| !fields.iter().any(|f| &f.name == *k))
                    {
                        return Err(format!("unknown field {unknown:?} (not declared on {self})"));
                    }
                    let mut out = serde_json::Map::new();
                    for field in fields {
                        match map.get(&field.name) {
                            Some(Value::Null) | None if field.optional => {
                                if map.contains_key(&field.name) {
                                    out.insert(field.name.clone(), Value::Null);
                                }
                            }
                            Some(v) => {
                                let cast = field.ty.cast_value(v).map_err(|e| {
                                    format!("field {:?}: {e}", field.name)
                                })?;
                                out.insert(field.name.clone(), cast);
                            }
                            None => {
                                return Err(format!("missing required field {:?}", field.name))
                            }
                        }
                    }
                    Ok(Value::Object(out))
                }
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    self.cast_value(&parsed)
                }
                _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
            },
            // A named type casts through its body; the RESULT is a value
            // of the named type (nominal-ness lives in the type system,
            // values carry no name tag).
            WeftType::Named { body, .. } => body
                .cast_value(value)
                .map_err(|e| format!("not a valid {self}: {e}")),
            // Exact compatibility was tried above; now EVERY member's
            // cast. One success (or several agreeing) wins; members
            // disagreeing on the result is ambiguous and refused with
            // both readings, so the writer disambiguates instead of the
            // compiler guessing.
            WeftType::Union(members) => {
                let successes: Vec<(&WeftType, serde_json::Value)> = members
                    .iter()
                    .filter_map(|m| m.cast_value(value).ok().map(|v| (m, v)))
                    .collect();
                match successes.as_slice() {
                    [] => Err(format!("no member of {self} can cast {}", Self::infer(value))),
                    [(_, v)] => Ok(v.clone()),
                    all => {
                        let (_, first) = &all[0];
                        if all.iter().all(|(_, v)| v == first) {
                            Ok(first.clone())
                        } else {
                            let readings: Vec<std::string::String> = all
                                .iter()
                                .map(|(m, v)| format!("{v} ({m})"))
                                .collect();
                            Err(format!(
                                "ambiguous cast into {self}: could be {}; write the value \
                                 in its exact type",
                                readings.join(" or ")
                            ))
                        }
                    }
                }
            }
            _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
        }
    }

    /// The Cast node's compile-time conversion table: may a value of
    /// `source` be CAST (converted / runtime-validated) into `target`?
    /// The one table the compiler's validate pass consults; the runtime
    /// side of the same door is [`Self::cast_value`], so a pair allowed
    /// here either converts or fails loudly at run time with a
    /// field-level message, and a pair refused here dies at compile
    /// time (`JsonDict -> Number` is nonsense, not a runtime surprise).
    ///
    /// Allowed, beyond plain compatibility (which needs no cast but is
    /// permitted as the trivial case):
    /// - `String` -> anything parseable from text (Number, Boolean, and
    ///   every structural type: the text is parsed as JSON, then held
    ///   to the target's contract);
    /// - anything text-serializable -> `String` (scalars print,
    ///   structures emit JSON; stored files, buses, and accesses do
    ///   not stringify);
    /// - `Number` <-> `Boolean` (1/0);
    /// - any generic or declared object shape (JsonDict, Dict, List,
    ///   Record, Named) -> a `Record` or `Named` target: the checked
    ///   claim, validated against the declared structure at run time.
    ///
    /// Unresolved endpoints answer Ok: `must-override-unmet` and the
    /// TypeVar machinery own those diagnostics.
    pub fn cast_allowed(source: &WeftType, target: &WeftType) -> Result<(), std::string::String> {
        if source.is_unresolved() || target.is_unresolved() {
            return Ok(());
        }
        if Self::is_compatible(source, target) {
            return Ok(());
        }
        let refused = || {
            Err(format!(
                "no cast from {source} into {target}; a cast parses text, stringifies data, \
                 or validates an object shape against a declared type"
            ))
        };
        // Union endpoints: one castable pairing is enough (the runtime
        // cast tries every member and refuses ambiguity loudly).
        if let WeftType::Union(members) = source {
            return if members.iter().any(|m| Self::cast_allowed(m, target).is_ok()) {
                Ok(())
            } else {
                refused()
            };
        }
        if let WeftType::Union(members) = target {
            return if members.iter().any(|m| Self::cast_allowed(source, m).is_ok()) {
                Ok(())
            } else {
                refused()
            };
        }
        let text_serializable = |t: &WeftType| {
            !t.references_file()
                && !matches!(t, WeftType::Bus | WeftType::Access | WeftType::Generator(_))
        };
        let object_shaped = |t: &WeftType| {
            matches!(
                t,
                WeftType::JsonDict
                    | WeftType::Dict(_, _)
                    | WeftType::List(_)
                    | WeftType::Record(_)
                    | WeftType::Named { .. }
            )
        };
        let ok = match (source, target) {
            (WeftType::Primitive(WeftPrimitive::String), t) => {
                matches!(t, WeftType::Primitive(WeftPrimitive::Number | WeftPrimitive::Boolean))
                    || object_shaped(t)
            }
            (s, WeftType::Primitive(WeftPrimitive::String)) => text_serializable(s),
            (
                WeftType::Primitive(WeftPrimitive::Number),
                WeftType::Primitive(WeftPrimitive::Boolean),
            )
            | (
                WeftType::Primitive(WeftPrimitive::Boolean),
                WeftType::Primitive(WeftPrimitive::Number),
            ) => true,
            (s, WeftType::Record(_) | WeftType::Named { .. }) => object_shaped(s),
            // Container targets from container sources lean on the
            // runtime cast's recursion (List[String] -> List[Number]).
            (WeftType::List(s_inner), WeftType::List(t_inner)) => {
                Self::cast_allowed(s_inner, t_inner).is_ok()
            }
            (WeftType::Dict(_, sv), WeftType::Dict(_, tv)) => Self::cast_allowed(sv, tv).is_ok(),
            _ => false,
        };
        if ok { Ok(()) } else { refused() }
    }

    /// Validate that a JSON value IS a value of this type, with an error
    /// naming the exact offending path (`messages[2].role: expected
    /// String, got Number`). Strict where the type is strict: a record
    /// refuses undeclared keys and missing required fields; a file
    /// primitive demands its stored-file marker (a raw URL string is not
    /// a stored file). Unlike [`Self::cast_value`] this never converts:
    /// the value either already matches or the error says why not.
    pub fn validate_value(&self, value: &serde_json::Value) -> Result<(), std::string::String> {
        let mut path = std::string::String::new();
        self.validate_at(value, &mut path)
    }

    fn validate_at(
        &self,
        value: &serde_json::Value,
        path: &mut std::string::String,
    ) -> Result<(), std::string::String> {
        use serde_json::Value;
        use std::fmt::Write;
        let here = |path: &std::string::String| {
            if path.is_empty() { "value".to_string() } else { path.clone() }
        };
        let mismatch = |path: &std::string::String| {
            Err(format!("{}: expected {self}, got {}", here(path), Self::infer(value)))
        };
        match self {
            WeftType::Primitive(p) => {
                let ok = match p {
                    WeftPrimitive::String => value.is_string(),
                    WeftPrimitive::Number => value.is_number(),
                    WeftPrimitive::Boolean => value.is_boolean(),
                    WeftPrimitive::Null => value.is_null(),
                    WeftPrimitive::Empty => false,
                    WeftPrimitive::Image
                    | WeftPrimitive::Video
                    | WeftPrimitive::Audio
                    | WeftPrimitive::Blob => value
                        .as_object()
                        .and_then(Self::detect_file_type)
                        .is_some_and(|t| t == WeftType::Primitive(*p)),
                };
                if ok { Ok(()) } else { mismatch(path) }
            }
            WeftType::List(inner) => match value {
                Value::Array(items) => {
                    for (i, item) in items.iter().enumerate() {
                        let len = path.len();
                        let _ = write!(path, "[{i}]");
                        inner.validate_at(item, path)?;
                        path.truncate(len);
                    }
                    Ok(())
                }
                _ => mismatch(path),
            },
            WeftType::Dict(_, v_ty) => match value {
                Value::Object(map) => {
                    for (k, v) in map {
                        let len = path.len();
                        let _ = write!(path, ".{k}");
                        v_ty.validate_at(v, path)?;
                        path.truncate(len);
                    }
                    Ok(())
                }
                _ => mismatch(path),
            },
            WeftType::JsonDict => {
                if value.is_object() { Ok(()) } else { mismatch(path) }
            }
            WeftType::Record(fields) => {
                let Value::Object(map) = value else { return mismatch(path) };
                if let Some(unknown) = map.keys().find(|k| !fields.iter().any(|f| &f.name == *k)) {
                    return Err(format!(
                        "{}: unknown field {unknown:?} (not declared on {self})",
                        here(path)
                    ));
                }
                for field in fields {
                    match map.get(&field.name) {
                        // An absent key (or a present null) is fine for
                        // an optional field, and for a field whose type
                        // itself admits null.
                        Some(Value::Null) | None
                            if field.optional || field.ty.contains_null() => {}
                        Some(v) => {
                            let len = path.len();
                            let _ = write!(path, ".{}", field.name);
                            field.ty.validate_at(v, path)?;
                            path.truncate(len);
                        }
                        None => {
                            return Err(format!(
                                "{}: missing required field {:?}",
                                here(path),
                                field.name
                            ))
                        }
                    }
                }
                Ok(())
            }
            WeftType::Union(members) => {
                let mut member_errors = Vec::new();
                for m in members {
                    match m.validate_at(value, &mut path.clone()) {
                        Ok(()) => return Ok(()),
                        Err(e) => member_errors.push((m, e)),
                    }
                }
                // Report the DEEP error when exactly one member even
                // matches the value's outer JSON kind (an array against
                // `String | List[Part]` can only have meant the list, so
                // its field-level path is the useful message); otherwise
                // the generic union mismatch.
                let candidates: Vec<&(&WeftType, std::string::String)> = member_errors
                    .iter()
                    .filter(|(m, _)| m.shallow_matches(value))
                    .collect();
                match candidates.as_slice() {
                    [(_, e)] => Err(e.clone()),
                    _ => mismatch(path),
                }
            }
            WeftType::Named { body, .. } => body
                .validate_at(value, path)
                .map_err(|e| format!("not a valid {self}: {e}")),
            WeftType::Bus => {
                if value.as_object().and_then(Self::detect_bus_type).is_some() {
                    Ok(())
                } else {
                    mismatch(path)
                }
            }
            WeftType::Access => {
                if value
                    .as_object()
                    .is_some_and(|o| o.contains_key(crate::access::ACCESS_MARKER_KEY))
                {
                    Ok(())
                } else {
                    mismatch(path)
                }
            }
            // A generator PORT value is the live-handle marker; the
            // items themselves are validated per emission against the
            // element type, never through this whole-value gate.
            WeftType::Generator(_) => {
                if value
                    .as_object()
                    .is_some_and(|o| o.contains_key(GENERATOR_MARKER_KEY))
                {
                    Ok(())
                } else {
                    mismatch(path)
                }
            }
            WeftType::TypeVar(_) | WeftType::MustOverride => Err(format!(
                "{}: cannot validate against unresolved type {self}",
                here(path)
            )),
        }
    }

    /// Does this DECLARED type accept `value` at run time? The one
    /// runtime gate semantics: the engine's output-type check and the
    /// firing-input readiness check both route here.
    /// A type carrying a declared shape (a `Named` or a `Record`
    /// anywhere) is checked by [`Self::validate_value`], its contract:
    /// inference can never produce a nominal name, so the infer path
    /// would refuse every legitimate value. Everything else keeps the
    /// structural infer-and-compare gate.
    pub fn accepts_runtime_value(&self, value: &serde_json::Value) -> bool {
        if self.contains_declared_shape() && !self.contains_unresolved_leaf() {
            self.validate_value(value).is_ok()
        } else {
            // A type still carrying an unresolved leaf (a TypeVar nested
            // in a union/container) keeps the permissive structural gate:
            // validate_at refuses unresolved leaves outright, which would
            // flip `A | T` from accept-anything to accept-only-A.
            Self::is_compatible(&Self::infer(value), self)
        }
    }

    /// True when a `Named` or `Record` sits anywhere in this type.
    fn contains_declared_shape(&self) -> bool {
        match self {
            WeftType::Named { .. } | WeftType::Record(_) => true,
            WeftType::List(inner) => inner.contains_declared_shape(),
            WeftType::Dict(k, v) => k.contains_declared_shape() || v.contains_declared_shape(),
            WeftType::Union(members) => members.iter().any(|m| m.contains_declared_shape()),
            _ => false,
        }
    }

    /// True when a `TypeVar` or `MustOverride` sits anywhere in this
    /// type. The compile-time unresolved-typevar diagnostic uses this
    /// (a nested unresolved leaf is as unusable as a bare one), and the
    /// runtime gate falls back to the structural check when one slipped
    /// through.
    // SYNC: contains_unresolved_leaf <-> packages/weft-graph/src/protocol.ts containsUnresolvedLeaf
    pub fn contains_unresolved_leaf(&self) -> bool {
        match self {
            WeftType::TypeVar(_) | WeftType::MustOverride => true,
            WeftType::List(inner) | WeftType::Generator(inner) => {
                inner.contains_unresolved_leaf()
            }
            WeftType::Dict(k, v) => k.contains_unresolved_leaf() || v.contains_unresolved_leaf(),
            WeftType::Union(members) => members.iter().any(|m| m.contains_unresolved_leaf()),
            WeftType::Record(fields) => fields.iter().any(|f| f.ty.contains_unresolved_leaf()),
            WeftType::Named { body, .. } => body.contains_unresolved_leaf(),
            _ => false,
        }
    }

    /// Does the value's OUTER JSON kind fit this type at all (an array
    /// for a list, an object for a dict/record, ...)? Only a triage for
    /// union error reporting; deep validation is [`Self::validate_at`].
    fn shallow_matches(&self, value: &serde_json::Value) -> bool {
        use serde_json::Value;
        match self {
            WeftType::Primitive(p) => match p {
                WeftPrimitive::String => value.is_string(),
                WeftPrimitive::Number => value.is_number(),
                WeftPrimitive::Boolean => value.is_boolean(),
                WeftPrimitive::Null => value.is_null(),
                WeftPrimitive::Empty => false,
                _ => value.is_object(),
            },
            WeftType::List(_) => matches!(value, Value::Array(_)),
            WeftType::Dict(_, _)
            | WeftType::JsonDict
            | WeftType::Record(_)
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Generator(_) => value.is_object(),
            WeftType::Union(members) => members.iter().any(|m| m.shallow_matches(value)),
            WeftType::Named { body, .. } => body.shallow_matches(value),
            WeftType::TypeVar(_) | WeftType::MustOverride => false,
        }
    }

    /// Unify a list of types into a single type.
    /// If all are identical, return that type. Otherwise, return a Union (deduplicated).
    // SYNC: unify_types <-> packages/weft-graph/src/webview/lib/types/index.ts unifyTypes
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
    // SYNC: WeftType::parse <-> packages/weft-graph/src/protocol.ts parseWeftType
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
            // Structural types: parse the file as JSON, then cast into
            // the declared type (`cast_value` covers plain compatibility
            // and the record/named contracts with field-level errors).
            _ => {
                let value: serde_json::Value = serde_json::from_str(text.trim())
                    .map_err(|e| format!("expected {}, file is not valid JSON: {}", self, e))?;
                self.cast_value(&value)
                    .map_err(|e| format!("file content does not fit @file's declared {self}: {e}"))
            }
        }
    }
}

fn parse_single_type(s: &str) -> Option<WeftType> {
    let s = s.trim();

    // SYNC: paren group <-> packages/weft-graph/src/protocol.ts parseSingleType (paren arm)
    // Parenthesized group: `(A | B)` is the type inside. Exists so a
    // named type's union body has an unambiguous wire form
    // (`Kind=(A | B)` vs the union `Kind=A | B`). Only strip when the
    // opening paren closes at the very end (otherwise `(A) | (B)`'s
    // halves would be mangled; the top-level union split already
    // protects that case, this guard keeps the function total).
    if s.starts_with('(') && s.ends_with(')') && find_top_level(&s[1..s.len() - 1], ')').is_none() {
        return WeftType::parse(&s[1..s.len() - 1]);
    }

    // Self-contained named form (`Name=Body`): the WIRE encoding of a
    // user-declared type (see `WeftType::wire_string`). The body rides
    // inline so a stored type string resolves in any process with no
    // registry. The `=` must be top-level: a record's fields use `:`.
    if let Some(eq) = find_top_level(s, '=') {
        let name = s[..eq].trim();
        let body = s[eq + 1..].trim();
        if TypeRegistry::check_declarable_name(name).is_ok() {
            let body = WeftType::parse(body)?;
            // A named type's whole contract is "one name, one body"
            // (equality and compatibility compare the NAME alone), so
            // this second construction door enforces the same
            // concreteness the registry enforces at declaration: no
            // type variable in the body, ever (no legitimate wire
            // string carries one). Parsing stays a PURE function of
            // the string on purpose: a stored project or journal row
            // must deserialize with no registry (and must keep
            // deserializing after the declaration is edited, so a
            // resumed execution gets the exact shape it started on);
            // agreement between same-named restatements is an
            // AUTHORING rule, enforced by the compiler's
            // `named-type-conflict` validation.
            // SYNC: named-body concreteness <-> packages/weft-graph/src/protocol.ts parseSingleType (Name=Body arm)
            if body.contains_unresolved_leaf() {
                return None;
            }
            return Some(WeftType::Named { name: name.to_string(), body: Box::new(body) });
        }
        return None;
    }

    // Record: `{ field: Type, field?: Type }`. At least one field (an
    // empty `{}` is not a type; use JsonDict for "some object").
    if let Some(inner) = s.strip_prefix('{').and_then(|r| r.strip_suffix('}')) {
        let mut fields: Vec<RecordField> = Vec::new();
        for part in split_top_level(inner, ',') {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let colon = find_top_level(part, ':')?;
            let mut name = part[..colon].trim();
            let optional = name.ends_with('?');
            if optional {
                name = name[..name.len() - 1].trim_end();
            }
            if name.is_empty()
                || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                || fields.iter().any(|f| f.name == name)
            {
                return None;
            }
            let ty = WeftType::parse(part[colon + 1..].trim())?;
            fields.push(RecordField { name: name.to_string(), ty, optional });
        }
        if fields.is_empty() {
            return None;
        }
        return Some(WeftType::Record(fields));
    }

    if s == "JsonDict" {
        return Some(WeftType::JsonDict);
    }

    if s == "Bus" {
        return Some(WeftType::Bus);
    }

    if s == "Access" {
        return Some(WeftType::Access);
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
            "Generator" => {
                let inner_type = WeftType::parse(inner)?;
                Some(WeftType::Generator(Box::new(inner_type)))
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
        // Declared names, through the one registry: builtin aliases
        // (Media, File: display sugar, expand structurally) and
        // user-declared nominal types (resolve to `Named`). See
        // `TypeRegistry` for how a registry becomes visible here.
        if let Some(resolved) = TypeRegistry::current().lookup(s) {
            return Some(resolved);
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
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => depth -= 1,
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

/// The byte position of the first top-level (outside `[]`/`{}`)
/// occurrence of `delimiter`, if any.
fn find_top_level(s: &str, delimiter: char) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => depth -= 1,
            c if c == delimiter && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

impl Default for WeftType {
    fn default() -> Self {
        WeftType::MustOverride
    }
}

impl WeftType {
    /// Render this type. `wire` decides how a `Named` prints: false is
    /// the AUTHORED form (the bare name, what humans read and write in
    /// source and metadata), true is the WIRE form (`Name=Body`, fully
    /// self-contained, what serialization emits so a stored type string
    /// resolves in any process with no registry).
    // SYNC: fmt_with <-> packages/weft-graph/src/webview/lib/types/index.ts weftTypeToString, weftTypeToWireString
    fn fmt_with(&self, out: &mut std::string::String, wire: bool) {
        use std::fmt::Write;
        match self {
            WeftType::Primitive(p) => out.push_str(p.as_str()),
            WeftType::List(inner) => {
                out.push_str("List[");
                inner.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Dict(k, v) => {
                out.push_str("Dict[");
                k.fmt_with(out, wire);
                out.push_str(", ");
                v.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Union(types) => {
                // An alias's member set renders under its NAME (see
                // UNION_ALIASES): `File`/`Media` must survive a
                // parse -> to_string round trip instead of leaking the
                // structural expansion into source lines and metadata.
                if let Some(name) = Self::union_alias_name(types) {
                    out.push_str(name);
                    return;
                }
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        out.push_str(" | ");
                    }
                    t.fmt_with(out, wire);
                }
            }
            WeftType::JsonDict => out.push_str("JsonDict"),
            WeftType::Bus => out.push_str("Bus"),
            WeftType::Access => out.push_str("Access"),
            WeftType::Generator(inner) => {
                out.push_str("Generator[");
                inner.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Record(fields) => {
                out.push('{');
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{}{}: ", field.name, if field.optional { "?" } else { "" });
                    field.ty.fmt_with(out, wire);
                }
                out.push('}');
            }
            WeftType::Named { name, body } => {
                out.push_str(name);
                if wire {
                    out.push('=');
                    // A union body is parenthesized so the rendered
                    // string is unambiguous: `Kind=(A | B)` is one named
                    // type, `Kind=A | B` would re-parse as the union
                    // `Kind=A | B` (a named member beside a plain one).
                    let parens = matches!(**body, WeftType::Union(_));
                    if parens {
                        out.push('(');
                    }
                    body.fmt_with(out, wire);
                    if parens {
                        out.push(')');
                    }
                }
            }
            WeftType::TypeVar(name) => out.push_str(name),
            WeftType::MustOverride => out.push_str("MustOverride"),
        }
    }

    /// The self-contained serialized form: like `to_string`, except a
    /// `Named` carries its body inline (`ChatHistory=List[...]`), so
    /// parsing the result never needs a registry. This is what
    /// `Serialize` emits (stored projects, journals, editor payloads);
    /// `Display` stays the authored bare-name form.
    // SYNC: wire_string <-> packages/weft-graph/src/webview/lib/types/index.ts weftTypeToWireString
    pub fn wire_string(&self) -> std::string::String {
        let mut out = std::string::String::new();
        self.fmt_with(&mut out, true);
        out
    }
}

impl std::fmt::Display for WeftType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = std::string::String::new();
        self.fmt_with(&mut out, false);
        f.write_str(&out)
    }
}

impl Serialize for WeftType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.wire_string())
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
