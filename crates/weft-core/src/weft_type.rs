use serde::{Deserialize, Serialize};

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Blob
// Parameterized:  List[T], Dict[String, V] (a dict's keys are always text)
// Records:        { name: String, age?: Number }, `..` last for an open one
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio;  File = Media | Blob
// Type variables: T, T1, T2... : node-scoped, same T on input and output = same type.
//                 A port whose type is decided where the node is used has one;
//                 a header port written with no type gets its own (`T__in__x`).
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

            /// The primitive `s` names, or `None` for any other word.
            /// Not `FromStr` (that trait answers with an error type;
            /// here absence is an ordinary answer, not a failure).
            pub fn from_name(s: &str) -> Option<Self> {
                match s {
                    $(stringify!($variant) => Some(WeftPrimitive::$variant)),+,
                    _ => None,
                }
            }

            /// Every primitive name, in declaration order. Lets a caller
            /// that has to KNOW the whole set (the syntax-highlighting
            /// vocabulary test) enumerate it instead of restating it.
            pub const ALL_NAMES: &'static [&'static str] = &[$(stringify!($variant)),+];
        }
    };
}

/// The type names that are not primitives but still mean something to
/// the type language: the two containers, the opaque dict, the
/// wired-only handles, and the two declaration markers. A node author
/// cannot name a type after any of them.
// SYNC: the type vocabulary (this list + define_primitives! below) <->
// packages/weft-syntax/weft.tmLanguage.json, packages/weft-syntax/highlight-weft.js,
// crates/weft-compiler/tests/integration/highlighting_vocabulary.rs (the word-list alarm),
// packages/weft-graph/src/protocol.ts CONTAINER_AND_SPECIAL_TYPES
pub const CONTAINER_AND_SPECIAL_TYPES: &[&str] =
    &["List", "Dict", "JsonDict", "Json", "Bus", "Access", "Infra", "Generator", "Secret"];

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

    /// The kind a media type names (`image/*` an Image...), its case
    /// ignored as HTTP ignores it.
    pub fn from_mime(mime: &str) -> Self {
        let mime = mime.trim_start().to_ascii_lowercase();
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
        [FileKind::Image, FileKind::Video, FileKind::Audio, FileKind::Blob]
            .into_iter()
            .find(|&kind| obj.contains_key(kind.marker_key()))
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

/// What [`TypeRegistry::extended`] does with a declaration whose name the
/// registry already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redeclaration {
    /// Two packages may ship one shared type: a structurally identical
    /// body is absorbed, a different one is a loud error.
    AbsorbIdentical,
    /// A source scope never redeclares a visible name, identical or not.
    Refuse,
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

    /// Why `body` does not parse against this registry: the reason a
    /// declaration [`TypeRegistry::extended`] could not resolve is
    /// stuck on. When it names other declarations that are stuck too
    /// (`stuck`), those are the reason (a cycle, or a chain ending on a
    /// name nobody declares, which that declaration's own line names);
    /// otherwise the parse error (a name nobody declares, a dict keyed by
    /// something other than text).
    fn reason_unparsed(&self, body: &str, stuck: &[&str]) -> std::string::String {
        let named: Vec<&str> = body
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .filter(|word| stuck.contains(word))
            .collect();
        if !named.is_empty() {
            let named: Vec<std::string::String> = named.iter().map(|n| format!("`{n}`")).collect();
            return format!("it names {}, which cannot resolve either", named.join(", "));
        }
        match std::sync::Arc::new(self.clone()).scoped_ref(|| WeftType::try_parse(body)) {
            Err(why) => why,
            Ok(_) => unreachable!("a body that parses against the final registry was resolved"),
        }
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
        Self::builtin().extended(declarations, Redeclaration::AbsorbIdentical)
    }

    /// This registry plus `declarations`, resolved the way [`build`] does
    /// (any order, fixed point, loud on a cycle or an unknown name). The
    /// scope form: a `.weft` file, or a group body, layers its own `type`
    /// declarations over whatever is visible around it. `policy` says
    /// what a name already present means: a package pair restating one
    /// shared type absorbs it, a source scope may never redeclare a name
    /// it can already see (the metadata table included), so nothing in
    /// the language shadows.
    ///
    /// [`build`]: Self::build
    pub fn extended(
        &self,
        declarations: &[(std::string::String, std::string::String, std::string::String)],
        policy: Redeclaration,
    ) -> Result<Self, std::string::String> {
        for (name, _, origin) in declarations {
            Self::check_declarable_name(name)
                .map_err(|e| format!("{origin}: type `{name}`: {e}"))?;
        }

        let mut registry = self.clone();
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
                            if policy == Redeclaration::Refuse {
                                return Err(format!(
                                    "{origin}: type `{name}` is already declared as \
                                     `{}`, and a name that is visible here cannot be \
                                     declared again; rename one",
                                    existing.body
                                ));
                            }
                            if existing.nominal && existing.body == body {
                                // Identical redeclaration: absorbed.
                            } else if existing.nominal {
                                // A name the registry held before this call has no
                                // origin here: say where it was, not "?".
                                let first = first_origin.get(name.as_str()).copied().unwrap_or("the registry this extends");
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
                let stuck: Vec<&str> = still_pending.iter().map(|(n, _, _)| n.as_str()).collect();
                let names: Vec<std::string::String> = still_pending
                    .iter()
                    .map(|(n, b, o)| format!("`{n}` = `{b}` ({o}): {}", registry.reason_unparsed(b, &stuck)))
                    .collect();
                return Err(format!("type declarations could not be resolved: {}", names.join("; ")));
            }
            pending = still_pending;
        }
    }

    /// A declarable name: an identifier starting with an uppercase
    /// letter, not colliding with anything the type language already
    /// means (a primitive, a container keyword, a special type, a
    /// TypeVar shape).
    // SYNC: check_declarable_name <-> packages/weft-graph/src/protocol.ts declarableNameProblem
    fn check_declarable_name(name: &str) -> Result<(), std::string::String> {
        let mut chars = name.chars();
        let valid_ident = chars.next().is_some_and(|c| c.is_ascii_uppercase())
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid_ident {
            return Err("a type name starts with an uppercase letter and contains only \
                        letters, digits, and underscores"
                .into());
        }
        let reserved = WeftPrimitive::from_name(name).is_some()
            || CONTAINER_AND_SPECIAL_TYPES.contains(&name)
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

/// One type variable a node or a group declares a bound for: `T` in
/// `Group[T: Media](file: T)`, or a `typeParams` entry in a node's
/// metadata. `resolved` is what the compiler worked the variable out to
/// be from what is connected (empty until then, and for good when
/// nothing pins it); the bound is then held to it, with the node's other
/// variables substituted into it as they resolve.
// SYNC: TypeParam <-> packages/weft-graph/src/protocol.ts TypeParamWire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeParam {
    pub name: std::string::String,
    pub bound: WeftType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<WeftType>,
}

impl TypeParam {
    /// A declared bound, not resolved yet.
    pub fn new(name: impl Into<std::string::String>, bound: WeftType) -> Self {
        Self { name: name.into(), bound, resolved: None }
    }

    /// The parameters a program writes after a type name, `[T: Media,
    /// T2: List[T]]` (the brackets included), in written order. Each
    /// entry is `Name: Bound`; [`Self::check_declarations`] then holds the
    /// list to the rules a declaration follows.
    pub fn parse_list(text: &str) -> Result<Vec<TypeParam>, std::string::String> {
        Self::written_entries(text)?
            .into_iter()
            .map(|entry| {
                let colon = find_top_level(entry, ':').ok_or_else(|| {
                    format!("type parameter `{entry}` has no bound; write `{entry}: <type>`")
                })?;
                let name = entry[..colon].trim();
                let bound = WeftType::try_parse(&entry[colon + 1..])
                    .map_err(|why| format!("the bound of type parameter `{name}`: {why}"))?;
                Ok(TypeParam::new(name, bound))
            })
            .collect()
    }

    /// The entries of a written parameter list (`[T: Media, T2: List[T]]`)
    /// as the author wrote each, trimmed, in order.
    pub fn written_entries(text: &str) -> Result<Vec<&str>, std::string::String> {
        let inner = text
            .trim()
            .strip_prefix('[')
            .and_then(|r| r.strip_suffix(']'))
            .ok_or_else(|| format!("`{text}` is not a type parameter list (`[T: Media]`)"))?;
        if inner.trim().is_empty() {
            return Err("an empty type parameter list; write `[T: Media]`, or drop the brackets".into());
        }
        Ok(split_top_level(inner, ',').into_iter().map(str::trim).collect())
    }

    /// Refuse a declaration that cannot be meant: a name that is not a
    /// type variable (`T`, `T1`...), one declared twice, or bounds that
    /// name each other in a cycle (`[T1: List[T2], T2: List[T1]]`),
    /// which is named. Whether a port uses each one is
    /// [`Self::check_used`], asked once every port is known.
    pub fn check_declarations(params: &[TypeParam]) -> Result<(), std::string::String> {
        let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for param in params {
            if !is_type_var_name(&param.name) || param.name.contains("__") || param.name == "T_Auto" {
                return Err(format!(
                    "`{}` is not a type variable: a type parameter is `T`, or `T` and a number (`T1`, `T2`)",
                    param.name
                ));
            }
            if !seen.insert(param.name.as_str()) {
                return Err(format!("type parameter `{}` is declared twice", param.name));
            }
        }
        Self::check_cycles(params)
    }

    /// Refuse bounds that name each other in a cycle, naming it: a bound
    /// mentioning a variable whose own bound leads back to it has no first
    /// variable to check. A variable bounded twice (a node type's bound
    /// and the one a program wrote over it) follows both.
    pub fn check_cycles(params: &[TypeParam]) -> Result<(), std::string::String> {
        let edges = |name: &str| -> Vec<std::string::String> {
            let mut vars = std::collections::BTreeSet::new();
            for param in params.iter().filter(|p| p.name == name) {
                param.bound.collect_type_vars(&mut vars);
            }
            vars.into_iter().filter(|v| params.iter().any(|p| &p.name == v)).collect()
        };
        for start in params {
            let mut path = vec![start.name.clone()];
            if let Some(cycle) = Self::find_cycle(&start.name, &edges, &mut path) {
                return Err(format!(
                    "the bounds of type parameters {} form a cycle ({}), so none of them can be checked first",
                    cycle[..cycle.len() - 1].iter().map(|v| format!("`{v}`")).collect::<Vec<_>>().join(", "),
                    cycle.join(" -> "),
                ));
            }
        }
        Ok(())
    }

    /// Refuse a parameter no port in `used` carries, and a bound naming
    /// a variable that is neither a parameter nor carried by a port:
    /// either bound could never check anything (nothing ever resolves
    /// the variable it waits on).
    pub fn check_used(
        params: &[TypeParam],
        used: &std::collections::BTreeSet<std::string::String>,
    ) -> Result<(), std::string::String> {
        if let Some(param) = params.iter().find(|p| !used.contains(&p.name)) {
            return Err(format!(
                "type parameter `{}` is declared, but no port uses it, so its bound would never check anything",
                param.name
            ));
        }
        for param in params {
            let mut named = std::collections::BTreeSet::new();
            param.bound.collect_type_vars(&mut named);
            if let Some(stray) =
                named.iter().find(|v| !used.contains(*v) && !params.iter().any(|p| &p.name == *v))
            {
                return Err(format!(
                    "the bound of type parameter `{}` names `{stray}`, which no port uses and no \
                     parameter declares, so it would never check anything",
                    param.name
                ));
            }
        }
        Ok(())
    }

    fn find_cycle(
        start: &str,
        edges: &dyn Fn(&str) -> Vec<std::string::String>,
        path: &mut Vec<std::string::String>,
    ) -> Option<Vec<std::string::String>> {
        let current = path.last().cloned().expect("the path starts with its first variable");
        for next in edges(&current) {
            if next == start {
                let mut cycle = path.clone();
                cycle.push(next);
                return Some(cycle);
            }
            if path.contains(&next) {
                continue;
            }
            path.push(next);
            if let Some(cycle) = Self::find_cycle(start, edges, path) {
                return Some(cycle);
            }
            path.pop();
        }
        None
    }

    /// Whether what the variable resolved to fits its bound. `None` when
    /// there is nothing to check yet: the variable is unresolved, or its
    /// bound still names one that is (that one's own unresolved port is
    /// what gets reported).
    // SYNC: violation <-> packages/weft-graph/src/webview/lib/types/index.ts typeParamViolation
    pub fn violation(&self) -> Option<std::string::String> {
        let resolved = self.resolved.as_ref()?;
        if self.bound.contains_unresolved_leaf() {
            return None;
        }
        (!WeftType::fits_within(resolved, &self.bound)).then(|| {
            format!(
                "`{}` has to fit in {}, and what is connected makes it {resolved}",
                self.name, self.bound
            )
        })
    }
}

/// Which side of a node a port is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortSide {
    Input,
    Output,
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

/// Which door a cast goes through, the one thing the two disagree on:
/// a value written into a port never turns a structure into text, and
/// the Cast node, asked outright, does. See [`WeftType::cast_value`] and
/// [`WeftType::cast_requested`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CastDoor {
    Literal,
    Requested,
}

/// What a file control offers for a port: the file type it picks (and
/// writes into `@asset(..., <Type>)`), and whether the port holds
/// several files rather than one.
#[derive(Debug, Clone, PartialEq)]
pub struct FileControl {
    pub file_type: WeftType,
    pub multiple: bool,
}

/// What a value holds at one place its type says is a secret
/// ([`WeftType::secret_places`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretPlace {
    /// The marker: the secret, made already.
    Sealed,
    /// A plain value, a secret yet to be made.
    Plain,
    /// A marker where the type declares no secret: a node sending the
    /// value outside the run would send the marker, never the value.
    Undeclared,
}

/// Recursive port type system.
#[derive(Debug, Clone, Eq)]
pub enum WeftType {
    /// Scalar: String, Number, Boolean, Image, Video, Audio, Blob
    Primitive(WeftPrimitive),
    /// Homogeneous list: List[T]
    List(Box<WeftType>),
    /// Key-value dict: `Dict[String, V]`, holding only the value type.
    /// A dict's keys are always text (a JSON object's are, and no value
    /// could have any other shape), so the key is not part of the type:
    /// the familiar spelling is kept and any other key is refused at
    /// parse, pointing at `List[{key: K, value: V}]`.
    Dict(Box<WeftType>),
    /// Union: String | Number
    Union(Vec<WeftType>),
    /// Opaque JSON dict: Dict[String, *] where value types are unchecked.
    /// Compatible with any Dict[String, V] in both directions.
    /// Use for raw API responses where the shape is unknown or too complex to declare.
    JsonDict,
    /// Any value made only of JSON: a string, a number, a boolean,
    /// null, or a list or object of those at any depth. For a value
    /// whose shape the program cannot know (a device's answer to an
    /// arbitrary command, a body the sender decides). Not an Any: a
    /// stored file, a bus, a stream, an access or an infra handle is
    /// no JSON value and never fits. Every JSON-made type flows in
    /// ([`WeftType::fits_json`]); a `Json` value flows out only into
    /// `Json` (narrowing it is the Cast node's job, which checks the
    /// value).
    // SYNC: WeftType::Json <-> packages/weft-graph/src/protocol.ts WeftType 'json'
    Json,
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
    /// A handle on one endpoint of an infra node: the value an infra
    /// node emits (`{"__weft_infra__": {...}}`, see
    /// `crate::infra::InfraHandle`) so other nodes reach that endpoint.
    /// One general type for every infra node; which node and endpoint it
    /// names is the value's, checked when it resolves. Wired-only: the
    /// marker is minted by an infra node, never typed as a literal.
    // SYNC: WeftType::Infra <-> packages/weft-graph/src/protocol.ts WeftType 'infra'
    Infra,
    /// A typed, one-directional, terminating stream: `Generator[T]`.
    /// The port itself accepts being emitted into multiple times; every
    /// item is checked against the element type `T`, the consumer pulls
    /// them one at a time, and the stream ends when the producer's body
    /// returns. Exactly one producer feeds exactly one consumer (a
    /// stream has one taker, compiler-enforced). Wired-only: the
    /// consumer's bag value is a live runtime handle, never a literal.
    // SYNC: WeftType::Generator <-> packages/weft-graph/src/protocol.ts WeftType 'generator'
    Generator(Box<WeftType>),
    /// A value its run's record never holds: `Secret[T]`, a marker naming
    /// a `T` kept sealed beside the record (`crate::secret`). Strict both
    /// ways: a `Secret[T]` fits only a `Secret[U]` whose `T` fits `U`,
    /// never a plain `T`, and nothing plain fits it, so a node that takes
    /// one says so and reveals it. Wired-only: a node makes it
    /// (`ctx.secret`), never a literal or a Cast.
    // SYNC: WeftType::Secret <-> packages/weft-graph/src/protocol.ts WeftType 'secret'
    Secret(Box<WeftType>),
    /// Dict with KNOWN field names and per-field types:
    /// `{ role: String, name?: String }`. `?` marks an optional field.
    /// A missing required field never matches. An undeclared key is
    /// refused on a closed record; an OPEN one (`{ role: String, .. }`)
    /// takes it and drops it wherever a value is held to the type and
    /// handed on ([`WeftType::conform_value`], [`WeftType::cast_value`]),
    /// so what flows on carries only the declared fields. Structural
    /// (two records with the same fields and openness are the same
    /// type); the nominal wrapper is [`WeftType::Named`].
    Record { fields: Vec<RecordField>, open: bool },
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
            (WeftType::Dict(a), WeftType::Dict(b)) => a == b,
            (WeftType::Union(a), WeftType::Union(b)) => {
                a.len() == b.len() && a.iter().all(|m| b.contains(m))
            }
            (WeftType::JsonDict, WeftType::JsonDict) => true,
            (WeftType::Json, WeftType::Json) => true,
            (WeftType::Bus, WeftType::Bus) => true,
            (WeftType::Access, WeftType::Access) => true,
            (WeftType::Infra, WeftType::Infra) => true,
            (WeftType::Generator(a), WeftType::Generator(b)) => a == b,
            (WeftType::Secret(a), WeftType::Secret(b)) => a == b,
            (
                WeftType::Record { fields: a, open: a_open },
                WeftType::Record { fields: b, open: b_open },
            ) => {
                a_open == b_open
                    && a.len() == b.len()
                    && a.iter().all(|fa| {
                        b.iter().any(|fb| {
                            fa.name == fb.name && fa.optional == fb.optional && fa.ty == fb.ty
                        })
                    })
            }
            (WeftType::Named { name: a, .. }, WeftType::Named { name: b, .. }) => a == b,
            (WeftType::TypeVar(a), WeftType::TypeVar(b)) => a == b,
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

    /// `Dict[String, value]`.
    pub fn dict(value: WeftType) -> Self {
        WeftType::Dict(Box::new(value))
    }

    /// A closed record of `fields` (an undeclared key is refused).
    pub fn record(fields: Vec<RecordField>) -> Self {
        WeftType::Record { fields, open: false }
    }

    // SYNC: WeftType::union <-> packages/weft-graph/src/protocol.ts unionOf
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

    /// The name of every union alias (`Media`, `File`), for a caller
    /// that needs the whole set rather than one lookup.
    pub fn union_alias_names() -> impl Iterator<Item = &'static str> {
        Self::UNION_ALIASES.iter().map(|(name, _)| *name)
    }

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
    /// Worked out once: every port's type asks it on every firing.
    pub fn file_primitives() -> &'static [WeftPrimitive] {
        static FILE_PRIMITIVES: std::sync::LazyLock<Vec<WeftPrimitive>> = std::sync::LazyLock::new(|| match WeftType::file() {
            WeftType::Union(types) => types
                .into_iter()
                .filter_map(|t| match t {
                    WeftType::Primitive(p) => Some(p),
                    _ => None,
                })
                .collect(),
            WeftType::Primitive(p) => vec![p],
            _ => Vec::new(),
        });
        &FILE_PRIMITIVES
    }

    pub fn type_var(name: &str) -> Self {
        WeftType::TypeVar(name.to_string())
    }

    /// The type variable of one port and no other: the type of a port
    /// written with no type, and of an input a key or a wire creates,
    /// taken from what is wired to it. An input's is `T__in__photo`, an
    /// output's `T__out__photo`: the side leads, so no two ports share
    /// one whatever their names, and the gates' are their own names'
    /// (`T__in___should_flow`), which no port a program declares can have.
    // SYNC: port_var <-> packages/weft-graph/src/protocol.ts portVar
    pub fn port_var(side: PortSide, port: &str) -> Self {
        match side {
            PortSide::Input => WeftType::TypeVar(format!("T__in__{port}")),
            PortSide::Output => WeftType::TypeVar(format!("T__out__{port}")),
        }
    }

    pub fn json_dict() -> Self {
        WeftType::JsonDict
    }

    // ── Queries ─────────────────────────────────────────────────────────

    pub fn is_type_var(&self) -> bool {
        matches!(self, WeftType::TypeVar(_))
    }

    /// A type variable: a type not yet resolved to a concrete one.
    pub fn is_unresolved(&self) -> bool {
        matches!(self, WeftType::TypeVar(_))
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

    /// Every file kind a value of this type may be as a WHOLE: the one
    /// kind of a file primitive, each file member of a union (`Image |
    /// Null` admits Image, `Media` Image, Video and Audio), nothing for
    /// containers and non-file types.
    pub fn admitted_file_kinds(&self) -> Vec<FileKind> {
        match self {
            WeftType::Primitive(p) => FileKind::from_primitive(*p).into_iter().collect(),
            WeftType::Named { body, .. } => body.admitted_file_kinds(),
            WeftType::Union(members) => members.iter().flat_map(Self::admitted_file_kinds).collect(),
            _ => Vec::new(),
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
    /// Whether a value going out on a port of this type opens a stream
    /// whose readers start reading at once: a `Bus` (its marker) or a
    /// `Generator[T]` (its first item). Once one is open, the readers
    /// already hold part of the output, which decides what a failure of
    /// the producer may still become (`context::caught_failure`).
    pub fn opens_a_stream(&self) -> bool {
        matches!(self.structural(), WeftType::Bus | WeftType::Generator(_))
    }

    // SYNC: structural <-> packages/weft-graph/src/webview/lib/types/index.ts structural
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
            WeftType::Dict(v) => v.references_file(),
            WeftType::Union(types) => types.iter().any(|t| t.references_file()),
            WeftType::Record { fields, .. } => fields.iter().any(|f| f.ty.references_file()),
            WeftType::Named { body, .. } => body.references_file(),
            // A generator's port value is a live handle; its ITEMS may
            // reference files, but nothing about the port value itself
            // is a file to cast or pick, so it sits with Bus here.
            WeftType::JsonDict
            | WeftType::Json
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Infra
            | WeftType::Generator(_)
            | WeftType::Secret(_)
            | WeftType::TypeVar(_) => false,
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

    /// What a file control would offer for this type, when one makes
    /// sense: the file type it picks and writes, and whether it may hold
    /// several. `Image` is one image; `List[Audio]` is several audio
    /// files; `Media | List[Media]` (an attachment port that takes one or
    /// many) is several too. None for a union mixing files with anything
    /// else, where a picker would take the other half away.
    pub fn file_control(&self) -> Option<FileControl> {
        if self.is_file_valued() {
            return Some(FileControl { file_type: self.clone(), multiple: false });
        }
        // A declared name over a list or a union of files offers what
        // its body offers (`type Attachments = List[Media]`).
        if let WeftType::Named { body, .. } = self {
            return body.file_control();
        }
        if let WeftType::List(inner) = self {
            if inner.is_file_valued() {
                return Some(FileControl { file_type: (**inner).clone(), multiple: true });
            }
        }
        let WeftType::Union(members) = self else {
            return None;
        };
        let mut files: Vec<WeftType> = Vec::new();
        let mut multiple = false;
        for member in members {
            match member {
                // A nullable file (`Image | Null`) still wants the
                // picker: Null narrows what may be LEFT EMPTY, not what
                // the control picks.
                WeftType::Primitive(WeftPrimitive::Null) => continue,
                m if m.is_file_valued() => files.push(m.clone()),
                WeftType::List(inner) if inner.is_file_valued() => {
                    multiple = true;
                    files.push((**inner).clone());
                }
                _ => return None,
            }
        }
        (!files.is_empty()).then(|| FileControl { file_type: WeftType::union(files), multiple })
    }

    /// True if the type includes Null as a valid value (null is legitimate
    /// data, not a skip signal).
    pub fn contains_null(&self) -> bool {
        match self {
            WeftType::Primitive(WeftPrimitive::Null) | WeftType::Json => true,
            WeftType::Union(types) => types.iter().any(|t| t.contains_null()),
            WeftType::Named { body, .. } => body.contains_null(),
            _ => false,
        }
    }

    /// The canonical zero value for this type: what an unwired,
    /// unseeded port of this type defaults to. Number -> 0, String ->
    /// "", Boolean -> false, List -> [], Dict/JsonDict -> {}. Types with
    /// no literal default (Null/Empty, Json, Media, Bus, and unresolved
    /// TypeVar) default to JSON null, the universal
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
            WeftType::Dict(_) | WeftType::JsonDict => Value::Object(serde_json::Map::new()),
            WeftType::Union(types) => types
                .iter()
                .find(|t| !t.contains_null())
                .map_or(Value::Null, |t| t.zero_value()),
            // An all-optional record zeroes to {}; one with required
            // fields has no honest zero (null, like Media).
            WeftType::Record { fields, .. } => {
                if fields.iter().all(|f| f.optional) {
                    Value::Object(serde_json::Map::new())
                } else {
                    Value::Null
                }
            }
            WeftType::Named { body, .. } => body.zero_value(),
            // Any JSON value is one, and none is more neutral than null.
            WeftType::Json
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Infra
            | WeftType::Generator(_)
            | WeftType::Secret(_)
            | WeftType::TypeVar(_) => Value::Null,
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
            (WeftType::Dict(a), WeftType::Dict(b)) => Self::is_compatible(a, b),
            // JsonDict: compatible with any Dict[String, V] in both directions
            (WeftType::JsonDict, WeftType::JsonDict) => true,
            (WeftType::JsonDict, WeftType::Dict(_)) => true,
            // An unchecked dict could carry a secret on into a port that
            // takes its value plain, so one holding a secret stays typed.
            (WeftType::Dict(v), WeftType::JsonDict) => !v.holds_secret(),
            // Json takes every JSON-made type, at any depth; Json itself
            // flows only into Json (and a union or a variable holding it).
            (src, WeftType::Json) => src.fits_json(),
            // A bus connects only to a bus; payloads are not type-checked.
            (WeftType::Bus, WeftType::Bus) => true,
            // An access connects only to an access; the KIND/service is
            // checked at runtime resolution, not by the type system.
            (WeftType::Access, WeftType::Access) => true,
            // An infra handle connects only to an infra handle; which
            // node it names is checked when it resolves.
            (WeftType::Infra, WeftType::Infra) => true,
            // A generator connects only to a same-element generator
            // (invariant in T, checked both ways): the stream contract
            // is one type end to end, so no variance-driven surprise
            // where an item flows into a widened consumer. A plain `T`
            // never accepts a `Generator[T]` and vice versa.
            // SYNC: generator compatibility <-> packages/weft-graph/src/webview/lib/types/index.ts isCompatible
            (WeftType::Generator(a), WeftType::Generator(b)) => {
                Self::is_compatible(a, b) && Self::is_compatible(b, a)
            }
            // A secret connects only to a secret of what it holds: a
            // plain `T` never takes a `Secret[T]` (the value would be a
            // marker its node cannot read) and a `Secret[T]` never takes
            // a plain `T` (only a node makes a secret).
            // SYNC: secret compatibility <-> packages/weft-graph/src/webview/lib/types/index.ts isCompatible
            (WeftType::Secret(a), WeftType::Secret(b)) => Self::is_compatible(a, b),
            // A NAMED type is nominal: the name is the contract, so only
            // the same name flows in. Same-named bodies are made to
            // agree by the compiler's `named-type-conflict` rule (not
            // by the parser, which stays registry-independent), so
            // name-only comparison is the deliberate nominal choice.
            (WeftType::Named { name: a, .. }, WeftType::Named { name: b, .. }) => a == b,
            // Records: the target's contract decides. Every source field
            // must be declared on the target unless the target is open
            // (it takes undeclared keys), every required target field
            // must be a required source field, and field types must
            // match pairwise. The SOURCE's openness says nothing here: a
            // value of an open record carries only its declared fields
            // once it was held to the type.
            (
                WeftType::Record { fields: sources, .. },
                WeftType::Record { fields: targets, open: target_open },
            ) => {
                (*target_open || sources.iter().all(|sf| targets.iter().any(|tf| tf.name == sf.name)))
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
            (WeftType::Record { .. }, WeftType::JsonDict) => !source.holds_secret(),
            // An optional field may hold null, so the dict's value type
            // must take null too.
            (WeftType::Record { fields, .. }, WeftType::Dict(v)) => fields.iter().all(|f| {
                Self::is_compatible(&f.ty, v)
                    && (!f.optional || Self::is_compatible(&WeftType::Primitive(WeftPrimitive::Null), v))
            }),
            (WeftType::JsonDict, WeftType::Record { .. }) => false,
            (WeftType::Dict(_), WeftType::Record { .. }) => false,
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

    /// Is every value of this type made only of JSON, so it fits a
    /// `Json` port? Text, numbers, booleans and null are; a list, a dict
    /// or a record is when what it holds is; a union when every member
    /// is; a declared name when its body is. A stored file, a bus, a
    /// stream, an access or an infra handle is not. A type variable
    /// answers yes, as it does for every compatibility question.
    // SYNC: fits_json <-> packages/weft-graph/src/webview/lib/types/index.ts fitsJson
    fn fits_json(&self) -> bool {
        match self {
            WeftType::Primitive(p) => matches!(
                p,
                WeftPrimitive::String
                    | WeftPrimitive::Number
                    | WeftPrimitive::Boolean
                    | WeftPrimitive::Null
                    | WeftPrimitive::Empty
            ),
            WeftType::List(inner) | WeftType::Dict(inner) => inner.fits_json(),
            WeftType::Union(members) => members.iter().all(Self::fits_json),
            WeftType::Record { fields, .. } => fields.iter().all(|f| f.ty.fits_json()),
            WeftType::Named { body, .. } => body.fits_json(),
            WeftType::JsonDict | WeftType::Json | WeftType::TypeVar(_) => true,
            WeftType::Bus | WeftType::Access | WeftType::Infra | WeftType::Generator(_) | WeftType::Secret(_) => false,
        }
    }

    /// Whether a `Secret` sits anywhere in this type, at any depth (a
    /// declared name read through its body).
    // SYNC: holds_secret <-> packages/weft-graph/src/webview/lib/types/index.ts holdsSecret
    pub fn holds_secret(&self) -> bool {
        match self {
            WeftType::Secret(_) => true,
            WeftType::List(inner) | WeftType::Dict(inner) | WeftType::Generator(inner) => inner.holds_secret(),
            WeftType::Union(members) => members.iter().any(Self::holds_secret),
            WeftType::Record { fields, .. } => fields.iter().any(|f| f.ty.holds_secret()),
            WeftType::Named { body, .. } => body.holds_secret(),
            WeftType::Primitive(_)
            | WeftType::JsonDict
            | WeftType::Json
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Infra
            | WeftType::TypeVar(_) => false,
        }
    }

    /// Where a value of this type holds a secret, or is to hold one: each
    /// place the type says is a `Secret` (a member of a union read as one
    /// when no other member takes the value), as a JSON pointer into
    /// `value`, with whether the value there is the marker already
    /// ([`SecretPlace::Sealed`]) or a plain value
    /// ([`SecretPlace::Plain`]). The type decides, never the value's
    /// look: a marker where the type says nothing of a secret is not one
    /// to reveal, and is named [`SecretPlace::Undeclared`]. One walk of
    /// the value.
    pub fn secret_places(&self, value: &serde_json::Value) -> Vec<(std::string::String, SecretPlace)> {
        let mut places = Vec::new();
        self.find_secret_places(value, &mut std::string::String::new(), &mut places);
        places
    }

    fn find_secret_places(
        &self,
        value: &serde_json::Value,
        at: &mut std::string::String,
        places: &mut Vec<(std::string::String, SecretPlace)>,
    ) {
        use serde_json::Value;
        let step = |at: &mut std::string::String, key: &str, ty: &WeftType, inner: &Value, places: &mut Vec<_>| {
            let len = at.len();
            crate::secret::push_pointer_key(at, key);
            ty.find_secret_places(inner, at, places);
            at.truncate(len);
        };
        match (self, value) {
            (WeftType::Secret(_), value) => {
                let place = match crate::secret::Secret::from_value(value) {
                    Some(_) => SecretPlace::Sealed,
                    None => SecretPlace::Plain,
                };
                places.push((at.clone(), place));
            }
            (WeftType::Named { body, .. }, value) => body.find_secret_places(value, at, places),
            (WeftType::List(inner), Value::Array(items)) => {
                for (i, item) in items.iter().enumerate() {
                    step(at, &i.to_string(), inner, item, places);
                }
            }
            (WeftType::Dict(inner), Value::Object(map)) => {
                for (key, item) in map {
                    step(at, key, inner, item, places);
                }
            }
            (WeftType::Record { fields, .. }, Value::Object(map)) => {
                for field in fields {
                    if let Some(item) = map.get(&field.name) {
                        step(at, &field.name, &field.ty, item, places);
                    }
                }
                // What an open record holds beyond its fields.
                for (key, item) in map.iter().filter(|(key, _)| !fields.iter().any(|field| field.name == **key)) {
                    let len = at.len();
                    crate::secret::push_pointer_key(at, key);
                    undeclared(item, at, places);
                    at.truncate(len);
                }
            }
            // The member that takes the value is the one that says where
            // its secrets are. Of the members that take it, the first that
            // declares every marker in it wins (a `JsonDict` member takes
            // any object, its markers included, so the order members are
            // written in never decides whether a declared secret reads as
            // one); with none taking it (a member holding a secret takes
            // only its marker there), the one member that is a secret, else
            // the one member holding a secret, is where a plain value is to
            // become one.
            (WeftType::Union(members), value) => {
                let mut first_taker = None;
                for member in members.iter().filter(|m| m.validate_value(value).is_ok()) {
                    let mut found = Vec::new();
                    member.find_secret_places(value, at, &mut found);
                    if !found.iter().any(|(_, place)| *place == SecretPlace::Undeclared) {
                        places.extend(found);
                        return;
                    }
                    first_taker.get_or_insert(found);
                }
                match first_taker {
                    Some(found) => places.extend(found),
                    None => {
                        let lone = |holds: fn(&WeftType) -> bool| {
                            let mut found = members.iter().filter(|m| holds(m));
                            match (found.next(), found.next()) {
                                (Some(member), None) => Some(member),
                                _ => None,
                            }
                        };
                        match lone(|m| matches!(m.structural(), WeftType::Secret(_))).or_else(|| lone(WeftType::holds_secret)) {
                            Some(member) => member.find_secret_places(value, at, places),
                            None => undeclared(value, at, places),
                        }
                    }
                }
            }
            (_, value) => undeclared(value, at, places),
        }
        /// Every marker in `value`, which sits at `at` where the type
        /// declares no secret.
        fn undeclared(value: &serde_json::Value, at: &str, places: &mut Vec<(std::string::String, SecretPlace)>) {
            places.extend(crate::secret::markers_in(value).into_iter().map(|(inner, _)| (format!("{at}{inner}"), SecretPlace::Undeclared)));
        }
    }

    /// May an author declare `declared` on a port whose type can be at
    /// most `bound`: a narrowable output's call-site type, or a type
    /// parameter's binding against its bound? Every value of `declared`
    /// must be a value of `bound`, which is [`Self::is_compatible`] with
    /// two differences: a stream narrows ITEM-wise (`Generator[Signup]`
    /// fits `Generator[JsonDict]`), where wiring one stream into another
    /// stays invariant, and a record or dict holding a secret fits a
    /// `JsonDict` bound, where wiring it into one is refused. A named bound stays nominal (only its own name
    /// fits it); a named declaration is read through its body.
    // SYNC: fits_within <-> packages/weft-graph/src/webview/lib/types/index.ts fitsWithin
    pub fn fits_within(declared: &WeftType, bound: &WeftType) -> bool {
        match (declared.structural(), bound) {
            (WeftType::Generator(a), WeftType::Generator(b)) => Self::fits_within(a, b),
            // Every value of a record or a dict is an object, one holding a
            // secret included: declaring one on an output that gives some
            // object says what it holds (wiring it on into an unchecked
            // dict is still refused, `is_compatible`).
            (WeftType::Record { .. } | WeftType::Dict(_), WeftType::JsonDict) => true,
            _ => Self::is_compatible(declared, bound),
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
                if obj.contains_key(crate::infra::INFRA_MARKER_KEY) {
                    return WeftType::Infra;
                }
                // A marker names a secret, never what it holds.
                if crate::secret::Secret::from_value(value).is_some() {
                    return WeftType::Secret(Box::new(WeftType::type_var("T")));
                }
                if obj.is_empty() {
                    return WeftType::dict(WeftType::Primitive(WeftPrimitive::Empty));
                }
                let value_types: Vec<WeftType> = obj.values().map(Self::infer).collect();
                WeftType::dict(Self::unify_types(&value_types))
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
    /// Each payload has a handle (`key` for a stored file, `url` for an
    /// outside one) + `mimeType`: the two handles weft can read the
    /// bytes through ([`crate::storage::FileHandle::from_value`]), so a
    /// marker carrying anything else is no file value.
    // SYNC: detect_file_type <-> packages/weft-graph/src/webview/lib/types/index.ts FILE_HANDLE_KEYS
    fn detect_file_type(obj: &serde_json::Map<String, serde_json::Value>) -> Option<WeftType> {
        let kind = FileKind::from_marker_obj(obj)?;
        let payload = obj.get(kind.marker_key())?.as_object()?;
        let has_handle = payload.contains_key("url") || payload.contains_key("key");
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

    /// A handle nested in a record or list still belongs to its original run.
    pub fn contains_bus_handle(value: &serde_json::Value) -> bool {
        if Self::bus_marker_id(value).is_some() { return true; }
        match value {
            serde_json::Value::Array(items) => items.iter().any(Self::contains_bus_handle),
            serde_json::Value::Object(fields) => fields.values().any(Self::contains_bus_handle),
            _ => false,
        }
    }

    /// Public helper: extract the mode from a Bus marker. Returns
    /// `Some(JournalMode)` on success, `None` if the value is not a Bus
    /// marker OR the mode field is missing OR the field carries an
    /// unrecognised string. Callers MUST treat `None` as "this is not
    /// a routable bus marker for THIS run"; do NOT default to
    /// `Journaled`, since silently misclassifying ephemeral as
    /// journaled would route a frame-rate stream into permanent
    /// journal storage.
    #[cfg(feature = "runtime")]
    pub fn bus_marker_mode(value: &serde_json::Value) -> Option<crate::stream_journal::JournalMode> {
        let s = Self::detect_bus_type(value.as_object()?)?.get("mode")?.as_str()?;
        crate::stream_journal::JournalMode::from_wire_str(s)
    }

    /// Build a Bus marker JSON value from a channel id and a mode.
    /// Takes `JournalMode` (not `&str`) so the wire-vocabulary invariant
    /// is enforced at the type system; a typo can't slip through.
    /// `id` and `mode` are the marker's whole payload: every other bus
    /// parameter is read off the live handle a consumer resolves from
    /// the marker.
    #[cfg(feature = "runtime")]
    pub fn bus_marker(id: &str, mode: crate::stream_journal::JournalMode) -> serde_json::Value {
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
    /// - Null, Bus, TypeVar, and file types never cast
    ///   (a file value is a marker, not convertible data);
    /// - a list or an object never becomes text here: one written where
    ///   text is wanted is a mistake far more often than a wish. The
    ///   Cast node, asked outright, does it ([`Self::cast_requested`]).
    pub fn cast_value(&self, value: &serde_json::Value) -> Result<serde_json::Value, std::string::String> {
        self.cast_through(value, CastDoor::Literal)
    }

    /// Cast a JSON value into this type the way the Cast node does: the
    /// rules of [`Self::cast_value`], plus a list or an object into
    /// `String` (at any depth) is its JSON text, the one conversion
    /// [`Self::cast_allowed`] grants a program that asks for it and a
    /// written value never gets.
    pub fn cast_requested(&self, value: &serde_json::Value) -> Result<serde_json::Value, std::string::String> {
        self.cast_through(value, CastDoor::Requested)
    }

    fn cast_through(&self, value: &serde_json::Value, door: CastDoor) -> Result<serde_json::Value, std::string::String> {
        use serde_json::Value;
        // Exact fit: hand it back untouched (preserves the written form).
        if Self::is_compatible(&Self::infer(value), self) {
            return Ok(value.clone());
        }
        if value.is_null() {
            return Err("null has no cast".into());
        }
        // A file-valued type (a bare Image/Media/...) never converts: a
        // file value is a marker, not convertible data, and a marker of
        // the right kind already passed the compatibility fast path
        // above. What is left is a marker of another kind (a blob
        // claimed as an image) or no marker at all; each is named.
        // Containers and records HOLDING files recurse below.
        if self.is_file_valued() {
            return Err(match Self::infer(value) {
                found if found.is_file_valued() => format!("the value is a stored {found}, not {self}"),
                _ => format!(
                    "no cast into {self}: a stored file is its marker object \
                     (`__weft_image__`, `__weft_video__`, `__weft_audio__` or `__weft_blob__` \
                     holding key, mimeType, sizeBytes and filename), and this value is not one"
                ),
            });
        }
        if matches!(self.structural(), WeftType::Secret(_)) {
            return Err(crate::secret::MADE_BY_A_NODE.to_string());
        }
        if self.is_unresolved()
            || matches!(self, WeftType::Bus | WeftType::Access | WeftType::Infra | WeftType::Generator(_))
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
                    Some(1.0) => Ok(Value::Bool(true)),
                    Some(0.0) => Ok(Value::Bool(false)),
                    _ => Err(format!("{n} is not a boolean (only 1/0 cast)")),
                },
                _ => Err(format!("no cast from {} into Boolean", Self::infer(value))),
            },
            WeftType::Primitive(WeftPrimitive::String) => match value {
                Value::Number(n) => Ok(Value::String(n.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                Value::Array(_) | Value::Object(_) if door == CastDoor::Requested => {
                    Ok(Value::String(value.to_string()))
                }
                _ => Err(format!("no cast from {} into String", Self::infer(value))),
            },
            WeftType::List(inner) => match value {
                Value::Array(items) => items
                    .iter()
                    .map(|v| inner.cast_through(v, door))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array),
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    self.cast_through(&parsed, door)
                }
                _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
            },
            WeftType::Dict(v_ty) => match value {
                Value::Object(map) => {
                    let mut out = serde_json::Map::new();
                    for (k, v) in map {
                        out.insert(k.clone(), v_ty.cast_through(v, door)?);
                    }
                    Ok(Value::Object(out))
                }
                Value::String(s) => {
                    let parsed: Value = serde_json::from_str(s.trim())
                        .map_err(|e| format!("{s:?} is not valid JSON: {e}"))?;
                    self.cast_through(&parsed, door)
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
            // undeclared keys refused (dropped by an open record),
            // required fields demanded, an optional field's null kept as
            // written. A string parses as JSON first.
            WeftType::Record { fields, open } => match value {
                Value::Object(map) => {
                    if !open {
                        if let Some(unknown) =
                            map.keys().find(|k| !fields.iter().any(|f| &f.name == *k))
                        {
                            return Err(format!("unknown field {unknown:?} (not declared on {self})"));
                        }
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
                                let cast = field.ty.cast_through(v, door).map_err(|e| {
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
                    self.cast_through(&parsed, door)
                }
                _ => Err(format!("no cast from {} into {self}", Self::infer(value))),
            },
            // A named type casts through its body; the RESULT is a value
            // of the named type (nominal-ness lives in the type system,
            // values carry no name tag).
            WeftType::Named { body, .. } => body
                .cast_through(value, door)
                .map_err(|e| format!("not a valid {self}: {e}")),
            // Exact compatibility was tried above; now EVERY member's
            // cast. One success (or several agreeing) wins; members
            // disagreeing on the result is ambiguous and refused with
            // both readings, so the writer disambiguates instead of the
            // compiler guessing.
            WeftType::Union(members) => {
                // A list or an object turned into its JSON text is the
                // reading of last resort, at any depth: every member is
                // tried the literal way first, and only when none takes
                // the value is the text allowed (`String | List[String]`
                // handed `[1, 2]` is `["1", "2"]`, and a declared name
                // over `String | Number` beside it changes nothing).
                let readings = |door: CastDoor| -> Vec<(&WeftType, serde_json::Value)> {
                    members
                        .iter()
                        .filter_map(|m| m.cast_through(value, door).ok().map(|v| (m, v)))
                        .collect()
                };
                let mut successes = readings(CastDoor::Literal);
                if successes.is_empty() && door == CastDoor::Requested {
                    successes = readings(CastDoor::Requested);
                }
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
    /// side of the same door is [`Self::cast_requested`], so a pair allowed
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
    /// - any generic or declared object shape (JsonDict, Json, Dict,
    ///   List, Record, Named) -> a `Record` or `Named` target: the
    ///   checked claim, validated against the declared structure at run
    ///   time;
    /// - `Json` -> any JSON-made type (a `Json` value may be any of
    ///   them; the run-time cast checks this one is).
    ///
    /// Unresolved endpoints answer Ok: `unresolved-typevar` owns those.
    pub fn cast_allowed(source: &WeftType, target: &WeftType) -> Result<(), std::string::String> {
        // Before anything else, a compatible pair included: a cast never
        // makes a secret (only a node does) and never opens one (a node
        // reveals it).
        if target.holds_secret() {
            return Err(format!("no cast into {target}: {}", crate::secret::MADE_BY_A_NODE));
        }
        if source.holds_secret() {
            return Err(format!(
                "no cast from {source}: a Secret stays sealed on its way, so a node that takes {source} reveals it"
            ));
        }
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
                && !matches!(t, WeftType::Bus | WeftType::Access | WeftType::Infra | WeftType::Generator(_) | WeftType::Secret(_))
        };
        let object_shaped = |t: &WeftType| {
            matches!(
                t,
                WeftType::JsonDict
                    | WeftType::Json
                    | WeftType::Dict(_)
                    | WeftType::List(_)
                    | WeftType::Record { .. }
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
            (s, WeftType::Record { .. } | WeftType::Named { .. }) => object_shaped(s),
            // A stored file that went through a database comes back as
            // a plain object holding its marker; the cast checks the
            // marker (and that its kind is the target's) at run time.
            (s, t) if t.is_file_valued() => object_shaped(s),
            // A Json value may already be any JSON-made type; the cast
            // checks at run time that this one is.
            (WeftType::Json, t) => t.fits_json(),
            // Container targets from container sources lean on the
            // runtime cast's recursion (List[String] -> List[Number]).
            (WeftType::List(s_inner), WeftType::List(t_inner)) => {
                Self::cast_allowed(s_inner, t_inner).is_ok()
            }
            (WeftType::Dict(sv), WeftType::Dict(tv)) => Self::cast_allowed(sv, tv).is_ok(),
            _ => false,
        };
        if ok { Ok(()) } else { refused() }
    }

    /// Validate that a JSON value IS a value of this type, with an error
    /// naming the exact offending path (`messages[2].role: expected
    /// String, got Number`). Strict where the type is strict: a record
    /// refuses missing required fields, and a closed one undeclared keys;
    /// a file primitive demands its stored-file marker (a raw URL string
    /// is not a stored file). Unlike [`Self::cast_value`] this never
    /// converts: the value either already matches or the error says why
    /// not.
    pub fn validate_value(&self, value: &serde_json::Value) -> Result<(), std::string::String> {
        let mut path = std::string::String::new();
        self.validate_at(value, &mut path)
    }

    /// Hold `value` to this type and hand back what flows on: the value
    /// itself, with every key an OPEN record does not declare dropped
    /// (at any depth). The gate where a value takes on a declared type
    /// at run time (a node's emit) goes through here, so `{ email:
    /// String, .. }` passes `{"email": "a@b", "utm": "x"}` on as
    /// `{"email": "a@b"}`. A type with no open record in it returns the
    /// value untouched, without a walk.
    pub fn conform_value(&self, value: serde_json::Value) -> Result<serde_json::Value, std::string::String> {
        self.validate_value(&value)?;
        if !self.has_open_record() {
            return Ok(value);
        }
        self.drop_undeclared(value)
    }

    /// Whether an open record sits anywhere in this type (the one case
    /// [`Self::conform_value`] rewrites a value).
    fn has_open_record(&self) -> bool {
        match self {
            WeftType::Record { fields, open } => *open || fields.iter().any(|f| f.ty.has_open_record()),
            WeftType::List(inner) | WeftType::Dict(inner) | WeftType::Generator(inner) => {
                inner.has_open_record()
            }
            WeftType::Union(members) => members.iter().any(Self::has_open_record),
            WeftType::Named { body, .. } => body.has_open_record(),
            _ => false,
        }
    }

    /// `value` (already validated against this type) with the keys open
    /// records do not declare removed. A union rewrites through the
    /// members that accept the value; when two of them would hand back
    /// different values (`{a: String, ..} | JsonDict` meeting
    /// `{"a": "x", "b": "y"}`), what flows on would depend on the order
    /// the union was written in, so it is refused the way an ambiguous
    /// [`Self::cast_value`] is.
    fn drop_undeclared(&self, value: serde_json::Value) -> Result<serde_json::Value, std::string::String> {
        use serde_json::Value;
        Ok(match (self, value) {
            (WeftType::Record { fields, open }, Value::Object(map)) => {
                let mut kept = serde_json::Map::new();
                for (key, inner) in map {
                    match fields.iter().find(|f| f.name == key) {
                        Some(field) => {
                            kept.insert(key, field.ty.drop_undeclared(inner)?);
                        }
                        // Validation already refused an undeclared key on
                        // a closed record, so only an open one sees it.
                        None if !open => {
                            kept.insert(key, inner);
                        }
                        None => {}
                    }
                }
                Value::Object(kept)
            }
            (WeftType::List(inner), Value::Array(items)) => Value::Array(
                items.into_iter().map(|item| inner.drop_undeclared(item)).collect::<Result<_, _>>()?,
            ),
            (WeftType::Dict(inner), Value::Object(map)) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| inner.drop_undeclared(v).map(|v| (k, v)))
                    .collect::<Result<_, _>>()?,
            ),
            (WeftType::Named { body, .. }, value) => body.drop_undeclared(value)?,
            (WeftType::Union(members), value) => {
                let readings: Vec<(&WeftType, Value)> = members
                    .iter()
                    .filter(|m| m.validate_value(&value).is_ok())
                    .map(|m| m.drop_undeclared(value.clone()).map(|v| (m, v)))
                    .collect::<Result<_, _>>()?;
                match readings.as_slice() {
                    [] => value,
                    [(_, first), rest @ ..] if rest.iter().all(|(_, v)| v == first) => first.clone(),
                    all => {
                        let readings: Vec<std::string::String> =
                            all.iter().map(|(m, v)| format!("{v} ({m})")).collect();
                        return Err(format!(
                            "ambiguous value for {self}: it reads as {}; declare the union \
                             so one member takes it",
                            readings.join(" or ")
                        ));
                    }
                }
            }
            (_, value) => value,
        })
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
            WeftType::Dict(v_ty) => match value {
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
            // Every JSON value is one, and like JsonDict nothing inside
            // is held to anything.
            WeftType::Json => Ok(()),
            WeftType::Record { fields, open } => {
                let Value::Object(map) = value else { return mismatch(path) };
                if !open {
                    if let Some(unknown) = map.keys().find(|k| !fields.iter().any(|f| &f.name == *k)) {
                        return Err(format!(
                            "{}: unknown field {unknown:?} (not declared on {self}; a type that \
                             should ignore keys it does not declare ends with `..`)",
                            here(path)
                        ));
                    }
                }
                for field in fields {
                    match map.get(&field.name) {
                        // Optional fields may be absent or null. A required
                        // nullable field must still be present; its value
                        // is checked against the declared type below.
                        Some(Value::Null) | None if field.optional => {}
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
            WeftType::Infra => {
                if crate::infra::InfraHandle::from_value(value).is_ok() {
                    Ok(())
                } else {
                    mismatch(path)
                }
            }
            // A secret is its marker, whatever it holds: the value itself
            // is never in hand to check.
            WeftType::Secret(_) => {
                if crate::secret::Secret::from_value(value).is_some() {
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
            // A leaf nothing pinned yet (a node test leaving a type
            // variable undeclared) stands for any type: there is nothing
            // to hold the value to here, and the shape around it is
            // still checked.
            WeftType::TypeVar(_) => Ok(()),
        }
    }

    /// Does this DECLARED type accept `value` at run time? The one
    /// runtime gate semantics: the engine's output-type check and the
    /// firing-input readiness check both route here, through
    /// [`Self::validate_value`] (with [`Self::conform_value`] for the
    /// emit, which also hands back what flows on).
    ///
    /// The declared type is the question and the value is only held
    /// against it: a `JsonDict` port takes any object, whatever keys it
    /// carries, and only a port declared as a file reads the file
    /// marker. Guessing the value's type from its shape first and
    /// comparing the guess would refuse a plain dict that happens to
    /// carry a marker, and could never accept a nominal name, since
    /// inference produces none. An unresolved leaf (a bare `T`, or one
    /// nested in a container or record) takes any value, and the shape
    /// around it is held as usual.
    pub fn accepts_runtime_value(&self, value: &serde_json::Value) -> bool {
        self.validate_value(value).is_ok()
    }

    /// True when a `TypeVar` sits anywhere in this
    /// type. The compile-time unresolved-typevar diagnostic uses this
    /// (a nested unresolved leaf is as unusable as a bare one).
    // SYNC: contains_unresolved_leaf <-> packages/weft-graph/src/protocol.ts containsUnresolvedLeaf
    pub fn contains_unresolved_leaf(&self) -> bool {
        match self {
            WeftType::TypeVar(_) => true,
            WeftType::List(inner) | WeftType::Generator(inner) | WeftType::Secret(inner) | WeftType::Dict(inner) => {
                inner.contains_unresolved_leaf()
            }
            WeftType::Union(members) => members.iter().any(|m| m.contains_unresolved_leaf()),
            WeftType::Record { fields, .. } => fields.iter().any(|f| f.ty.contains_unresolved_leaf()),
            WeftType::Named { body, .. } => body.contains_unresolved_leaf(),
            _ => false,
        }
    }

    /// Every type variable named anywhere in this type, into `out`.
    pub fn collect_type_vars(&self, out: &mut std::collections::BTreeSet<std::string::String>) {
        match self {
            WeftType::TypeVar(name) => {
                out.insert(name.clone());
            }
            WeftType::List(inner) | WeftType::Generator(inner) | WeftType::Secret(inner) | WeftType::Dict(inner) => {
                inner.collect_type_vars(out)
            }
            WeftType::Union(members) => members.iter().for_each(|m| m.collect_type_vars(out)),
            WeftType::Record { fields, .. } => fields.iter().for_each(|f| f.ty.collect_type_vars(out)),
            WeftType::Named { body, .. } => body.collect_type_vars(out),
            _ => {}
        }
    }

    /// Replace every occurrence of the variable `var` with `concrete`, at
    /// any depth, and say whether anything moved. The one substitution
    /// step: resolving a node's ports and a type parameter's bound both
    /// go through it. It rewrites every shape a variable can OCCUR in,
    /// unions included: a union member's `T` is the same node-scoped
    /// variable a sibling port binds. No `Named` arm: a declared body
    /// never holds a variable (the registry and the wire parser both
    /// refuse one).
    // SYNC: substitute_var <-> packages/weft-graph/src/webview/lib/types/index.ts substituteTypeVars
    pub fn substitute_var(&mut self, var: &str, concrete: &WeftType) -> bool {
        match self {
            WeftType::TypeVar(n) if n == var => {
                *self = concrete.clone();
                true
            }
            WeftType::List(inner) | WeftType::Generator(inner) | WeftType::Secret(inner) | WeftType::Dict(inner) => {
                inner.substitute_var(var, concrete)
            }
            WeftType::Union(members) => {
                let mut any = false;
                for m in members.iter_mut() {
                    any |= m.substitute_var(var, concrete);
                }
                // `T | Null` with `T` a union is one flat union.
                if any {
                    *self = WeftType::union(std::mem::take(members));
                }
                any
            }
            WeftType::Record { fields, .. } => {
                let mut any = false;
                for f in fields.iter_mut() {
                    any |= f.ty.substitute_var(var, concrete);
                }
                any
            }
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
            WeftType::Dict(_)
            | WeftType::JsonDict
            | WeftType::Record { .. }
            | WeftType::Bus
            | WeftType::Access
            | WeftType::Infra
            | WeftType::Generator(_)
            | WeftType::Secret(_) => value.is_object(),
            WeftType::Json => true,
            WeftType::Union(members) => members.iter().any(|m| m.shallow_matches(value)),
            WeftType::Named { body, .. } => body.shallow_matches(value),
            WeftType::TypeVar(_) => false,
        }
    }

    /// Unify a list of types into a single type.
    /// If all are identical, return that type. Otherwise, return a Union (deduplicated).
    // SYNC: unify_types <-> packages/weft-graph/src/webview/lib/types/index.ts unifyTypes
    pub fn unify_types(types: &[WeftType]) -> WeftType {
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

    /// Parse a port type string, `None` when it is not one. The callers
    /// that only need yes or no (deserializing a stored type, which was
    /// valid when written); an author's text goes through
    /// [`Self::try_parse`], which says what is wrong.
    // SYNC: WeftType::parse <-> packages/weft-graph/src/protocol.ts parseWeftType
    pub fn parse(s: &str) -> Option<Self> {
        Self::try_parse(s).ok()
    }

    /// Parse a port type string, or say why it is not one. Strict: no
    /// bare List/Dict/Any, and a dict keyed by anything but text is
    /// refused naming the shape that works.
    ///
    /// Valid: "String", "List[String]", "Dict[String, Number]",
    ///        "String | Number", "Media", "T", "T1", "T2",
    ///        "List[T]", "Dict[String, T1 | T2]", "{ a: String, .. }"
    /// Invalid: "Any", "List", "Dict", "Foo", "Dict[Number, String]"
    // SYNC: WeftType::try_parse <-> packages/weft-graph/src/protocol.ts tryParseWeftType
    pub fn try_parse(s: &str) -> Result<Self, std::string::String> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err("an empty type".into());
        }

        let parts = split_top_level(trimmed, '|');
        if parts.len() > 1 {
            let types: Vec<WeftType> = parts
                .iter()
                .map(|p| parse_single_type(p.trim()))
                .collect::<Result<_, _>>()?;
            return Ok(WeftType::union(types));
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
    /// Unresolved targets (a `TypeVar`) are rejected: a `@file`
    /// cast must name a concrete type.
    pub fn cast_text(&self, text: &str) -> Result<serde_json::Value, std::string::String> {
        match self {
            WeftType::Named { body, .. } => body.cast_text(text)
                .map_err(|error| format!("{self}: {error}")),
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
            WeftType::TypeVar(_) => {
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

fn parse_single_type(s: &str) -> Result<WeftType, std::string::String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("an empty type".into());
    }

    // SYNC: paren group <-> packages/weft-graph/src/protocol.ts parseSingleType (paren arm)
    // Parenthesized group: `(A | B)` is the type inside. Exists so a
    // named type's union body has an unambiguous wire form
    // (`Kind=(A | B)` vs the union `Kind=A | B`). Only strip when the
    // opening paren closes at the very end: `(A)(B)` is not one group,
    // and stripping its outer pair would leave `A)(B`.
    if s.starts_with('(') && opening_bracket_closes_last(s) {
        return WeftType::try_parse(&s[1..s.len() - 1]);
    }

    // Self-contained named form (`Name=Body`): the WIRE encoding of a
    // user-declared type (see `WeftType::wire_string`). The body rides
    // inline so a stored type string resolves in any process with no
    // registry. The `=` must be top-level: a record's fields use `:`.
    if let Some(eq) = find_top_level(s, '=') {
        let name = s[..eq].trim();
        TypeRegistry::check_declarable_name(name)
            .map_err(|why| format!("`{s}`: `{name}` is not a type name: {why}"))?;
        let body = WeftType::try_parse(s[eq + 1..].trim())?;
        // A named type's whole contract is "one name, one body"
        // (equality and compatibility compare the NAME alone), so
        // this second construction door enforces the same
        // concreteness the registry enforces at declaration: no
        // type variable in the body, ever (no legitimate wire
        // string carries one). Parsing stays a PURE function of
        // the string on purpose: a stored project or journal row
        // must deserialize with no registry (and must keep
        // deserializing after the declaration is edited, so a
        // resumed run gets the exact shape it started on);
        // agreement between same-named restatements is an
        // AUTHORING rule, enforced by the compiler's
        // `named-type-conflict` validation.
        // SYNC: named-body concreteness <-> packages/weft-graph/src/protocol.ts parseSingleType (Name=Body arm)
        if body.contains_unresolved_leaf() {
            return Err(format!("`{name}` is a declared type, so its body cannot hold a type variable"));
        }
        return Ok(WeftType::Named { name: name.to_string(), body: Box::new(body) });
    }

    // Record: `{ field: Type, field?: Type }`, closed, or with `..` as
    // its last item, open. At least one field (an empty `{}` is not a
    // type, nor is `{ .. }`: `JsonDict` is "some object").
    if let Some(inner) = s.strip_prefix('{').and_then(|r| r.strip_suffix('}')) {
        if inner.trim().is_empty() {
            return Err(format!("`{s}` declares no field; `JsonDict` is the type for any object"));
        }
        let mut fields: Vec<RecordField> = Vec::new();
        let mut open = false;
        let parts = split_top_level(inner, ',');
        let last = parts.len() - 1;
        for (i, part) in parts.into_iter().enumerate() {
            let part = part.trim();
            if part == ".." {
                if i != last {
                    return Err(format!("`{s}`: `..` opens a record, so it comes last"));
                }
                open = true;
                continue;
            }
            if part.is_empty() {
                return Err(format!("`{s}`: an empty field"));
            }
            let colon = find_top_level(part, ':')
                .ok_or_else(|| format!("`{s}`: the field `{part}` has no type (`name: Type`)"))?;
            let mut name = part[..colon].trim();
            let optional = name.ends_with('?');
            if optional {
                name = name[..name.len() - 1].trim_end();
            }
            if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(format!("`{s}`: `{name}` is not a field name"));
            }
            if fields.iter().any(|f| f.name == name) {
                return Err(format!("`{s}`: the field `{name}` is declared twice"));
            }
            let ty = WeftType::try_parse(part[colon + 1..].trim())?;
            fields.push(RecordField { name: name.to_string(), ty, optional });
        }
        if fields.is_empty() {
            return Err(format!("`{s}` declares no field; `JsonDict` is the type for any object"));
        }
        return Ok(WeftType::Record { fields, open });
    }

    match s {
        "JsonDict" => return Ok(WeftType::JsonDict),
        "Json" => return Ok(WeftType::Json),
        "Bus" => return Ok(WeftType::Bus),
        "Access" => return Ok(WeftType::Access),
        "Infra" => return Ok(WeftType::Infra),
        _ => {}
    }

    // Parameterized: List[T], Generator[T], Secret[T], Dict[String, V]
    if let Some(bracket_pos) = s.find('[') {
        if !s.ends_with(']') {
            return Err(format!("`{s}` opens a `[` it never closes at its end"));
        }
        let name = s[..bracket_pos].trim();
        let inner = &s[bracket_pos + 1..s.len() - 1];

        return match name {
            "List" => Ok(WeftType::List(Box::new(WeftType::try_parse(inner)?))),
            "Generator" => Ok(WeftType::Generator(Box::new(WeftType::try_parse(inner)?))),
            "Secret" => Ok(WeftType::Secret(Box::new(WeftType::try_parse(inner)?))),
            "Dict" => {
                let parts = split_top_level(inner, ',');
                let [key, value] = parts.as_slice() else {
                    return Err(format!("`{s}`: a dict takes two types, `Dict[String, V]`"));
                };
                let key = WeftType::try_parse(key.trim())?;
                let value = WeftType::try_parse(value.trim())?;
                // A JSON object's keys are text, so no value has any
                // other key type: `Dict[Number, String]` would compile
                // and then match nothing.
                // SYNC: dict-key rule <-> packages/weft-graph/src/protocol.ts parseSingleType (Dict arm)
                if key != WeftType::Primitive(WeftPrimitive::String) {
                    return Err(format!(
                        "`{s}`: a dict's keys are always text, so its key type is `String`. \
                         To key by a {key}, use `List[{{key: {key}, value: {value}}}]`"
                    ));
                }
                Ok(WeftType::Dict(Box::new(value)))
            }
            _ => Err(format!("`{name}[...]` is not a type; the types that take one are List, Generator, Secret and Dict")),
        };
    }

    // Try primitive first
    if let Some(p) = WeftPrimitive::from_name(s) {
        return Ok(WeftType::Primitive(p));
    }
    // Type variable: T, T1, T2, ... (starts with uppercase T, optionally followed by digits)
    if is_type_var_name(s) {
        return Ok(WeftType::TypeVar(s.to_string()));
    }
    // Declared names, through the one registry: builtin aliases
    // (Media, File: display sugar, expand structurally) and
    // user-declared nominal types (resolve to `Named`). See
    // `TypeRegistry` for how a registry becomes visible here.
    if let Some(resolved) = TypeRegistry::current().lookup(s) {
        return Ok(resolved);
    }
    Err(format!("`{s}` is not a type"))
}

/// Type variable names users can write: T, T1, T2, ..., T99.
///
/// Also accepted (catalog-internal only, not user-facing):
///   - `T_Auto`: sentinel emitted by catalog helpers like `PortTemplate::any`
///     to request a per-port-instance TypeVar. Replaced with `T__in__{key}`
///     at enrichment time.
///   - `T__scope` (e.g. `T__in__hook`): materialized form of a `T_Auto` marker,
///     scoped to a specific port instance. Must round-trip through the parser
///     because the frontend representation of port types is a string.
///
/// The internal forms exist so catalog authors can express "this port accepts
/// anything, independently from sibling ports" without forcing the same rule
/// on nodes that genuinely want shared `T` semantics (FirstInOrder, future
/// Zip, etc.).
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

/// Whether the bracket opening `s` is the one its last character closes
/// (`(A | B)` yes, `(A) | (B)`, `(A)(B)` and a mismatched `(A]` no).
// SYNC: opening_bracket_closes_last <-> packages/weft-graph/src/protocol.ts openingBracketClosesLast
fn opening_bracket_closes_last(s: &str) -> bool {
    let mut open: Vec<char> = Vec::new();
    for (i, c) in s.char_indices() {
        match c {
            '[' | '{' | '(' => open.push(c),
            ']' | '}' | ')' => {
                let expected = match c {
                    ']' => '[',
                    '}' => '{',
                    _ => '(',
                };
                if open.pop() != Some(expected) {
                    return false;
                }
                if open.is_empty() {
                    return i + c.len_utf8() == s.len();
                }
            }
            _ => {}
        }
    }
    false
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

impl WeftType {
    /// Render this type. `wire` decides how a `Named` prints: false is
    /// the AUTHORED form (the bare name, what humans read and write in
    /// source and metadata), true is the WIRE form (`Name=Body`, fully
    /// self-contained, what serialization emits so a stored type string
    /// resolves in any process with no registry).
    // SYNC: fmt_with <-> packages/weft-graph/src/protocol.ts weftTypeToString, weftTypeToWireString
    fn fmt_with(&self, out: &mut std::string::String, wire: bool) {
        use std::fmt::Write;
        match self {
            WeftType::Primitive(p) => out.push_str(p.as_str()),
            WeftType::List(inner) => {
                out.push_str("List[");
                inner.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Dict(v) => {
                out.push_str("Dict[String, ");
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
            WeftType::Json => out.push_str("Json"),
            WeftType::Bus => out.push_str("Bus"),
            WeftType::Access => out.push_str("Access"),
            WeftType::Infra => out.push_str("Infra"),
            WeftType::Generator(inner) => {
                out.push_str("Generator[");
                inner.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Secret(inner) => {
                out.push_str("Secret[");
                inner.fmt_with(out, wire);
                out.push(']');
            }
            WeftType::Record { fields, open } => {
                out.push('{');
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{}{}: ", field.name, if field.optional { "?" } else { "" });
                    field.ty.fmt_with(out, wire);
                }
                if *open {
                    out.push_str(", ..");
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
        }
    }

    /// The self-contained serialized form: like `to_string`, except a
    /// `Named` carries its body inline (`ChatHistory=List[...]`), so
    /// parsing the result never needs a registry. This is what
    /// `Serialize` emits (stored projects, journals, editor payloads);
    /// `Display` stays the authored bare-name form.
    // SYNC: wire_string <-> packages/weft-graph/src/protocol.ts weftTypeToWireString
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
        WeftType::try_parse(&s).map_err(|why| serde::de::Error::custom(format!("invalid port type: {s} ({why})")))
    }
}

#[cfg(test)]
#[path = "tests/weft_type_tests.rs"]
mod tests;

#[cfg(test)]
mod nested_shape_tests {
    use super::WeftType;

    /// A record shape can sit anywhere a type can, at any depth: inside
    /// a list, inside a dict's value, inside another record, inside a
    /// list of lists. So "a list of things that each have an id" is
    /// written with the id still in it, and none of the shape is lost
    /// to a bare `List[JsonDict]`.
    #[test]
    fn a_record_shape_nests_anywhere_a_type_goes() {
        let round_trips = |written: &str, expected: &str| {
            let ty = WeftType::parse(written)
                .unwrap_or_else(|| panic!("`{written}` should parse"));
            assert_eq!(ty.to_string(), expected, "written as `{written}`");
        };
        round_trips("List[{ id: String }]", "List[{id: String}]");
        round_trips(
            "List[{ id: String, tags: List[String] }]",
            "List[{id: String, tags: List[String]}]",
        );
        round_trips(
            "{ items: List[{ id: String, meta: { n: Number } }] }",
            "{items: List[{id: String, meta: {n: Number}}]}",
        );
        round_trips("Dict[String, List[{ id: String }]]", "Dict[String, List[{id: String}]]");
        round_trips("List[List[{ id: String }]]", "List[List[{id: String}]]");
        // And the optional marker survives the nesting.
        round_trips("List[{ id: String, note?: String }]", "List[{id: String, note?: String}]");
    }
}



