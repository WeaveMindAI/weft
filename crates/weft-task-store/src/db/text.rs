//! The rules for what a statement hands the database, applied where every
//! statement passes ([`super::Client`]) or where a value is bound
//! ([`Json`]), so no write can forget them.
//!
//! Postgres refuses the NUL character (U+0000) in text: a `TEXT` value
//! holding one fails the whole statement (`invalid byte sequence for
//! encoding "UTF8": 0x00`), and so does the escape `\u0000` inside
//! `JSONB` (`unsupported Unicode escape sequence`). Text from outside weft
//! carries it (a scraped page, a PDF, a node's error message), so:
//!
//! - text a person reads (an error, a reason, the words a search
//!   matches) is written without it: every text value a statement binds
//!   loses its NUL characters on the way out ([`NulFree`]). Losing an
//!   invisible character loses nothing anybody reads; refusing the write
//!   loses the write.
//! - a value that must come back exactly (a payload, an answer, a run's
//!   spec, a signal's state) is never kept in `JSONB`: it is kept as its
//!   JSON text in a `TEXT` column ([`JsonText`]), where JSON writes NUL as
//!   the six characters `\u0000`, which `TEXT` keeps and which read back as
//!   the character they stand for.
//!
//! `JSONB` is left to values that can hold no NUL: [`Json`] binds only a
//! type that says so by implementing [`JsonbSafe`], so a value a node or a
//! provider made cannot reach a `JSONB` column by accident.
//! `serde_json::Value` never does: it can hold anything.

use std::error::Error as StdError;

use bytes::{Buf as _, BufMut as _, BytesMut};
use tokio_postgres::types::{to_sql_checked, Format, FromSql, IsNull, Kind, ToSql, Type};

use super::Param;

type BoxError = Box<dyn StdError + Sync + Send>;

/// A statement's parameter as it goes to the database: the value it wraps,
/// minus every NUL character when the parameter is text (or an array of
/// text). Every other parameter goes as the value wrote itself.
#[derive(Debug)]
pub(crate) struct NulFree<'a>(pub(crate) Param<'a>);

/// Every parameter of a statement, as [`NulFree`] sends them.
pub(crate) fn nul_free<'a>(params: &'a [Param<'a>]) -> impl ExactSizeIterator<Item = NulFree<'a>> {
    params.iter().map(|param| NulFree(*param))
}

impl ToSql for NulFree<'_> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        let start = out.len();
        let null = self.0.to_sql_checked(ty, out)?;
        if matches!(null, IsNull::No) && (is_text(ty) || is_text_array(ty)) && out[start..].contains(&0) {
            let format = self.0.encode_format(ty);
            let written = out.split_off(start);
            out.unsplit(without_nul(ty, format, written)?);
        }
        Ok(null)
    }

    /// The wrapped value says what it accepts, in `to_sql`.
    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, ty: &Type) -> Format {
        self.0.encode_format(ty)
    }

    to_sql_checked!();
}

/// Whether `ty` is text: a value of it written in either format is its
/// UTF-8 bytes, nothing around them.
fn is_text(ty: &Type) -> bool {
    matches!(*ty, Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN)
}

fn is_text_array(ty: &Type) -> bool {
    matches!(ty.kind(), Kind::Array(element) if is_text(element))
}

/// `written`, the value `ty` (text, or an array of text) was written as in
/// `format`, without its NUL characters. UTF-8 never uses the byte 0
/// inside another character, so a 0 in text is a NUL character and
/// nothing else.
fn without_nul(ty: &Type, format: Format, written: BytesMut) -> Result<BytesMut, BoxError> {
    match format {
        // Text format is text, whatever the type: an array is `{a,b}`.
        Format::Text => Ok(dropped(&written)),
        Format::Binary if is_text(ty) => Ok(dropped(&written)),
        Format::Binary => array_without_nul(written),
    }
}

fn dropped(bytes: &[u8]) -> BytesMut {
    let mut kept = BytesMut::with_capacity(bytes.len());
    for chunk in bytes.split(|b| *b == 0) {
        kept.extend_from_slice(chunk);
    }
    kept
}

/// An array in Postgres's binary format with each element's NUL
/// characters removed: its dimension count, a has-nulls flag and the
/// element type (an `i32`, an `i32`, a `u32`), each dimension's length and
/// lower bound (two `i32`), then each element as its length (an `i32`,
/// `-1` for NULL) and its bytes. Only the elements and their lengths
/// change; an array none of whose elements holds one comes back as it was.
fn array_without_nul(written: BytesMut) -> Result<BytesMut, BoxError> {
    let array = &written[..];
    let mut read = array;
    let short = || -> BoxError { "a text array is shorter than its header says".into() };
    if read.len() < 12 {
        return Err(short());
    }
    let dimensions = read.get_i32();
    let _has_nulls = read.get_i32();
    let _element_type = read.get_u32();
    let dimensions = usize::try_from(dimensions).map_err(|_| -> BoxError { "a text array has a negative dimension count".into() })?;
    if read.len() < dimensions * 8 {
        return Err(short());
    }
    let mut elements: usize = 1;
    for at in 0..dimensions {
        let len = i32::from_be_bytes(read[at * 8..at * 8 + 4].try_into().expect("four bytes"));
        elements *= usize::try_from(len).map_err(|_| -> BoxError { "a text array has a negative length".into() })?;
    }
    if dimensions == 0 {
        elements = 0;
    }
    let header = 12 + dimensions * 8;
    read.advance(dimensions * 8);
    let mut out = BytesMut::with_capacity(array.len());
    out.extend_from_slice(&array[..header]);
    let mut changed = false;
    for _ in 0..elements {
        if read.len() < 4 {
            return Err(short());
        }
        let len = read.get_i32();
        if len < 0 {
            out.put_i32(len);
            continue;
        }
        let len = len as usize;
        if read.len() < len {
            return Err(short());
        }
        let kept = dropped(&read[..len]);
        changed |= kept.len() != len;
        out.put_i32(i32::try_from(kept.len()).expect("no longer than it was"));
        out.extend_from_slice(&kept);
        read.advance(len);
    }
    Ok(if changed { out } else { written })
}

/// A type whose JSON can hold no NUL character, so it may be bound into
/// `JSONB` ([`Json`]), where one in any string refuses the whole write:
/// every string in it is one weft wrote (a hash, an id, a name weft
/// checked, a path in a project), never a value a node, a provider or a
/// person handed over ([`WeftMade`] says so of a plain map or list), or it
/// drops NUL as it writes ([`Names`]). Anything else is kept as
/// [`JsonText`].
///
/// `serde_json::Value` is not one, nor is anything holding it:
///
/// ```compile_fail
/// let value = serde_json::json!({ "page": "a\u{0}b" });
/// let _: &dyn weft_task_store::db::ToSql = &weft_task_store::db::Json(value);
/// ```
pub trait JsonbSafe {}

impl<T: JsonbSafe + ?Sized> JsonbSafe for &T {}
impl<T: JsonbSafe> JsonbSafe for Option<T> {}
impl<T: JsonbSafe> JsonbSafe for Vec<T> {}
impl<T: JsonbSafe> JsonbSafe for [T] {}
impl<T: JsonbSafe> JsonbSafe for std::collections::BTreeSet<T> {}
impl<K: JsonbSafe, V: JsonbSafe> JsonbSafe for std::collections::BTreeMap<K, V> {}
impl JsonbSafe for bool {}
impl JsonbSafe for i32 {}
impl JsonbSafe for i64 {}
impl JsonbSafe for u32 {}
impl JsonbSafe for u64 {}
impl JsonbSafe for uuid::Uuid {}
/// Hashes, and the names of the node types they are of.
impl JsonbSafe for weft_core::project::hash::ProgramIdentity {}

/// Names a person reads (scopes, the names of stored values, media types)
/// kept as a `JSONB` list: each loses its NUL characters on the way, as
/// every text parameter does ([`NulFree`]).
#[derive(Debug, Clone, Copy)]
pub struct Names<'a, S>(pub &'a [S]);

impl<S: AsRef<str>> serde::Serialize for Names<'_, S> {
    fn serialize<Z: serde::Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        serializer.collect_seq(self.0.iter().map(|name| name.as_ref().replace('\0', "")))
    }
}

impl<S> JsonbSafe for Names<'_, S> {}

/// A plain map or list of strings (and numbers, ids) every one of which
/// weft made or checked: hashes, ids, tokens weft minted, paths in a
/// project, names of nodes and node types. Wrapping one says so where it
/// is bound into `JSONB`. Nothing holding a `serde_json::Value` is plain.
#[derive(Debug, Clone, Copy)]
pub struct WeftMade<'a, T: ?Sized>(pub &'a T);

impl<T: serde::Serialize + ?Sized> serde::Serialize for WeftMade<'_, T> {
    fn serialize<Z: serde::Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        self.0.serialize(serializer)
    }
}

impl<T: Plain + ?Sized> JsonbSafe for WeftMade<'_, T> {}

/// What [`WeftMade`] wraps: strings, numbers and ids, in maps and lists.
pub trait Plain {}

impl Plain for String {}
impl Plain for str {}
impl Plain for bool {}
impl Plain for i32 {}
impl Plain for i64 {}
impl Plain for u32 {}
impl Plain for u64 {}
impl Plain for uuid::Uuid {}
impl<T: Plain + ?Sized> Plain for &T {}
impl<T: Plain> Plain for Option<T> {}
impl<T: Plain> Plain for Vec<T> {}
impl<T: Plain> Plain for [T] {}
impl<T: Plain> Plain for std::collections::BTreeSet<T> {}
impl<K: Plain, V: Plain> Plain for std::collections::BTreeMap<K, V> {}

/// A value kept in a `JSON` or `JSONB` column: written with `serde_json`
/// when it can hold no NUL ([`JsonbSafe`]), read back with it whatever it
/// is (a read refuses nothing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Json<T>(pub T);

impl<T: serde::Serialize + JsonbSafe + std::fmt::Debug> ToSql for Json<T> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        if *ty == Type::JSONB {
            // The version of JSONB's binary format, the one there is.
            out.put_u8(1);
        }
        serde_json::to_writer(out.writer(), &self.0)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB)
    }

    to_sql_checked!();
}

impl<'a, T: serde::Deserialize<'a>> FromSql<'a> for Json<T> {
    fn from_sql(ty: &Type, mut raw: &'a [u8]) -> Result<Self, BoxError> {
        if *ty == Type::JSONB {
            match raw.split_first() {
                Some((1, rest)) => raw = rest,
                _ => return Err("a JSONB value in a binary format other than version 1".into()),
            }
        }
        Ok(Json(serde_json::from_slice(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB)
    }
}

/// A value kept as its JSON text in a `TEXT` column, for a value that must
/// read back exactly as it was written (see the module doc): written with
/// `serde_json`, read back with it. Like [`Json`], which keeps it in
/// `JSONB` instead, where a NUL character in any of its strings refuses the
/// write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonText<T>(pub T);

impl<T: serde::Serialize + std::fmt::Debug> ToSql for JsonText<T> {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        serde_json::to_writer(out.writer(), &self.0)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TEXT | Type::VARCHAR)
    }

    to_sql_checked!();
}

impl<'a, T: serde::Deserialize<'a>> FromSql<'a> for JsonText<T> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        Ok(JsonText(serde_json::from_slice(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TEXT | Type::VARCHAR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sent(param: Param<'_>, ty: &Type) -> Vec<u8> {
        let mut out = BytesMut::from(&b"before"[..]);
        let null = NulFree(param).to_sql_checked(ty, &mut out).unwrap();
        assert!(matches!(null, IsNull::No));
        assert_eq!(&out[..6], b"before", "what was written before the parameter is left alone");
        out[6..].to_vec()
    }

    /// Text loses its NUL characters and nothing else.
    #[test]
    fn text_is_sent_without_nul() {
        assert_eq!(sent(&"a\0b\0\0c", &Type::TEXT), b"abc");
        assert_eq!(sent(&String::from("caf\u{e9}\0!"), &Type::VARCHAR), "caf\u{e9}!".as_bytes());
        assert_eq!(sent(&"plain", &Type::TEXT), b"plain");
        assert_eq!(sent(&"\0", &Type::TEXT), b"");
    }

    /// An array of text loses the NUL characters of each element, and
    /// keeps its NULLs, its dimensions and its other elements.
    #[test]
    fn a_text_array_is_sent_without_nul() {
        let values: Vec<Option<&str>> = vec![Some("a\0b"), None, Some(""), Some("\0\0"), Some("clean")];
        let clean: Vec<Option<&str>> = vec![Some("ab"), None, Some(""), Some(""), Some("clean")];
        let mut expected = BytesMut::new();
        clean.to_sql_checked(&Type::TEXT_ARRAY, &mut expected).unwrap();
        assert_eq!(sent(&values, &Type::TEXT_ARRAY), expected.to_vec());
        let empty: Vec<&str> = Vec::new();
        let mut expected = BytesMut::new();
        empty.to_sql_checked(&Type::TEXT_ARRAY, &mut expected).unwrap();
        assert_eq!(sent(&empty, &Type::TEXT_ARRAY), expected.to_vec());
    }

    /// Anything else goes exactly as it wrote itself, a 0 byte in it
    /// included.
    #[test]
    fn other_values_go_as_they_are() {
        let mut expected = BytesMut::new();
        0i64.to_sql_checked(&Type::INT8, &mut expected).unwrap();
        assert_eq!(sent(&0i64, &Type::INT8), expected.to_vec());
        let bytes: Vec<u8> = vec![0, 1, 0];
        assert_eq!(sent(&bytes, &Type::BYTEA), vec![0, 1, 0]);
        let ints: Vec<i32> = vec![0, 1];
        let mut expected = BytesMut::new();
        ints.to_sql_checked(&Type::INT4_ARRAY, &mut expected).unwrap();
        assert_eq!(sent(&ints, &Type::INT4_ARRAY), expected.to_vec());
    }

    /// A NULL stays NULL, and a value that does not fit its parameter's
    /// type is still refused by the value's own check.
    #[test]
    fn null_and_wrong_types_go_through_the_values_own_rules() {
        let mut out = BytesMut::new();
        let none: Option<&str> = None;
        assert!(matches!(NulFree(&none).to_sql_checked(&Type::TEXT, &mut out).unwrap(), IsNull::Yes));
        assert!(out.is_empty());
        assert!(NulFree(&1i64).to_sql_checked(&Type::TEXT, &mut out).is_err());
    }

    /// A value kept as JSON text reads back exactly, a NUL character in
    /// it included: JSON writes it as `\u0000`, which text keeps.
    #[test]
    fn json_text_reads_back_exactly() {
        let value = serde_json::json!({ "page": "a\0b", "n": [1, 2] });
        let mut out = BytesMut::new();
        JsonText(&value).to_sql_checked(&Type::TEXT, &mut out).unwrap();
        assert!(!out.contains(&0), "the text holds no NUL byte");
        assert!(std::str::from_utf8(&out).unwrap().contains("\\u0000"));
        // Sent as any statement's parameter is, it is left as it is.
        assert_eq!(sent(&JsonText(&value), &Type::TEXT), out.to_vec());
        let back: JsonText<serde_json::Value> = JsonText::from_sql(&Type::TEXT, &out).unwrap();
        assert_eq!(back.0, value);
        assert!(!<JsonText<serde_json::Value> as ToSql>::accepts(&Type::JSONB), "JSONB is what it replaces");
    }

    /// A value safe in JSONB goes in JSONB's binary format (its version
    /// byte, then the JSON) and reads back from it.
    #[test]
    fn json_reads_back_from_jsonb() {
        let tokens = std::collections::BTreeMap::from([("t1".to_string(), 2u32)]);
        let mut out = BytesMut::new();
        Json(WeftMade(&tokens)).to_sql_checked(&Type::JSONB, &mut out).unwrap();
        assert_eq!(out[0], 1);
        let back: Json<std::collections::BTreeMap<String, u32>> = Json::from_sql(&Type::JSONB, &out).unwrap();
        assert_eq!(back.0, tokens);
        assert!(Json::<serde_json::Value>::from_sql(&Type::JSONB, b"\x02{}").is_err(), "another binary format is refused");
    }

    /// Names lose their NUL characters on the way into JSONB.
    #[test]
    fn names_go_without_nul() {
        let names = ["chat:\0write".to_string(), "read".to_string()];
        let mut out = BytesMut::new();
        Json(Names(&names)).to_sql_checked(&Type::JSONB, &mut out).unwrap();
        assert_eq!(&out[1..], br#"["chat:write","read"]"#);
    }
}
