//! Where a run's record keeps things: an event's place in it
//! ([`Position`]), and where a value an event names was written down
//! ([`ValueRef`]). A value travels once: it is in the record where it was
//! written, and everything that shows it later (a firing's input, its
//! output, a wait's answer) names it by reference, which a reader resolves
//! when it shows the value.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::Located;
use crate::RunId;

/// An event's place in its run's record, in the order the events were
/// written: the row it was written in (one write is one row) and its index
/// inside it. A read of a run's record resumes after a row, never inside
/// one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position {
    pub seq: i32,
    pub index: u32,
}

/// Where a value an event names is kept, and how to read it back. The
/// one copy of a value a node sent is its `PortEmitted` row; what a wire
/// delivered of it, a loop's item of it, a loop's gathered list of such
/// values, all point back at the rows that hold them.
// SYNC: ValueRef <-> packages/weft-graph/src/protocol.ts ValueRef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum ValueRef {
    /// The value one record row carries: a port's emission
    /// (`PortEmitted`), or a wait's answer (`SuspensionResolved`).
    Row { run: RunId, seq: i32, index: u32 },
    /// A value the run was given at its birth, which its first row holds:
    /// handed to a port (`side: input`), or sent by a node standing in for
    /// itself (`side: output`).
    Given { run: RunId, place: Located, port: String, side: GivenSide },
    /// A part of another value: the keys and list items read off it, in
    /// order (a wire's key path, a loop's item).
    Part { of: Box<ValueRef>, path: Vec<Step> },
    /// A list whose items are other values: what a loop gathered, one item
    /// per iteration.
    List { items: Vec<ValueRef> },
    /// A value kept in no row, written out: a literal the program holds, a
    /// loop's index, the null a refused value becomes. Small by
    /// construction, except where a value's origin is not tracked (a
    /// reader that folded without positions), which writes it out whole.
    Written { value: Arc<Value> },
}

/// Which side of a node a value given at a run's birth lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GivenSide {
    Input,
    Output,
}

/// One step into a value: a key of an object, or an item of a list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Step {
    Key(String),
    Item(u64),
}

/// Why a reference did not resolve.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unresolved {
    /// The record no longer holds what it points at: the run was erased
    /// (its keep time ran out, `weft clean`).
    #[error("the record of run {0} no longer holds this value")]
    Gone(RunId),
    /// The record holds the row, and the value is not where the
    /// reference says: a record and a reference that disagree.
    #[error("{0}")]
    Broken(String),
}

impl ValueRef {
    /// The reference to `self` read along `path`; `self` when `path` is
    /// empty. A part of a part is one part.
    pub fn part(self, path: Vec<Step>) -> Self {
        if path.is_empty() {
            return self;
        }
        match self {
            Self::Part { of, path: mut first } => {
                first.extend(path);
                Self::Part { of, path: first }
            }
            Self::Written { value } => match read_path(&value, &path) {
                Ok(part) => Self::Written { value: Arc::new(part.clone()) },
                Err(_) => Self::Part { of: Box::new(Self::Written { value }), path },
            },
            whole => Self::Part { of: Box::new(whole), path },
        }
    }

    /// The runs whose records this reference reads.
    pub fn runs(&self, into: &mut Vec<RunId>) {
        match self {
            Self::Row { run, .. } | Self::Given { run, .. } => into.push(*run),
            Self::Part { of, .. } => of.runs(into),
            Self::List { items } => items.iter().for_each(|item| item.runs(into)),
            Self::Written { .. } => {}
        }
    }

    /// Whether this reference nests more than `limit` references deep
    /// (a row or a written value is one deep; a part or a list is one
    /// deeper than what it holds). The walk stops at `limit`, so a
    /// reference of any depth is measured on a stack no deeper than that.
    pub fn deeper_than(&self, limit: usize) -> bool {
        let Some(below) = limit.checked_sub(1) else { return true };
        match self {
            Self::Row { .. } | Self::Given { .. } | Self::Written { .. } => false,
            Self::Part { of, .. } => of.deeper_than(below),
            Self::List { items } => items.iter().any(|item| item.deeper_than(below)),
        }
    }

    /// The value this reference names. `kept` answers what a row or a
    /// birth holds (a [`ValueRef::Row`] or [`ValueRef::Given`]), `None`
    /// when the record no longer holds it.
    pub fn resolve(&self, kept: &mut impl FnMut(&ValueRef) -> Result<Option<Arc<Value>>, Unresolved>) -> Result<Arc<Value>, Unresolved> {
        match self {
            Self::Row { run, .. } | Self::Given { run, .. } => kept(self)?.ok_or(Unresolved::Gone(*run)),
            Self::Part { of, path } => {
                let whole = of.resolve(kept)?;
                read_path(&whole, path).map(|part| Arc::new(part.clone())).map_err(Unresolved::Broken)
            }
            Self::List { items } => {
                let mut list = Vec::with_capacity(items.len());
                for item in items {
                    list.push(item.resolve(kept)?.as_ref().clone());
                }
                Ok(Arc::new(Value::Array(list)))
            }
            Self::Written { value } => Ok(value.clone()),
        }
    }
}

/// `value` read along `path`.
fn read_path<'v>(value: &'v Value, path: &[Step]) -> Result<&'v Value, String> {
    let mut at = value;
    for step in path {
        at = match step {
            Step::Key(key) => at.get(key.as_str()).ok_or_else(|| format!("the value has no key '{key}'"))?,
            Step::Item(index) => at.get(*index as usize).ok_or_else(|| format!("the value has no item {index}"))?,
        };
    }
    Ok(at)
}

/// A value an event names, as the reader that made the event holds it:
/// where the record keeps it, and the value itself while it is at hand
/// (a fold has every value it shows; an event read off another process's
/// announcement carries only the reference). An event goes out either as
/// references (the editor fetches a value when it shows it) or with the
/// values written in (`weft events`, a program reading a run).
#[derive(Debug, Clone, PartialEq)]
pub struct Shown {
    pub at: ValueRef,
    pub value: Option<Arc<Value>>,
}

impl Shown {
    /// `value`, kept at `origin` when the fold knew it, written out
    /// otherwise.
    pub fn of(origin: Option<&Arc<ValueRef>>, value: &Arc<Value>) -> Self {
        let at = match origin {
            Some(origin) => origin.as_ref().clone(),
            None => ValueRef::Written { value: value.clone() },
        };
        Self { at, value: Some(value.clone()) }
    }

    /// A value only the reference to is at hand.
    pub fn at(at: ValueRef) -> Self {
        let value = match &at {
            ValueRef::Written { value } => Some(value.clone()),
            _ => None,
        };
        Self { at, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(seq: i32) -> ValueRef {
        ValueRef::Row { run: RunId::from_u128(1), seq, index: 0 }
    }

    /// Every shape crosses the wire and comes back the same, tagged by
    /// where it reads from, in the casing the editor reads.
    #[test]
    fn a_reference_round_trips_on_the_wire() {
        let reference = ValueRef::List {
            items: vec![
                row(3).part(vec![Step::Key("profile".into()), Step::Item(2)]),
                ValueRef::Given { run: RunId::from_u128(2), place: Located::top("src"), port: "out".into(), side: GivenSide::Output },
                ValueRef::Written { value: Arc::new(json!(7)) },
            ],
        };
        let wire = serde_json::to_value(&reference).unwrap();
        assert_eq!(wire["from"], "list");
        assert_eq!(wire["items"][0], json!({ "from": "part", "of": { "from": "row", "run": RunId::from_u128(1), "seq": 3, "index": 0 }, "path": ["profile", 2] }));
        assert_eq!(wire["items"][1]["side"], "output");
        assert_eq!(wire["items"][2], json!({ "from": "written", "value": 7 }));
        assert_eq!(serde_json::from_value::<ValueRef>(wire).unwrap(), reference);
    }

    /// A reference resolves through what the record keeps, reading parts
    /// and assembling lists; a row the record no longer holds says so.
    #[test]
    fn a_reference_resolves_through_the_record() {
        let mut kept = |at: &ValueRef| -> Result<Option<Arc<Value>>, Unresolved> {
            Ok(match at {
                ValueRef::Row { seq: 1, .. } => Some(Arc::new(json!({ "profile": { "wpm": [10, 42] } }))),
                _ => None,
            })
        };
        let part = row(1).part(vec![Step::Key("profile".into())]).part(vec![Step::Key("wpm".into()), Step::Item(1)]);
        assert!(matches!(&part, ValueRef::Part { path, .. } if path.len() == 3), "a part of a part is one part");
        assert_eq!(*part.resolve(&mut kept).unwrap(), json!(42));
        let list = ValueRef::List { items: vec![part.clone(), ValueRef::Written { value: Arc::new(Value::Null) }] };
        assert_eq!(*list.resolve(&mut kept).unwrap(), json!([42, null]));
        assert_eq!(row(2).resolve(&mut kept).unwrap_err(), Unresolved::Gone(RunId::from_u128(1)));
        assert!(matches!(row(1).part(vec![Step::Key("nope".into())]).resolve(&mut kept), Err(Unresolved::Broken(_))));
    }

    /// A reference's depth counts each part and list it goes through.
    #[test]
    fn a_reference_is_as_deep_as_its_nesting() {
        assert!(!row(1).deeper_than(1));
        assert!(row(1).deeper_than(0));
        let nested = ValueRef::List { items: vec![row(1), ValueRef::List { items: vec![row(1).part(vec![Step::Item(0)])] }] };
        assert!(!nested.deeper_than(4));
        assert!(nested.deeper_than(3));
    }
}
