//! Scheduled fire. The listener spawns a tokio task that enqueues
//! a FireSignal broker task when the timer fires; a dispatcher
//! Pod claims it and runs the same dispatch path a stateless fire
//! takes. `After` and `At` are single-shot; `Cron` recurs until
//! torn down.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timer {
    pub spec: TimerSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimerSpec {
    After {
        duration_ms: u64,
    },
    At {
        when: chrono::DateTime<chrono::Utc>,
    },
    Cron {
        expression: String,
        /// The IANA zone the expression counts in (`Europe/Paris`), so
        /// "nine every weekday" stays nine across a daylight-saving
        /// change. A signal written before the field existed is UTC,
        /// which is what it always counted in.
        #[serde(default = "utc")]
        timezone: String,
    },
}

fn utc() -> String {
    "UTC".to_string()
}

/// The zone behind an IANA name, or what is wrong with the name. One
/// reader for the validator here and the listener that computes the
/// next fire, so the two cannot accept different spellings.
pub fn parse_timezone(name: &str) -> Result<chrono_tz::Tz, String> {
    name.parse::<chrono_tz::Tz>().map_err(|_| {
        format!(
            "'{name}' is not an IANA time zone; write one like `UTC`, `Europe/Paris` or \
             `America/New_York`"
        )
    })
}

/// A zone as a wall clock reads it, for finding a schedule's next
/// moment. On its own, a zone answers "which instant is 02:30 on
/// October 25th in Paris" with *two* (the hour repeats when the clocks
/// go back) or *none* (in spring the hour is skipped), and the cron
/// crate drops a candidate it cannot pin to one instant: a schedule at
/// half past two would silently miss one day a year. A clock has one
/// answer: the FIRST 02:30 in autumn, and in spring the moment the
/// skipped time would have been, read on the offset that held before
/// the change (so 02:30 fires at 03:30 on the new clock), which is
/// what an alarm clock does.
#[derive(Debug, Clone, Copy)]
pub struct WallClock(pub chrono_tz::Tz);

impl chrono::TimeZone for WallClock {
    type Offset = <chrono_tz::Tz as chrono::TimeZone>::Offset;

    fn from_offset(offset: &Self::Offset) -> Self {
        WallClock(chrono_tz::Tz::from_offset(offset))
    }

    fn offset_from_local_date(&self, local: &chrono::NaiveDate) -> chrono::LocalResult<Self::Offset> {
        one_offset(self.0.offset_from_local_date(local), || {
            self.0.offset_from_utc_date(&(*local - chrono::Duration::days(1)))
        })
    }

    fn offset_from_local_datetime(
        &self,
        local: &chrono::NaiveDateTime,
    ) -> chrono::LocalResult<Self::Offset> {
        one_offset(self.0.offset_from_local_datetime(local), || {
            self.0.offset_from_utc_datetime(&(*local - chrono::Duration::days(1)))
        })
    }

    fn offset_from_utc_date(&self, utc: &chrono::NaiveDate) -> Self::Offset {
        self.0.offset_from_utc_date(utc)
    }

    fn offset_from_utc_datetime(&self, utc: &chrono::NaiveDateTime) -> Self::Offset {
        self.0.offset_from_utc_datetime(utc)
    }
}

/// The zone's answer made single: the earlier of two, and for a
/// skipped time the offset that held the day before (every real
/// change is further apart than that).
fn one_offset<O>(
    answer: chrono::LocalResult<O>,
    before_the_change: impl FnOnce() -> O,
) -> chrono::LocalResult<O> {
    match answer {
        chrono::LocalResult::Single(o) | chrono::LocalResult::Ambiguous(o, _) => {
            chrono::LocalResult::Single(o)
        }
        chrono::LocalResult::None => chrono::LocalResult::Single(before_the_change()),
    }
}

impl Signal for Timer {
    const TAG: &'static str = "timer";

    fn validate(&self) -> Result<(), String> {
        match &self.spec {
            TimerSpec::Cron { expression, timezone } => {
                parse_timezone(timezone)?;
                cron::Schedule::from_str(expression).map(|_| ()).map_err(|e| {
                    // The commonest mistake is the five-field (minute-first)
                    // form every other cron takes; name the count and the
                    // field order so the fix is in the message.
                    let fields = expression.split_whitespace().count();
                    format!(
                        "invalid cron expression '{expression}' ({fields} fields): {e}. \
                         A weft cron expression has six fields, seconds first \
                         (`sec min hour day month weekday`), so every five minutes is \
                         `0 */5 * * * *`"
                    )
                })
            }
            TimerSpec::After { duration_ms } => {
                if *duration_ms == 0 {
                    return Err("timer.after.duration_ms must be > 0".into());
                }
                // The listener adds this to the clock and stores the
                // result as milliseconds since the epoch in an i64. A
                // duration that overflows that wraps to a moment in the
                // PAST, and the wait fires at once instead of never, so
                // the impossible wait is refused where it is written.
                let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                if *duration_ms > (i64::MAX as u64) - now_ms {
                    return Err(format!(
                        "timer.after.duration_ms is {duration_ms}, so far ahead that the moment \
                         it names cannot be represented; the longest wait a timer can hold is \
                         about 292 million years"
                    ));
                }
                Ok(())
            }
            TimerSpec::At { when } => {
                if *when <= chrono::Utc::now() {
                    return Err(format!(
                        "the moment {} is already past; a wait has to point ahead of when \
                         the run reaches it",
                        when.to_rfc3339()
                    ));
                }
                Ok(())
            }
        }
    }
}

impl Timer {
    /// The moment the listener actually fired, out of the payload a
    /// timer wake carries. The shape of that payload is the
    /// listener's, so reading it lives here rather than being copied
    /// into every node that parks on a timer.
    pub fn woke_at(wake: &serde_json::Value) -> crate::error::WeftResult<String> {
        wake.get("actualTime")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| crate::error::node_error("the timer fired without an actualTime"))
    }
}

crate::register_signal_kind!(Timer);

#[cfg(test)]
mod tests {
    use super::*;

    /// A duration past what a millisecond timestamp can hold is
    /// refused: it used to wrap into the past in the listener, and a
    /// wait of ten million years fired immediately.
    #[test]
    fn a_duration_that_cannot_be_represented_is_refused() {
        let err = Timer { spec: TimerSpec::After { duration_ms: u64::MAX } }
            .validate()
            .expect_err("an unrepresentable wait must refuse");
        assert!(err.contains("cannot be represented"), "{err}");
        Timer { spec: TimerSpec::After { duration_ms: 5_000 } }.validate().expect("five seconds is fine");
    }

    /// The five-field form every other cron takes is the mistake the
    /// message has to catch: it names the field count and the six-field
    /// order with an example, so the fix is in the refusal.
    #[test]
    fn a_five_field_cron_is_refused_naming_the_shape() {
        let err = Timer { spec: TimerSpec::Cron { expression: "*/5 * * * *".into(), timezone: utc() } }
            .validate()
            .expect_err("five fields must refuse");
        assert!(err.contains("5 fields") && err.contains("six fields") && err.contains("0 */5 * * * *"), "{err}");
        Timer { spec: TimerSpec::Cron { expression: "0 */5 * * * *".into(), timezone: utc() } }
            .validate()
            .expect("six fields, seconds first, is the shape");
    }

    /// The zone is checked where the signal is written, and a signal
    /// stored before the field existed still reads as UTC.
    #[test]
    fn a_cron_zone_is_an_iana_name_and_defaults_to_utc() {
        let err = Timer { spec: TimerSpec::Cron { expression: "0 0 9 * * *".into(), timezone: "Paris".into() } }
            .validate()
            .expect_err("a bare city is not a zone");
        assert!(err.contains("'Paris'") && err.contains("Europe/Paris"), "{err}");
        Timer { spec: TimerSpec::Cron { expression: "0 0 9 * * *".into(), timezone: "Europe/Paris".into() } }
            .validate()
            .expect("an IANA name is fine");
        let old: TimerSpec = serde_json::from_value(serde_json::json!({ "kind": "cron", "expression": "0 0 9 * * *" }))
            .expect("the field is optional on the wire");
        assert!(matches!(old, TimerSpec::Cron { timezone, .. } if timezone == "UTC"));
    }

    /// The day the clocks change, a schedule inside the changed hour
    /// still fires exactly once: the first of the two 02:30s in
    /// autumn, and 03:30 on the new clock in spring, where 02:30 does
    /// not exist. The bare zone would drop both days.
    #[test]
    fn a_schedule_in_the_changed_hour_fires_once_either_way() {
        use chrono::TimeZone;
        let paris = parse_timezone("Europe/Paris").unwrap();
        let schedule = cron::Schedule::from_str("0 30 2 * * *").unwrap();
        // Compared as instants (the listener carries them as UTC):
        // 02:30+02:00 is 00:30Z, and spring's 03:30+02:00 is 01:30Z.
        let fires = |from: &str| -> Vec<String> {
            let from = chrono::DateTime::parse_from_rfc3339(from).unwrap().with_timezone(&WallClock(paris));
            schedule.after(&from).take(3).map(|t| t.with_timezone(&chrono::Utc).to_rfc3339()).collect()
        };
        assert_eq!(
            fires("2026-10-24T12:00:00Z"),
            ["2026-10-25T00:30:00+00:00", "2026-10-26T01:30:00+00:00", "2026-10-27T01:30:00+00:00"]
        );
        assert_eq!(
            fires("2026-03-28T12:00:00Z"),
            ["2026-03-29T01:30:00+00:00", "2026-03-30T00:30:00+00:00", "2026-03-31T00:30:00+00:00"]
        );
        let bare: Vec<String> = schedule
            .after(&chrono::DateTime::parse_from_rfc3339("2026-10-24T12:00:00Z").unwrap().with_timezone(&paris))
            .take(1)
            .map(|t| t.to_rfc3339())
            .collect();
        assert_eq!(bare, ["2026-10-26T02:30:00+01:00"], "the bare zone skips the 25th");
    }
}
