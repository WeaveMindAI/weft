//! Terminal progress for a live node-test run.
//!
//! A live run is long (each test is its own cluster pod) and
//! concurrent, so a silent wait reads as a hang. This renders a
//! stacked, in-place-updating status block on stderr while tests run:
//!
//!   ✓ google/drive_copy · one_real_copy_then_both_deleted  12.4s
//!   ✗ slack/send_message · one_real_message  9.1s
//!   google  ██████████░░░░░░░░░░  5/9  running: drive_move 8s
//!   slack   ████░░░░░░░░░░░░░░░░  2/9  running: react 41s, upload 3s
//!
//! Finished tests print one PERMANENT line each (they scroll away
//! normally); below them one bar per package still in flight, the most
//! recently updated package kept at the bottom where the eye rests.
//! A once-a-second tick keeps the elapsed times moving. Every bar line
//! is truncated to the terminal width so the block's height always
//! equals its line count (a wrapped line would desynchronize the
//! cursor-up erase and leave orphaned fragments).
//!
//! When stderr is not a terminal (a scripted run buffering to a log)
//! the in-place block is meaningless: the permanent lines still print,
//! and the long-wait breadcrumbs (`note`) print instead of the bars,
//! so a buffered log stays legible without ANSI garbage.
//!
//! This runs beside pod drivers spending real provider money, so its
//! contract is: NEVER panic a run over presentation bookkeeping, and
//! never let a late render undo the final cleanup.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

/// One package's in-flight bar state.
struct PackageBar {
    name: String,
    total: usize,
    done: usize,
    failed: usize,
    /// Test name -> start instant, insertion-ordered by the Vec.
    running: Vec<(String, Instant)>,
}

struct State {
    /// Bars in render order; an updated package moves to the end
    /// (the bottom of the block). A fully finished package leaves the
    /// stack (its permanent summary line already printed).
    bars: Vec<PackageBar>,
    /// Height of the block currently drawn on screen, so the next
    /// render knows how far to move back up.
    drawn_lines: usize,
    /// Set by `clear`: the block is gone and stays gone. A ticker task
    /// blocked on the lock when the run ends cannot be cancelled there
    /// (a std mutex is not an await point), so without this latch it
    /// would repaint the block right after `clear` erased it, under
    /// the end-of-run summary.
    done: bool,
}

/// Shared progress sink for one live run. Concurrent pod drivers call
/// `started` / `finished` / `note`; every call re-renders.
pub struct LiveProgress {
    tty: bool,
    state: Mutex<State>,
}

impl LiveProgress {
    /// Build from the full selection: `(package, test)` per live run,
    /// so every package's total is known before anything starts.
    pub fn new(runs: &[(String, String)]) -> Self {
        Self::with_tty(runs, std::io::stderr().is_terminal())
    }

    /// The testable constructor: rendering mode as plain input.
    fn with_tty(runs: &[(String, String)], tty: bool) -> Self {
        let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
        for (package, _) in runs {
            *totals.entry(package.as_str()).or_default() += 1;
        }
        let bars = totals
            .into_iter()
            .map(|(name, total)| PackageBar {
                name: name.to_string(),
                total,
                done: 0,
                failed: 0,
                running: Vec::new(),
            })
            .collect();
        Self {
            tty,
            state: Mutex::new(State { bars, drawn_lines: 0, done: false }),
        }
    }

    /// The one way in to the state. A poisoned lock (a panic elsewhere
    /// while holding it) is RECOVERED, not propagated: the state is
    /// plain counters with no invariant a torn update can break, and a
    /// progress panic here would unwind the pod-driving task and skip
    /// the run's credential cleanup, which is the wrong failure to
    /// trade for a cosmetic glitch.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn started(&self, package: &str, test: &str) {
        let mut s = self.lock();
        let bar = touch(&mut s.bars, package);
        bar.running.push((test.to_string(), Instant::now()));
        self.render(&mut s, &[]);
    }

    /// Record a test's outcome; prints its permanent line. `detail` is
    /// the short failure reason shown inline (the full error lands in
    /// the end-of-run summary).
    pub fn finished(&self, package: &str, test: &str, passed: bool, detail: Option<&str>) {
        let mut s = self.lock();
        let bar = touch(&mut s.bars, package);
        let elapsed = bar
            .running
            .iter()
            .position(|(name, _)| name == test)
            .map(|i| bar.running.remove(i).1.elapsed());
        bar.done += 1;
        if !passed {
            bar.failed += 1;
        }
        let elapsed = match elapsed {
            Some(d) => format!("  {:.1}s", d.as_secs_f64()),
            // A run that broke before its pod started has no start
            // mark; the line still records the outcome.
            None => String::new(),
        };
        let mark = if passed { "✓" } else { "✗" };
        let mut line = format!("{mark} {package}/{test}{elapsed}");
        if let (false, Some(d)) = (passed, detail) {
            line.push_str(&format!("  ({})", first_line(d)));
        }
        let mut permanent = vec![line];
        // A finished package leaves the stack with a summary line, so
        // long multi-package runs keep the block short.
        if let Some(i) = s.bars.iter().position(|b| b.name == package) {
            if s.bars[i].done >= s.bars[i].total {
                let b = s.bars.remove(i);
                permanent.push(match b.failed {
                    0 => format!("── {}: all {} live tests passed", b.name, b.total),
                    n => format!("── {}: {n} of {} live tests FAILED", b.name, b.total),
                });
            }
        }
        self.render(&mut s, &permanent);
    }

    /// A long-wait breadcrumb. On a terminal the bars already show
    /// elapsed time, so this only prints where the bars don't render.
    pub fn note(&self, line: &str) {
        if self.tty {
            return;
        }
        eprintln!("{line}");
    }

    /// Re-render elapsed times; driven by the caller's 1s ticker.
    pub fn tick(&self) {
        let mut s = self.lock();
        self.render(&mut s, &[]);
    }

    /// Erase the block for good (end of run; the summary prints
    /// after). Terminal: every later render, including a ticker that
    /// was already blocked on the lock when the run ended, is a no-op.
    pub fn clear(&self) {
        let mut s = self.lock();
        s.done = true;
        if self.tty && s.drawn_lines > 0 {
            eprint!("\x1b[{}A\x1b[0J", s.drawn_lines);
            s.drawn_lines = 0;
            let _ = std::io::stderr().flush();
        }
    }

    /// The one writer: erase the previous block, print any new
    /// permanent lines, redraw the bars. Everything through one path
    /// so concurrent events never interleave mid-line.
    fn render(&self, s: &mut State, permanent: &[String]) {
        if s.done {
            return;
        }
        if !self.tty {
            for line in permanent {
                eprintln!("{line}");
            }
            return;
        }
        let mut out = String::new();
        if s.drawn_lines > 0 {
            out.push_str(&format!("\x1b[{}A\x1b[0J", s.drawn_lines));
        }
        for line in permanent {
            out.push_str(line);
            out.push('\n');
        }
        let name_width = s.bars.iter().map(|b| b.name.len()).max().unwrap_or(0);
        let columns = terminal_columns();
        for bar in &s.bars {
            out.push_str(&render_bar(bar, name_width, columns));
            out.push('\n');
        }
        s.drawn_lines = s.bars.len();
        eprint!("{out}");
        let _ = std::io::stderr().flush();
    }
}

/// The terminal's column count, defaulting to a classic 80 when it
/// cannot be read. Every bar line is truncated to this so the drawn
/// block's height equals its line count (a wrapped line would break
/// the cursor-up erase).
fn terminal_columns() -> usize {
    terminal_size::terminal_size()
        .map(|(terminal_size::Width(w), _)| w as usize)
        .unwrap_or(80)
}

/// Find a package's bar and move it to the end of the stack (the
/// bottom of the block), where the most recent activity is easiest to
/// watch.
fn touch<'a>(bars: &'a mut Vec<PackageBar>, package: &str) -> &'a mut PackageBar {
    let i = match bars.iter().position(|b| b.name == package) {
        Some(i) => i,
        // A package outside the initial selection cannot happen (the
        // selection builds the bars), but a progress render must never
        // panic a run over its own bookkeeping.
        None => {
            bars.push(PackageBar {
                name: package.to_string(),
                total: 0,
                done: 0,
                failed: 0,
                running: Vec::new(),
            });
            bars.len() - 1
        }
    };
    let bar = bars.remove(i);
    bars.push(bar);
    bars.last_mut().expect("just pushed")
}

fn render_bar(bar: &PackageBar, name_width: usize, columns: usize) -> String {
    const WIDTH: usize = 20;
    // Clamped: `done` can only exceed `total` through the defensive
    // zero-total bar `touch` creates, but the arithmetic must be
    // impossible to underflow regardless (this code must never panic a
    // run).
    let filled = (WIDTH * bar.done).checked_div(bar.total).unwrap_or(0).min(WIDTH);
    let cells: String = "█".repeat(filled) + &"░".repeat(WIDTH - filled);
    let fail = if bar.failed > 0 { format!("  {} failed", bar.failed) } else { String::new() };
    let running = match bar.running.len() {
        0 => String::new(),
        // Up to two names with their elapsed; beyond that, the oldest
        // (the one worth watching) plus a count.
        1 | 2 => format!(
            "  running: {}",
            bar.running
                .iter()
                .map(|(name, at)| format!("{name} {}s", at.elapsed().as_secs()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        n => {
            let (name, at) = &bar.running[0];
            format!("  {n} running (oldest {name} {}s)", at.elapsed().as_secs())
        }
    };
    let line = format!(
        "{:<name_width$}  {cells}  {}/{}{fail}{running}",
        bar.name, bar.done, bar.total
    );
    truncate_chars(&line, columns)
}

/// Truncate to at most `max` characters, on a char boundary (names may
/// be non-ASCII). One column per char is an approximation, but every
/// overshoot it misses is double-width glyphs weft never emits in bar
/// lines.
fn truncate_chars(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => s[..idx].to_string(),
        None => s.to_string(),
    }
}

/// The first line of a (possibly multi-line) error, for the inline
/// failure tag.
fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bar_reflects_progress_and_failures() {
        let bar = PackageBar {
            name: "google".into(),
            total: 4,
            done: 2,
            failed: 1,
            running: vec![("drive_copy".into(), Instant::now())],
        };
        let line = render_bar(&bar, 6, 200);
        assert!(line.starts_with("google"), "{line}");
        assert!(line.contains("2/4"), "{line}");
        assert!(line.contains("1 failed"), "{line}");
        assert!(line.contains("running: drive_copy"), "{line}");
        // Half done = half the cells filled.
        assert_eq!(line.matches('█').count(), 10, "{line}");
    }

    #[test]
    fn many_running_collapse_to_a_count_with_the_oldest() {
        let bar = PackageBar {
            name: "slack".into(),
            total: 9,
            done: 0,
            failed: 0,
            running: vec![
                ("first".into(), Instant::now()),
                ("second".into(), Instant::now()),
                ("third".into(), Instant::now()),
            ],
        };
        let line = render_bar(&bar, 5, 200);
        assert!(line.contains("3 running (oldest first"), "{line}");
    }

    #[test]
    fn touch_moves_the_updated_package_to_the_bottom() {
        let mut bars = vec![
            PackageBar { name: "a".into(), total: 1, done: 0, failed: 0, running: vec![] },
            PackageBar { name: "b".into(), total: 1, done: 0, failed: 0, running: vec![] },
        ];
        touch(&mut bars, "a");
        assert_eq!(bars.last().expect("two bars").name, "a");
    }

    #[test]
    fn degenerate_counts_never_break_the_bar_arithmetic() {
        // total == 0: the defensive bar `touch` creates for a package
        // outside the initial selection.
        let zero = PackageBar { name: "x".into(), total: 0, done: 3, failed: 0, running: vec![] };
        let line = render_bar(&zero, 1, 200);
        assert_eq!(line.chars().filter(|c| *c == '█' || *c == '░').count(), 20, "{line}");
        // done > total: must clamp full, never underflow the repeat.
        let over = PackageBar { name: "y".into(), total: 2, done: 5, failed: 0, running: vec![] };
        let line = render_bar(&over, 1, 200);
        assert_eq!(line.matches('█').count(), 20, "{line}");
        assert_eq!(line.matches('░').count(), 0, "{line}");
    }

    #[test]
    fn bar_lines_are_truncated_to_the_column_budget() {
        let bar = PackageBar {
            name: "a_rather_long_package_name".into(),
            total: 9,
            done: 1,
            failed: 0,
            running: vec![
                ("one_real_very_long_test_name_indeed".into(), Instant::now()),
                ("another_extremely_long_test_name".into(), Instant::now()),
            ],
        };
        let line = render_bar(&bar, 26, 60);
        assert_eq!(line.chars().count(), 60, "{line}");
    }

    #[test]
    fn a_finished_package_leaves_the_stack_and_a_stray_event_cannot_panic() {
        let p = LiveProgress::with_tty(
            &[("pkg".to_string(), "t1".to_string()), ("pkg".to_string(), "t2".to_string())],
            false,
        );
        p.started("pkg", "t1");
        p.finished("pkg", "t1", true, None);
        p.started("pkg", "t2");
        p.finished("pkg", "t2", false, Some("boom"));
        assert!(p.lock().bars.is_empty(), "the completed package left the stack");
        // A stray late event resurrects a defensive zero-total bar,
        // renders without panicking, and self-removes again (done >=
        // total holds immediately on a zero-total bar).
        p.finished("pkg", "t2", false, None);
        assert!(p.lock().bars.is_empty());
        p.tick();
    }

    #[test]
    fn after_clear_every_render_is_a_no_op() {
        let p = LiveProgress::with_tty(&[("pkg".to_string(), "t".to_string())], false);
        p.started("pkg", "t");
        p.clear();
        // A ticker that lost the race to `clear` must not repaint.
        p.tick();
        p.finished("pkg", "t", true, None);
        assert!(p.lock().done);
        assert_eq!(p.lock().drawn_lines, 0);
    }
}
