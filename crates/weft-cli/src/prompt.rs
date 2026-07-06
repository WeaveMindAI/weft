//! Interactive prompts shared by the destructive / choice-bearing CLI
//! verbs. ONE rule, enforced in one place: every command must be fully
//! runnable via flags, and when the deciding flag is absent we either
//! ask a human (a real terminal) or, if there is no human (piped /
//! redirected stdin, an AI or a script), error LOUDLY naming the flag,
//! never hang on a read that nobody will answer and never silently pick
//! a default.

use std::io::{IsTerminal, Write};

/// Read one line from the user for `prompt`. On a terminal: print the
/// prompt, flush, read a trimmed line. With NO terminal (piped/closed
/// stdin): bail naming `flag_hint`, the flag(s) that choose this
/// non-interactively, so a scripted/AI run fails fast with a fix instead
/// of blocking forever. The caller interprets the returned line (yes/no,
/// a number, a menu choice); this owns only the terminal gate + read.
pub fn prompt_line(prompt: &str, flag_hint: &str) -> anyhow::Result<String> {
    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "no input for this prompt (stdin is not a terminal); pass {flag_hint} \
             to choose non-interactively"
        );
    }
    // A prompt whose text goes into a pipe while the read comes from the
    // terminal is an invisible hang (the user never sees the question).
    // Prompting requires BOTH ends to be the terminal.
    if !std::io::stdout().is_terminal() {
        anyhow::bail!(
            "cannot prompt (stdout is not a terminal, the prompt would be invisible); \
             pass {flag_hint} to choose non-interactively"
        );
    }
    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

/// Yes/no confirmation for a destructive verb. `true` only on an
/// explicit "yes"/"y" (any casing); anything else (including a bare
/// Enter or EOF) is the safe answer: no. Non-interactive stdin bails via
/// [`prompt_line`] naming `flag_hint`.
pub fn confirm(prompt: &str, flag_hint: &str) -> anyhow::Result<bool> {
    let answer = prompt_line(prompt, flag_hint)?;
    Ok(matches!(answer.to_lowercase().as_str(), "yes" | "y"))
}
