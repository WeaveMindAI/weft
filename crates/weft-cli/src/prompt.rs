//! Interactive prompts shared by the destructive / choice-bearing CLI
//! verbs. ONE rule, enforced in one place: every command must be fully
//! runnable via flags, and when the deciding flag is absent we either
//! ask a human (a real terminal) or, if there is no human (piped /
//! redirected stdin, an AI or a script), error LOUDLY naming the flag,
//! never hang on a read that nobody will answer and never silently pick
//! a default.

use std::io::{IsTerminal, Write};

/// Is there a human at every end? Prompting needs stdin, stdout AND
/// stderr on a terminal. The question rides stderr (so a `--json`
/// run's stdout stays one JSON object per line even when a prompt
/// fires), but the CONTEXT the question refers to (a numbered
/// connection list, a consent URL) prints on stdout, so a redirected
/// stdout would leave the user answering a question whose list went
/// into a file; and a read from a pipe blocks on nobody. THE one
/// definition of "interactive"; every prompt and every
/// prompt-or-default decision reads it, never a bare `is_terminal()`
/// probe of its own.
pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
        && std::io::stderr().is_terminal()
}

/// Read one line from the user for `prompt`. On a terminal: print the
/// prompt to stderr, flush, read a trimmed line. With NO terminal
/// (piped/closed stdin or stderr): bail naming `flag_hint`, the flag(s) that choose
/// this non-interactively, so a scripted/AI run fails fast with a fix
/// instead of blocking forever. The caller interprets the returned line
/// (yes/no, a number, a menu choice); this owns only the terminal gate
/// + read.
pub fn prompt_line(prompt: &str, flag_hint: &str) -> anyhow::Result<String> {
    if !is_interactive() {
        // Name the failing end: a run with a piped output stream reads
        // fine but the prompt (stderr) or its context (stdout) would
        // vanish into the pipe.
        let why = if !std::io::stdin().is_terminal() {
            "no terminal to read this prompt's answer from"
        } else if !std::io::stdout().is_terminal() {
            "stdout is not a terminal, so what the prompt refers to would be invisible"
        } else {
            "stderr is not a terminal, so the prompt would be invisible"
        };
        anyhow::bail!("{why}; pass {flag_hint} to choose non-interactively");
    }
    eprint!("{prompt}");
    std::io::stderr().flush()?;
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
