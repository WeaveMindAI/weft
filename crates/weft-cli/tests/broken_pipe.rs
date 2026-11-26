//! A command whose output nobody reads any more (`weft ... | grep -q X`,
//! `| head`) ends quietly and successfully instead of panicking.

#[test]
fn a_reader_that_went_away_ends_the_command_quietly() {
    let (reader, writer) = std::io::pipe().unwrap();
    // Gone before the command writes a byte.
    drop(reader);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_weft"))
        .args(["daemon", "which"])
        .env("WEFT_INSTALL", "default")
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{:?}: {stderr}", out.status);
    assert!(!stderr.contains("panicked"), "{stderr}");
}
