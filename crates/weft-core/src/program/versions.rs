//! A program's calls on a project's code: its working copies, its
//! version tree, and building it (`ctx.project(..).files()`,
//! `.checkpoint()`, `.switch(..)`, `.tree()`, `.build()`).

use serde::{Deserialize, Serialize};

use crate::working_copy::{ChangeFiles, SwitchRequest, WorkingCopyRef};

/// One call on a project's code.
// SYNC: VersionCall <-> crates/weft-dispatcher/src/actions/versions.rs (`program_call`'s match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum VersionCall {
    /// Every working copy of the project.
    WorkingCopies,
    /// A working copy with its files.
    WorkingState { copy: WorkingCopyRef },
    /// Change files of a working copy (every blob already stored).
    ChangeFiles { request: ChangeFiles },
    /// One file of a working copy, as a stored file the program reads
    /// with `ctx.storage(..).get(..)`; none when the copy holds no such
    /// file.
    ReadFile { copy: WorkingCopyRef, path: String },
    /// Write files into a working copy from their bytes.
    WriteFiles { request: crate::working_copy::WriteFiles },
    /// Stamp a working copy's files into a version.
    Checkpoint { request: crate::versions::CheckpointRequest },
    /// Move a working copy to a version or a run, keeping what it held as
    /// a version first.
    Switch { request: SwitchRequest },
    /// Record a working copy's files as a version and remove the copy (a
    /// folder that is gone, a program copy nobody uses).
    Forget { copy: crate::working_copy::WorkingCopyId },
    /// The version tree.
    Tree,
    /// Delete a version subtree and its runs, or (with `plan`) say what
    /// that would remove.
    Prune {
        version: String,
        #[serde(default)]
        frozen: Vec<String>,
        #[serde(default)]
        plan: bool,
    },
    /// Associate a recorded run with a saved example.
    UpdateRun { run: crate::RunId, update: crate::versions::RunUpdate },
    /// The files of the build the project runs.
    Running,
    /// Build a working copy (stamped first) or a version.
    Build { request: Box<crate::builds::BuildRequest> },
    /// Where a version build that waited on its images stands.
    BuildState { build: uuid::Uuid },
}

impl VersionCall {
    pub const NAMES: &'static [&'static str] = &[
        "weft.working_copies.list",
        "weft.working_copies.read",
        "weft.working_copies.change",
        "weft.working_copies.read_file",
        "weft.working_copies.write",
        "weft.versions.checkpoint",
        "weft.versions.switch",
        "weft.working_copies.forget",
        "weft.versions.tree",
        "weft.versions.prune",
        "weft.versions.update_run",
        "weft.versions.running",
        "weft.builds.build",
        "weft.builds.state",
    ];

    pub fn name(&self) -> &'static str {
        match self {
            VersionCall::WorkingCopies => "weft.working_copies.list",
            VersionCall::WorkingState { .. } => "weft.working_copies.read",
            VersionCall::ChangeFiles { .. } => "weft.working_copies.change",
            VersionCall::ReadFile { .. } => "weft.working_copies.read_file",
            VersionCall::WriteFiles { .. } => "weft.working_copies.write",
            VersionCall::Checkpoint { .. } => "weft.versions.checkpoint",
            VersionCall::Switch { .. } => "weft.versions.switch",
            VersionCall::Forget { .. } => "weft.working_copies.forget",
            VersionCall::Tree => "weft.versions.tree",
            VersionCall::Prune { .. } => "weft.versions.prune",
            VersionCall::UpdateRun { .. } => "weft.versions.update_run",
            VersionCall::Running => "weft.versions.running",
            VersionCall::Build { .. } => "weft.builds.build",
            VersionCall::BuildState { .. } => "weft.builds.state",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::ProgramCall;
    use crate::working_copy::{Expect, FileContent, FileWrite, WriteFiles};

    /// What a program writes travels as its area, its call and the files,
    /// each with what it puts there.
    #[test]
    fn a_write_of_files_round_trips_with_its_contents() {
        let request = WriteFiles {
            working_copy: WorkingCopyRef::default(),
            files: vec![
                FileWrite { path: "main.weft".into(), content: FileContent::Text { text: "a = Text".into() }, expected: Expect::Any },
                FileWrite { path: "assets/clip.mp4".into(), content: FileContent::Stored { key: "run/r/clip".into() }, expected: Expect::Absent },
                FileWrite { path: "old.weft".into(), content: FileContent::Removed, expected: Expect::Any },
            ],
        };
        let call = ProgramCall::Versions(VersionCall::WriteFiles { request });
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!((&wire["area"], &wire["call"]), (&serde_json::json!("versions"), &serde_json::json!("write_files")));
        assert_eq!(
            wire["request"]["files"][0],
            serde_json::json!({ "path": "main.weft", "content": "text", "text": "a = Text", "expected": { "expect": "any" } })
        );
        assert_eq!(wire["request"]["files"][2]["content"], "removed");
        assert_eq!(serde_json::from_value::<ProgramCall>(wire).unwrap(), call);
        // A write that names no expectation lands over whatever is there.
        let bare: FileWrite = serde_json::from_value(serde_json::json!({ "path": "a", "content": "removed" })).unwrap();
        assert_eq!(bare.expected, Expect::Any);
    }

    #[test]
    fn every_call_has_its_name_listed() {
        let calls = [
            VersionCall::WorkingCopies,
            VersionCall::WorkingState { copy: WorkingCopyRef::default() },
            VersionCall::ChangeFiles { request: ChangeFiles::default() },
            VersionCall::ReadFile { copy: WorkingCopyRef::default(), path: "a".into() },
            VersionCall::WriteFiles { request: Default::default() },
            VersionCall::Checkpoint { request: Default::default() },
            VersionCall::Switch { request: SwitchRequest::default() },
            VersionCall::Forget { copy: uuid::Uuid::nil() },
            VersionCall::Tree,
            VersionCall::Prune { version: "v".into(), frozen: vec![], plan: true },
            VersionCall::UpdateRun { run: crate::RunId::nil(), update: Default::default() },
            VersionCall::Running,
            VersionCall::Build { request: Default::default() },
            VersionCall::BuildState { build: uuid::Uuid::nil() },
        ];
        let names: Vec<&str> = calls.iter().map(VersionCall::name).collect();
        assert_eq!(names, VersionCall::NAMES, "one name per call, in the listed order");
    }
}
