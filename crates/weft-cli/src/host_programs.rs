//! The programs this CLI compiles on the machine: the standard worker (for
//! every install on the machine, and the release's prebuilt asset) and a
//! package's node-test server. Both compile through
//! `weft_platform_local::host_build::compile`, the one compile an install's
//! own worker builds go through too, in the machine's compile lanes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use weft_platform_local::host_build::{self, CompileIn, HostBuilt};
use weft_platform_local::{InstallLayout, MachineLayout};

/// Compile the host build `staged` on this machine into `out_dir`, in
/// `cache`, with its output in `log` (written fresh): the standard worker,
/// or a node package's test server. Says so on stderr while every lane, or
/// the seed, is held by another build.
pub(crate) async fn compile(
    staged: &weft_compiler::build::StagedImageBuild,
    out_dir: PathBuf,
    log: &Path,
    cache: CompileIn,
) -> Result<HostBuilt> {
    let waiting = match cache {
        CompileIn::Seed => "waiting for another weft compiling the standard worker on this machine",
        CompileIn::Lane { .. } => "every compile lane of this machine is busy with other builds; waiting for one (WEFT_COMPILE_LANES sets how many)",
    };
    host_build::compile(
        &weft_platform_local::MachineToolchain,
        &MachineLayout::here(),
        cache,
        &staged.build_context,
        out_dir,
        log,
        move || eprintln!("{waiting}"),
    )
    .await
}

/// A free compile lane of this machine's (`weft_platform_local::lanes`).
pub(crate) fn any_lane() -> Result<CompileIn> {
    Ok(CompileIn::Lane { lanes: weft_platform_local::lanes::lane_count()? })
}

/// The standard worker as a program on this machine (a local install's
/// workers run as processes): its folder in the machine's workers folder,
/// named by `hash` (the stock worker's binary hash,
/// `weft_compiler::build::stock_binary_hash` of a host build). When it is
/// missing (or with `rebuild`) it is made from the release's prebuilt
/// program when setup unpacked it there, else compiled on the machine in
/// the seed every compile lane starts from, so a project with nodes of its
/// own compiles only those. Answers its ref (`weft-worker:<hash>`), the one
/// every install's builds name it by: a project whose nodes are all the
/// standard library's finds it there and compiles nothing.
pub async fn ensure_standard_program(install: &weft_core::infra::Install, hash: &str, rebuild: bool) -> Result<String> {
    let image = weft_compiler::build::worker_image_tag(hash);
    let workers = MachineLayout::here().workers();
    let program = weft_platform_local::images::Program::Worker(hash);
    if !rebuild && program.file(&workers).is_file() {
        return Ok(image);
    }
    let (_stock, staged) = stage_standard_program()?;
    anyhow::ensure!(
        staged.content_hash == hash,
        "the standard worker staged as {}, where its hash says {hash}; the stock hash and the stock build disagree",
        staged.content_hash
    );
    let log = InstallLayout::new(install.dir()).build_log(&image);
    eprintln!("compiling the standard worker on this machine, once per weft version (its log: {})", log.display());
    compile_standard_program(&staged, program.dir(&workers), &log).await?;
    Ok(image)
}

/// `weft build-standard-worker --out <file>`: the standard worker
/// compiled on this machine, packed as one gzipped tar of its folder
/// (`worker`, `left-out.json`, and `hash`, its binary hash), the asset
/// the release publishes per machine type and setup.sh unpacks into
/// the machine's workers folder.
// SYNC: the asset's layout <-> setup.sh (install_prebuilt_standard_worker), .github/actions/build-binaries
pub async fn pack_standard_program(out: &Path) -> Result<()> {
    let (_stock, staged) = stage_standard_program()?;
    let packing = tempfile::tempdir()?;
    let folder = packing.path().join("standard-worker");
    let log = packing.path().join("build.log");
    compile_standard_program(&staged, folder.clone(), &log).await.inspect_err(|_| {
        if let Ok(text) = std::fs::read_to_string(&log) {
            eprintln!("{text}");
        }
    })?;
    std::fs::write(folder.join("hash"), &staged.content_hash)?;
    let status = tokio::process::Command::new("tar")
        .arg("-czf")
        .arg(out)
        .arg("-C")
        .arg(&folder)
        .args([host_build::PROGRAM_FILE, host_build::LEFT_OUT_FILE, "hash"])
        .status()
        .await
        .context("run tar")?;
    anyhow::ensure!(status.success(), "tar of the standard worker failed ({status})");
    println!("{}", staged.content_hash);
    Ok(())
}

/// The stock project staged as a host build. The project's own folder (a
/// temporary one) holds the staged build, so the two go together: the
/// build is read while the project is kept.
fn stage_standard_program() -> Result<(weft_compiler::build::StockProject, weft_compiler::build::StagedImageBuild)> {
    let stock = weft_compiler::build::StockProject::materialize()?;
    let packaging = MachineLayout::here().host_packaging();
    let staged = tokio::task::block_in_place(|| {
        weft_compiler::build::build_project(
            &stock.project,
            &stock.definition,
            &stock.contents,
            &stock.catalog,
            weft_compiler::CompileFs::disk(&stock.project.root),
            &packaging,
            weft_core::builds::NodeSet::Full,
            &weft_compiler::hash::DISK,
        )
    })?;
    Ok((stock, staged))
}

/// Compile `staged` in the machine's seed cache into `out_dir`, with its
/// output in `log`. It must leave nothing out: every install uses it as
/// built, reading no record of it, so a stock node that does not compile
/// fails it here, naming the package, and the program the compile already
/// placed in `out_dir` is removed, so no later start finds it there and
/// runs it short of a package. The folder stays, with the holds other
/// installs have on it: theirs build it again too. Whole, the seed is marked so
/// every lane's cache may start from it (`host_build::compile`).
async fn compile_standard_program(staged: &weft_compiler::build::StagedImageBuild, out_dir: PathBuf, log: &Path) -> Result<()> {
    match compile(staged, out_dir.clone(), log, CompileIn::Seed).await? {
        HostBuilt::Built(record) => {
            let record = weft_core::builds::LeftOut::parse(&record).map_err(anyhow::Error::msg)?;
            if !record.is_empty() {
                let program = out_dir.join(host_build::PROGRAM_FILE);
                let kept = match std::fs::remove_file(&program) {
                    Ok(()) => "so it was not kept".to_string(),
                    Err(e) => format!("and it could not be removed ({e}); delete {} by hand", program.display()),
                };
                anyhow::bail!(
                    "the standard worker left packages out because they did not compile, {kept}:\n  {}",
                    record.packages.iter().map(|p| p.reason(Some(&log.display().to_string()))).collect::<Vec<_>>().join("\n  ")
                );
            }
            Ok(())
        }
        HostBuilt::Failed(reason) => anyhow::bail!("the standard worker did not compile: {reason}\nits log: {}", log.display()),
    }
}
