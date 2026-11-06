//! `weft deps-install [NodeType...] [--all]`: install on this machine what
//! nodes need from it (`weft_core::needs`): the programs they launch, what
//! their packages need to compile. With no node type, what the program in
//! this folder uses, its infra processes included; with `--all`, every
//! node of the project's catalog. Only the needs this machine does not
//! meet yet are installed: with the system's own package manager, after
//! the person agreed to the exact command (`--yes` for a script, `--print`
//! to only show it), and, for a need weft installs itself, with its recipe
//! into weft's own folder, as an infra process's setup does
//! (`Need::meet`).

use std::path::Path;

use anyhow::{Context, Result};

use weft_core::needs::{Need, NeedStage, NodeNeed, System};

use super::Ctx;

pub struct DepsInstall {
    pub node_types: Vec<String>,
    pub all: bool,
    pub stage: Option<NeedStage>,
    pub yes: bool,
    pub print: bool,
}

/// One need asked about, with who needs it, as the command says it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Wanted {
    who: String,
    need: Need,
    /// An infra process's, which only a start of the infra uses.
    infra: bool,
}

pub async fn run(ctx: Ctx, args: DepsInstall) -> Result<()> {
    let project = ctx.project()?;
    let mut wanted: Vec<Wanted> =
        needs_wanted(project, &args)?.into_iter().map(|row| Wanted { who: row.node_type, need: row.need, infra: false }).collect();
    if args.node_types.is_empty() && !args.all {
        wanted.extend(infra_needs(project.id())?);
    }
    let wanted: Vec<Wanted> = wanted.into_iter().filter(|row| args.stage.is_none_or(|stage| row.need.stage == stage)).collect();
    if wanted.is_empty() {
        println!("{}", nothing_needed(&args));
        return Ok(());
    }
    let root = weft_platform_local::MachineLayout::here().needs();
    let path = std::env::var("PATH").unwrap_or_default();
    // Checked the way the install checks them before anything starts: on
    // the PATH, or, for a need weft installs, in its folder.
    let missing = unmet(&root, &path, wanted).await?;
    if missing.is_empty() {
        println!("everything these nodes need is on this machine already");
        return Ok(());
    }
    let system = System::here();
    let plan = {
        let system = system.clone();
        tokio::task::spawn_blocking(move || {
            let lacking = |packages: &[String]| system.as_ref().map(|s| s.lacking(packages)).unwrap_or_default();
            install_plan(&system, &missing, lacking)
        })
        .await
        .context("plan the install")?
    };
    for row in plan.covered.iter().chain(&plan.recipes) {
        println!("{} needs {}", row.who, row.need.name);
    }
    for row in &plan.unknown {
        eprintln!("{} needs {}, and names no package for this system: {}", row.who, row.need.name, row.need.by_hand());
    }
    if let Err(why) = &system {
        if !plan.unknown.is_empty() {
            eprintln!("({why})");
        }
    }
    if plan.packages.is_empty() && plan.recipes.is_empty() {
        anyhow::bail!("nothing here can be installed for this system; install what is said above by hand, then run this again");
    }
    let command = match &system {
        Ok(system) if !plan.packages.is_empty() => Some(system.install_command(&plan.packages)),
        _ => None,
    };
    if let Some(command) = &command {
        println!("to install them: {}", command.join(" "));
    }
    for row in &plan.recipes {
        println!("{}weft installs {} into {}", if command.is_some() { "then " } else { "" }, row.need.name, recipe_dir(&root, &row.need));
    }
    if args.print {
        return Ok(());
    }
    if !args.yes && !crate::prompt::ask_yes_no("go ahead? [Y/n] ", true, "--yes (or --print to only show it)")? {
        println!("nothing was installed");
        return Ok(());
    }
    if let Some(command) = &command {
        let status = tokio::process::Command::new(&command[0])
            .args(&command[1..])
            .status()
            .await
            .with_context(|| format!("run `{}`", command.join(" ")))?;
        anyhow::ensure!(status.success(), "`{}` failed ({status}); nothing more was done", command.join(" "));
    }
    install_recipes(&root, &path, &system, &plan.recipes).await?;
    // Only what was installed is checked again: a need with no package
    // for this system is still missing, and is said apart.
    let still = unmet(&root, &path, plan.covered.clone()).await?;
    if !still.is_empty() {
        anyhow::bail!(
            "installed, but this machine still does not meet: {}; check that what was installed is on your PATH, \
             then run ./setup.sh again from that shell: the install's workers get the PATH of the shell that last \
             started it",
            named(&still)
        );
    }
    if !plan.unknown.is_empty() {
        anyhow::bail!(
            "installed what this system has packages for; still to install by hand: {}; then run this again",
            named(&plan.unknown)
        );
    }
    println!("installed; every need these nodes have is met");
    if plan.covered.iter().chain(&plan.recipes).any(|row| row.infra) {
        println!("start the infra again to use it: weft infra start");
    }
    Ok(())
}

/// The needs of the infra processes this install runs for the project
/// (`weft_platform_local::process_units::held_needs`): what the next start
/// of its infra needs, a process that could not set up included.
fn infra_needs(project: uuid::Uuid) -> Result<Vec<Wanted>> {
    // No install acts from here (a detached HEAD, a branch whose install
    // is not set up), so no infra of the project runs here either.
    let Ok(install) = weft_core::infra::Install::from_env() else { return Ok(Vec::new()) };
    let held = weft_platform_local::process_units::held_needs(&weft_platform_local::InstallLayout::new(install.dir()).process_units(), project)?;
    Ok(held
        .into_iter()
        .flat_map(|(node, unit, needs)| {
            let who = format!("infra node '{}' (unit '{unit}')", weft_core::project::plain_id(&node));
            needs.into_iter().map(move |need| Wanted { who: who.clone(), need, infra: true })
        })
        .collect())
}

/// The ones of `rows` this machine does not meet (`Need::met_at`), each
/// need checked once even when several ask for it.
async fn unmet(root: &Path, path: &str, rows: Vec<Wanted>) -> Result<Vec<Wanted>> {
    let (root, path) = (root.to_path_buf(), path.to_string());
    tokio::task::spawn_blocking(move || {
        let mut met: Vec<(Need, bool)> = Vec::new();
        rows.into_iter()
            .filter(|row| {
                let is_met = match met.iter().find(|(need, _)| need == &row.need) {
                    Some((_, is_met)) => *is_met,
                    None => {
                        let is_met = row.need.met_at(&root, &path).is_some();
                        met.push((row.need.clone(), is_met));
                        is_met
                    }
                };
                !is_met
            })
            .collect()
    })
    .await
    .context("check what the machine has")
}

/// Install each of `rows` with its recipe, the way an infra process's
/// setup does (`Need::meet`, which takes turns with any other setup of the
/// same need on the machine), saying each step.
async fn install_recipes(root: &Path, path: &str, system: &Result<System, String>, rows: &[Wanted]) -> Result<()> {
    let mut done: Vec<&Need> = Vec::new();
    for row in rows {
        if done.contains(&&row.need) {
            continue;
        }
        done.push(&row.need);
        let (need, root, path, system) = (row.need.clone(), root.to_path_buf(), path.to_string(), system.clone());
        tokio::task::spawn_blocking(move || need.meet(&root, &path, &system, &mut |step| eprintln!("{}", step.words(&need.name))))
            .await
            .context("install a need")?
            .map_err(anyhow::Error::msg)?;
    }
    Ok(())
}

/// Where weft installs `need` (one with a recipe), as the person reads it.
fn recipe_dir(root: &Path, need: &Need) -> String {
    match need.recipe.as_ref().map(|r| r.place(root)) {
        Some(Ok(place)) => place.dir.display().to_string(),
        Some(Err(why)) => format!("nowhere: {why}"),
        None => "nowhere: it has no recipe".to_string(),
    }
}

/// The needs the command was asked about: the named node types', every
/// catalog type's with `--all`, else the program's (only the nodes it uses).
fn needs_wanted(project: &weft_compiler::project::Project, args: &DepsInstall) -> Result<Vec<NodeNeed>> {
    if args.all && !args.node_types.is_empty() {
        anyhow::bail!("--all installs what every node needs, so it takes no node type; drop one of them");
    }
    if args.node_types.is_empty() && !args.all {
        let (definition, catalog) = weft_compiler::hash::load_program(project, None).map_err(|e| anyhow::anyhow!("compile the program in this folder: {e}"))?;
        return weft_compiler::codegen::needs_of(&definition, &catalog).map_err(|e| anyhow::anyhow!("{e}"));
    }
    let roots = weft_compiler::project::node_roots(&project.root);
    let catalog = weft_catalog::FsCatalog::discover_roots(&roots.iter().map(|r| r.as_path()).collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("read the project's catalog: {e}"))?;
    let node_types: std::collections::BTreeSet<String> = if args.all {
        catalog.iter().map(|entry| entry.node_type.clone()).collect()
    } else {
        for node_type in &args.node_types {
            anyhow::ensure!(
                catalog.entry(node_type).is_some(),
                "no node type '{node_type}' in this project's catalog (`weft describe-nodes --list` lists them)"
            );
        }
        args.node_types.iter().cloned().collect()
    };
    let needs = weft_compiler::codegen::node_needs(&node_types, &catalog).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(needs
        .into_iter()
        .flat_map(|(node_type, needs)| needs.into_iter().map(move |need| NodeNeed { node_type: node_type.clone(), nodes: Vec::new(), need }))
        .collect())
}

fn nothing_needed(args: &DepsInstall) -> &'static str {
    match (args.all, args.node_types.is_empty()) {
        (true, _) => "no node of this project's catalog needs anything from the machine",
        (false, true) => "no node this program uses needs anything from the machine",
        (false, false) => "these nodes need nothing from the machine",
    }
}

/// What to install for `missing` on `system`: every package, once, the
/// needs those packages meet, the needs weft installs with their recipe
/// (after the system packages they need that `lacking` says are not
/// there), and the needs that name no package for this system.
#[derive(Debug, PartialEq, Eq)]
struct InstallPlan {
    packages: Vec<String>,
    /// The needs `packages` are installed for, the ones checked again after.
    covered: Vec<Wanted>,
    /// The needs weft installs itself, after `packages`.
    recipes: Vec<Wanted>,
    /// The needs with no package for this system, left to install by hand.
    unknown: Vec<Wanted>,
}

fn install_plan(system: &Result<System, String>, missing: &[Wanted], lacking: impl Fn(&[String]) -> Vec<String>) -> InstallPlan {
    let mut plan = InstallPlan { packages: Vec::new(), covered: Vec::new(), recipes: Vec::new(), unknown: Vec::new() };
    let add = |packages: &[String], plan: &mut InstallPlan| {
        for package in packages {
            if !plan.packages.contains(package) {
                plan.packages.push(package.clone());
            }
        }
    };
    for row in missing {
        let named = system.as_ref().ok().and_then(|s| row.need.packages_for(s.manager, s.distro.as_deref()));
        match (&row.need.recipe, named) {
            (Some(_), named) => {
                add(&lacking(named.unwrap_or_default()), &mut plan);
                plan.recipes.push(row.clone());
            }
            (None, Some(packages)) => {
                add(packages, &mut plan);
                plan.covered.push(row.clone());
            }
            (None, None) => plan.unknown.push(row.clone()),
        }
    }
    plan
}

/// `need (who)` for each of `rows`, comma-separated.
fn named(rows: &[Wanted]) -> String {
    rows.iter().map(|row| format!("{} ({})", row.need.name, row.who)).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::needs::{Os, PackageManager, Recipe};

    fn row(who: &str, name: &str, apt: &[&str]) -> Wanted {
        let mut install = std::collections::BTreeMap::new();
        if !apt.is_empty() {
            install.insert(PackageManager::Apt, [("default".to_string(), apt.iter().map(|p| p.to_string()).collect())].into());
        }
        Wanted { who: who.into(), need: Need { name: name.into(), stage: NeedStage::Run, check: vec![name.into()], install, how: None, recipe: None }, infra: false }
    }

    /// A need weft installs, `fakeprog`, whose recipe puts it into the
    /// folder `tools/fake` and counts its runs in `count`.
    fn installed(count: &Path, apt: &[&str]) -> Wanted {
        let mut wanted = row("infra node 'agent' (unit 'server')", "fakeprog", apt);
        let script = format!(
            "echo ran >> '{}'\nmkdir -p \"$WEFT_NEED_DIR/bin\"\nprintf '#!/bin/sh\\nexit 0\\n' > \"$WEFT_NEED_DIR/bin/fakeprog\"\nchmod +x \"$WEFT_NEED_DIR/bin/fakeprog\"",
            count.display()
        );
        wanted.need.recipe =
            Some(Recipe { folder: "tools/fake".into(), script: [(Os::here().unwrap(), script)].into(), bin: vec!["bin".into()], prelude: String::new() });
        wanted.infra = true;
        wanted
    }

    #[test]
    fn each_package_is_installed_once_and_a_need_with_none_is_named() {
        let apt = Ok(System { manager: PackageManager::Apt, distro: Some("debian_12".into()) });
        let (python, ffmpeg, odd) = (row("ExecPython", "python3", &["python3"]), row("Studio", "ffmpeg", &["ffmpeg", "python3"]), row("Odd", "thing", &[]));
        let plan = install_plan(&apt, &[python.clone(), ffmpeg.clone(), odd.clone()], |_| panic!("no need here has a recipe"));
        assert_eq!(plan.packages, ["python3", "ffmpeg"]);
        // Only the needs a package meets are checked again after the
        // install; the one with none is left to say apart.
        assert_eq!(plan.covered, [python, ffmpeg]);
        assert_eq!(plan.unknown, std::slice::from_ref(&odd));
        assert_eq!(named(&plan.unknown), "thing (Odd)");
        let unknown = install_plan(&Err("this system (arch)".into()), std::slice::from_ref(&odd), |_| Vec::new());
        assert_eq!(unknown.unknown, [odd]);
    }

    /// A need weft installs is installed with its recipe, after only the
    /// system packages it needs that the machine lacks.
    #[test]
    fn a_need_weft_installs_takes_only_the_system_packages_it_lacks() {
        let dir = tempfile::tempdir().unwrap();
        let apt = Ok(System { manager: PackageManager::Apt, distro: None });
        let agent = installed(&dir.path().join("count"), &["curl", "git"]);
        let plan = install_plan(&apt, &[agent.clone(), row("ExecPython", "python3", &["python3"])], |asked| {
            assert_eq!(asked, ["curl", "git"]);
            vec!["git".into()]
        });
        assert_eq!(plan.packages, ["git", "python3"]);
        assert_eq!(plan.recipes, [agent]);
        assert!(plan.unknown.is_empty());
    }

    /// The command installs a need with the very setup an infra process
    /// runs (`Need::meet`), into weft's folder for it: once, however many
    /// ask for it, and found there afterwards.
    #[tokio::test]
    async fn a_need_weft_installs_is_installed_by_the_infras_own_setup() {
        let dir = tempfile::tempdir().unwrap();
        let (root, count) = (dir.path().join("weft"), dir.path().join("count"));
        let agent = installed(&count, &[]);
        let path = "/usr/bin:/bin";
        let missing = unmet(&root, path, vec![agent.clone(), agent.clone()]).await.unwrap();
        assert_eq!(missing.len(), 2);
        install_recipes(&root, path, &Err("not asked".into()), &missing).await.unwrap();
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "ran\n", "installed once");
        assert_eq!(agent.need.met_at(&root, path), Some(vec![root.join("tools/fake/bin")]));
        assert!(unmet(&root, path, vec![agent.clone()]).await.unwrap().is_empty(), "met now");
        assert_eq!(recipe_dir(&root, &agent.need), root.join("tools/fake").display().to_string());
    }
}
