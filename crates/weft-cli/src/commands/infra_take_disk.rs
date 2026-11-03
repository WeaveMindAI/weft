//! `weft infra take-disk <volume>`: give a disk volume an older weft named
//! without the install to the install this command acts on, when weft
//! cannot tell on its own whose data it holds
//! (`weft_platform_local::infra_names::plan_disk`). It only says so
//! (`weft_platform_local::disk_claims`); the install's next start of that
//! infra does the copy.

use anyhow::Result;

use super::Ctx;

pub async fn run(ctx: Ctx, volume: &str) -> Result<()> {
    ctx.refuse_other_install("weft infra take-disk")?;
    let install = weft_core::infra::Install::from_env().map_err(anyhow::Error::msg)?;
    let docker = weft_platform_local::DockerCli;
    let Some(labels) = weft_platform_local::docker::volume_labels(&docker, volume).await? else {
        anyhow::bail!("there is no volume {volume} on this machine's Docker (`docker volume ls` lists them)");
    };
    let Some(node) = weft_platform_local::infra_host::old_disk_copy(volume, &labels) else {
        anyhow::bail!(
            "{volume} is not a disk volume an older weft named without the install; only those can be taken, and \
             every other one already belongs to the install in its name"
        );
    };
    let label = install.label_value();
    weft_platform_local::disk_claims::claim(&weft_platform_local::MachineLayout::here().disk_claims(), volume, label)?;
    println!(
        "install '{label}' takes {volume}. Its next start of the infra '{}' copies the volume into this install's own \
         and removes it (`weft infra start`, or `weft infra stop` then `weft infra start` if it is running); every \
         other install that ran it starts that disk empty.",
        node.node
    );
    Ok(())
}
