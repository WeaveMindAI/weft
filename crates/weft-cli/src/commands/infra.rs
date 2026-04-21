use super::Ctx;

pub enum InfraAction {
    Up,
    Down,
}

pub async fn run(_ctx: Ctx, _action: InfraAction) -> anyhow::Result<()> {
    anyhow::bail!("`weft infra` not yet implemented")
}
