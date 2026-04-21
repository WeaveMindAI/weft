use super::Ctx;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    match ctx.client().get_json("/projects").await {
        Ok(value) => {
            let arr = value.as_array().cloned().unwrap_or_default();
            println!("dispatcher ok, {} project(s) registered", arr.len());
            Ok(())
        }
        Err(e) => {
            anyhow::bail!("dispatcher unreachable: {e}")
        }
    }
}
