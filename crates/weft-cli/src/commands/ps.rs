use super::Ctx;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let projects: serde_json::Value = client.get_json("/projects").await?;
    if ctx.json_out(&projects)? {
        return Ok(());
    }
    let arr: Vec<weft_core::projects::ProjectSummary> =
        anyhow::Context::context(serde_json::from_value(projects), "read the install's projects")?;
    if arr.is_empty() {
        println!("no projects registered");
        return Ok(());
    }
    println!("{:<38}  {:<24}  status", "id", "name");
    for p in arr {
        println!("{:<38}  {:<24}  {}", p.id, p.name, p.status);
    }
    Ok(())
}
