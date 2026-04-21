use super::Ctx;

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client();
    let projects: serde_json::Value = client.get_json("/projects").await?;
    let arr = projects.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("no projects registered");
        return Ok(());
    }
    println!("{:<38}  {:<24}  status", "id", "name");
    for p in arr {
        let id = p.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let status = p.get("status").and_then(|v| v.as_str()).unwrap_or("");
        println!("{id:<38}  {name:<24}  {status}");
    }
    Ok(())
}
