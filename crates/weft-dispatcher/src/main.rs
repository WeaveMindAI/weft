//! The weft dispatcher binary. A thin shim over `weft_dispatcher::app::run`,
//! which builds the single-tenant dispatcher from the boot building blocks and
//! serves.

use clap::Parser;

use weft_dispatcher::app::run;

#[derive(Debug, Parser)]
#[command(name = "weft-dispatcher", version)]
struct Args {
    #[arg(long, env = "WEFT_HTTP_PORT", default_value_t = 9999)]
    http_port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_dispatcher=info,tower_http=debug".into()),
        )
        .init();

    let args = Args::parse();
    run(args.http_port).await
}
