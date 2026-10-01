//! `weft domain add|list|rm`: the domains the install answers at.
//!
//! A domain is bought anywhere. Adding one here stores it and prints the
//! DNS record to set at the registrar; the command then waits until the
//! name points at the install, after which the install's front door gets
//! its certificate on its own.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Context;
use weft_core::install::{Domain, DomainEntry, DomainServes};

use super::Ctx;

/// What a domain serves, as the flags spell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Serves {
    /// The install itself: its management API and every public door.
    Install,
    /// The cwd project's frontend (give where it runs with `--to`).
    Frontend,
    /// The cwd project's API: its routes, at the root of the domain.
    Api,
}

pub enum DomainAction {
    Add { name: String, serves: Serves, to: Option<String>, no_wait: bool },
    List,
    Rm { name: String },
}

/// How often the DNS is looked at while waiting for the record.
const DNS_LOOK_EVERY: Duration = Duration::from_secs(5);

pub async fn run(ctx: Ctx, action: DomainAction) -> anyhow::Result<()> {
    let client = ctx.client()?;
    match action {
        DomainAction::List => {
            let entries = client.get_json("/install/domains").await?;
            if ctx.json_out(&entries)? {
                return Ok(());
            }
            let entries: Vec<DomainEntry> = serde_json::from_value(entries).context("read the install's domains")?;
            if entries.is_empty() {
                println!("no domains; the install answers at its own address only (add one with `weft domain add <name>`)");
                return Ok(());
            }
            for e in entries {
                let what = match &e.domain.serves {
                    DomainServes::Frontend { project, upstream } => format!("frontend of {project} ({upstream})"),
                    DomainServes::Api { project } => format!("API of {project}"),
                    DomainServes::Install => "install".to_string(),
                };
                println!("{:<32} {what}", e.domain.name);
                println!("  DNS: {}", e.record);
            }
            Ok(())
        }
        DomainAction::Rm { name } => {
            client.delete(&format!("/install/domains/{name}")).await?;
            println!("removed {name}; you can delete its DNS record at your registrar");
            Ok(())
        }
        DomainAction::Add { name, serves, to, no_wait } => {
            let name = weft_core::install::normalize_domain_name(&name).map_err(anyhow::Error::msg)?;
            let project = || -> anyhow::Result<uuid::Uuid> { Ok(ctx.project()?.id()) };
            let serves = match (serves, to) {
                (Serves::Install, None) => DomainServes::Install,
                (Serves::Frontend, Some(upstream)) => DomainServes::Frontend { project: project()?, upstream },
                (Serves::Frontend, None) => anyhow::bail!(
                    "a frontend domain needs where the frontend runs: add `--to <its https address>` (the address its CI printed)"
                ),
                (Serves::Api, None) => DomainServes::Api { project: project()? },
                (_, Some(_)) => anyhow::bail!("--to is only for a frontend domain"),
            };
            let domain = Domain { name: name.clone(), serves };
            let answer = client.post_json("/install/domains", &serde_json::to_value(&domain)?).await?;
            if ctx.json_out(&answer)? {
                return Ok(());
            }
            let entry: DomainEntry = serde_json::from_value(answer).context("read the stored domain")?;
            println!("added {name}. At your domain's registrar, set this DNS record:");
            println!("  {}", entry.record);
            if no_wait {
                println!("the install gets the domain's certificate once the record is in place (`weft domain list` shows it again)");
                return Ok(());
            }
            let address: IpAddr = entry
                .record
                .value
                .parse()
                .map_err(|_| anyhow::anyhow!("the install answered '{}' as the record's address", entry.record.value))?;
            wait_for_dns(&name, address).await;
            println!("{name} points at the install; its certificate follows within a minute, then https://{name} works");
            Ok(())
        }
    }
}

/// Wait, for as long as it takes, until `name` resolves to `address`. DNS
/// changes can take hours to spread; Ctrl+C stops waiting and changes
/// nothing (the install keeps looking on its own).
async fn wait_for_dns(name: &str, address: IpAddr) {
    println!("waiting for {name} to point at {address} (this can take a few minutes, sometimes hours; Ctrl+C stops waiting, the install keeps looking)");
    let started = std::time::Instant::now();
    let mut said = started;
    loop {
        let seen: Vec<IpAddr> = match tokio::net::lookup_host((name, 443)).await {
            Ok(addrs) => addrs.map(|a| a.ip()).collect(),
            Err(_) => Vec::new(),
        };
        if seen.contains(&address) {
            return;
        }
        if said.elapsed() >= Duration::from_secs(60) {
            said = std::time::Instant::now();
            let now = if seen.is_empty() { "nothing yet".to_string() } else { seen.iter().map(IpAddr::to_string).collect::<Vec<_>>().join(", ") };
            println!("  still waiting after {} min; {name} resolves to {now}", started.elapsed().as_secs() / 60);
        }
        tokio::time::sleep(DNS_LOOK_EVERY).await;
    }
}
