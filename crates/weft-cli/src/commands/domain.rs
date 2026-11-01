//! `weft domain add|list|rm`: the domains the install answers at.
//!
//! A domain is bought anywhere. The install answers at its own address
//! without one; a domain needs a door in front of the install, which the
//! first domain makes (on GCP a load balancer, which costs money while it
//! stands, so the first add needs `--accept-cost`) and the last one takes
//! down. Adding one stores it and prints the DNS record to set at the
//! registrar; the command then waits until the name points at the door,
//! which then gets the name's certificate on its own, and until that
//! certificate is issued. From then on an install domain is the address
//! the install hands out in its links, its OAuth callback included.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Context;
use weft_core::install::{AddDomain, Certificate, Domain, DomainAdded, DomainList, DomainServes};

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
    Add { name: String, serves: Serves, to: Option<String>, no_wait: bool, accept_cost: bool },
    List,
    Rm { name: String },
}

/// How often the DNS is looked at while waiting for the record.
const DNS_LOOK_EVERY: Duration = Duration::from_secs(5);

/// How often the install is asked after the certificate while waiting
/// for it (each ask is a call to the platform).
const CERTIFICATE_LOOK_EVERY: Duration = Duration::from_secs(20);

pub async fn run(ctx: Ctx, action: DomainAction) -> anyhow::Result<()> {
    let client = ctx.client()?;
    match action {
        DomainAction::List => {
            let entries = client.get_json("/install/domains").await?;
            if ctx.json_out(&entries)? {
                return Ok(());
            }
            let list: DomainList = serde_json::from_value(entries).context("read the install's domains")?;
            if let Some(why) = &list.door_refused {
                eprintln!("the door in front of the install's domains refuses to follow them, and the install keeps trying: {why}");
            }
            if let Some(why) = &list.certificates_unread {
                eprintln!("the certificates could not be asked after just now, so each one below is as last recorded: {why}");
            }
            if list.domains.is_empty() {
                println!("no domains; the install answers at its own address only (add one with `{}`)", ctx.weft("domain add <name>"));
                return Ok(());
            }
            for e in &list.domains {
                let what = match &e.domain.serves {
                    DomainServes::Frontend { project, upstream } => format!("frontend of {project} ({upstream})"),
                    DomainServes::Api { project } => format!("API of {project}"),
                    DomainServes::Install => "install".to_string(),
                };
                println!("{:<32} {what}", e.domain.name);
                println!("  DNS: {}", e.record);
                println!("  certificate: {}", certificate_line(&e.certificate));
            }
            print_address(&ctx, &list);
            Ok(())
        }
        DomainAction::Rm { name } => {
            // On a cloud install it answers once the load balancer let go of it.
            client.waiting_on_its_work().delete(&format!("/install/domains/{name}")).await?;
            println!("removed {name}; you can delete its DNS record at your registrar (the door in front of the domains goes with the last one)");
            Ok(())
        }
        DomainAction::Add { name, serves, to, no_wait, accept_cost } => {
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
            // On a cloud install it answers once the load balancer serves it.
            let answer = client.waiting_on_its_work().post_json("/install/domains", &serde_json::to_value(AddDomain { domain, accept_cost })?).await?;
            if ctx.json_out(&answer)? {
                return Ok(());
            }
            let entry: DomainAdded = serde_json::from_value(answer).context("read the stored domain")?;
            println!("added {name}. At your domain's registrar, set this DNS record:");
            println!("  {}", entry.record);
            if let Some(cost) = &entry.cost {
                println!("the door in front of the install's domains is {cost}; removing the last domain takes it down");
            }
            if no_wait {
                println!("the door gets the domain's certificate once the record is in place (`{}` shows it again)", ctx.weft("domain list"));
                return Ok(());
            }
            let address: IpAddr = entry
                .record
                .value
                .parse()
                .map_err(|_| anyhow::anyhow!("the install answered '{}' as the record's address", entry.record.value))?;
            wait_for_dns(&name, address).await;
            println!("{name} points at the install; waiting for its certificate, which Google issues on its own (usually minutes; Ctrl+C stops waiting, the install keeps looking)");
            let list = wait_for_certificate(&client, &name).await?;
            match list.domains.iter().find(|e| e.domain.name == name).map(|e| &e.certificate) {
                Some(Certificate::Failed { why }) => {
                    anyhow::bail!(
                        "{name}'s certificate could not be issued: {why}. Fix that, then `{}` and add it again",
                        ctx.weft(&format!("domain rm {name}"))
                    )
                }
                _ => println!("{name}'s certificate is issued: https://{name} works"),
            }
            print_address(&ctx, &list);
            Ok(())
        }
    }
}

/// Who asks after a waiting certificate: the install, for a while, and
/// every `weft domain list`.
// SYNC: CHECKED_HOW <-> crates/weft-dispatcher/src/domains.rs CERTIFICATE_LOOKS_FOR
const CHECKED_HOW: &str = " (the install asks Google on its own for 3 days after the domain is added, and `weft domain list` asks again)";

/// How a certificate's state reads in `weft domain list`.
fn certificate_line(certificate: &Certificate) -> String {
    match certificate {
        Certificate::Issued => "issued".to_string(),
        Certificate::Pending { why: None } => format!("waiting (Google checks the DNS record, then issues it){CHECKED_HOW}"),
        Certificate::Pending { why: Some(why) } => format!("waiting: {why}{CHECKED_HOW}"),
        Certificate::Failed { why } => format!("failed: {why} (remove the domain and add it again to retry)"),
    }
}

/// What the install's address is now and, once an install domain is it,
/// what the person has to do about it: tell each sign-in provider the new
/// callback, and hand their site the new address.
fn print_address(ctx: &Ctx, list: &DomainList) {
    println!("links the install hands out use {}", list.live_address);
    let moved = list
        .domains
        .iter()
        .any(|e| e.domain.serves == DomainServes::Install && e.certificate == Certificate::Issued && list.live_address == format!("https://{}", e.domain.name));
    if !moved {
        return;
    }
    if !list.callback_services.is_empty() {
        println!(
            "sign-ins now come back to {}: add it to the allowed redirect URLs of your app at {}",
            list.callback_url,
            list.callback_services.join(", ")
        );
    }
    println!("if a site uses this install, export its settings again: `{}`", ctx.weft("target export <target>"));
}

/// Ask the install after `name`'s certificate until it is no longer
/// pending, and answer the list that says so.
async fn wait_for_certificate(client: &crate::client::DispatcherClient, name: &str) -> anyhow::Result<DomainList> {
    let started = std::time::Instant::now();
    let mut said = started;
    loop {
        let list: DomainList = serde_json::from_value(client.get_json("/install/domains").await?).context("read the install's domains")?;
        let Some(entry) = list.domains.iter().find(|e| e.domain.name == name) else {
            anyhow::bail!("'{name}' is no longer one of the install's domains");
        };
        if !matches!(entry.certificate, Certificate::Pending { .. }) {
            return Ok(list);
        }
        if said.elapsed() >= Duration::from_secs(120) {
            said = std::time::Instant::now();
            println!("  still waiting after {} min; {}", started.elapsed().as_secs() / 60, certificate_line(&entry.certificate));
        }
        tokio::time::sleep(CERTIFICATE_LOOK_EVERY).await;
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
