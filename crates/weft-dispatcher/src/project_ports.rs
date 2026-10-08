//! A local project's own port: where its front worker answers its routes
//! at the root of an address of its own (`http://127.0.0.1:14200/hello`),
//! with no `/connect/<tenant>` in front and nothing of weft's in between
//! (`crate::front`, `weft_platform_local`'s runner publishes it).
//!
//! A project keeps the port it was given (`project.api_port`), so its
//! address stays put, whether weft picked it or the person did (`weft
//! activate --port`). When another program on the machine took it, the
//! project moves to a free one. A port the person names is checked before
//! anything is activated ([`ProjectPorts::check_asked`]): taken, the
//! activation is refused and nothing changes. Ports come from the
//! install's block (`projectPorts` in the config), or, with no block, are
//! any the machine has free.

use std::net::{IpAddr, SocketAddr};

use weft_platform_traits::config::PortRange;

/// Where the install's project ports come from.
pub struct ProjectPorts {
    /// The address every project port opens on: the public port's.
    ip: IpAddr,
    /// The block ports are taken from; `None`, free ones the machine picks.
    block: Option<PortRange>,
}

impl ProjectPorts {
    pub fn new(public: SocketAddr, block: Option<PortRange>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { ip: public.ip(), block })
    }

    /// Why `port`, named by the person for `project`, cannot be its
    /// address right now: another project's, or held by another program.
    /// `None` when it is free. Asked before anything is activated, so a
    /// refusal changes nothing.
    pub async fn check_asked(&self, pool: &sqlx::PgPool, project: uuid::Uuid, port: u16) -> anyhow::Result<Option<String>> {
        let owner: Option<uuid::Uuid> = sqlx::query_scalar("SELECT id FROM project WHERE api_port = $1 AND id <> $2")
            .bind(i32::from(port))
            .bind(project)
            .fetch_optional(pool)
            .await?;
        if let Some(owner) = owner {
            return Ok(Some(format!("port {port} is the own address of project {owner}")));
        }
        // The project's own front, while it is up, holds the port it has;
        // a front that went (its address cleared) holds nothing, and the
        // port is checked like any other.
        let ours: Option<i32> = sqlx::query_scalar("SELECT api_port FROM project WHERE id = $1 AND api_address IS NOT NULL")
            .bind(project)
            .fetch_optional(pool)
            .await?
            .flatten();
        if ours == Some(i32::from(port)) {
            return Ok(None);
        }
        Ok(match tokio::net::TcpListener::bind(SocketAddr::new(self.ip, port)).await {
            Ok(_) => None,
            Err(e) => Some(format!("port {port} is held by another program ({e})")),
        })
    }

    /// The port the project's front opens on: `asked` when the person named
    /// one, else the one it has, else a free one, kept on its row.
    pub async fn port_of(&self, pool: &sqlx::PgPool, project: uuid::Uuid, asked: Option<u16>) -> anyhow::Result<u16> {
        if let Some(port) = asked {
            return match keep(pool, project, port).await? {
                Kept::Yes => Ok(port),
                Kept::OtherProjects => anyhow::bail!("port {port} is another project's own address"),
            };
        }
        let given: Option<Option<i32>> = sqlx::query_scalar("SELECT api_port FROM project WHERE id = $1")
            .bind(project)
            .fetch_optional(pool)
            .await?;
        match given.flatten() {
            Some(port) => u16::try_from(port).map_err(|_| anyhow::anyhow!("project {project}'s port {port} is no port")),
            None => self.free_port(pool, project, None).await,
        }
    }

    /// A free port for the project in place of `taken`, which another
    /// program holds, kept on its row.
    pub async fn move_from(&self, pool: &sqlx::PgPool, project: uuid::Uuid, taken: u16) -> anyhow::Result<u16> {
        self.free_port(pool, project, Some(taken)).await
    }

    /// The first port no project has and no program on the machine holds
    /// right now (`but` aside), kept on the project's row.
    async fn free_port(&self, pool: &sqlx::PgPool, project: uuid::Uuid, but: Option<u16>) -> anyhow::Result<u16> {
        let taken: Vec<i32> = sqlx::query_scalar("SELECT api_port FROM project WHERE api_port IS NOT NULL AND id <> $1")
            .bind(project)
            .fetch_all(pool)
            .await?;
        // With no block, the machine picks: the port it gives may be one a
        // project keeps while its front is down (its number stays on its
        // row), so it is asked again, at most once per such project.
        let candidates: Vec<u16> = match self.block {
            Some(block) => block.ports().filter(|p| !taken.contains(&i32::from(*p)) && Some(*p) != but).collect(),
            None => vec![0; taken.len() + 1],
        };
        for candidate in candidates {
            // The probe lets go at once; the project's front opens it.
            let Ok(probe) = tokio::net::TcpListener::bind(SocketAddr::new(self.ip, candidate)).await else { continue };
            let port = probe.local_addr()?.port();
            drop(probe);
            // Another process of the install may have given it to another
            // project meanwhile: the next one is tried.
            if Some(port) != but && keep(pool, project, port).await? == Kept::Yes {
                return Ok(port);
            }
        }
        match self.block {
            Some(b) => anyhow::bail!(
                "every project port of this install ({}-{}) is taken; remove a project, or widen `projectPorts` in the install's config",
                b.first,
                b.last
            ),
            None => anyhow::bail!("every free port the machine gave project {project} is another project's; activate again"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Kept {
    Yes,
    /// Another project has it (`idx_project_api_port`).
    OtherProjects,
}

async fn keep(pool: &sqlx::PgPool, project: uuid::Uuid, port: u16) -> anyhow::Result<Kept> {
    let kept = sqlx::query("UPDATE project SET api_port = $2 WHERE id = $1")
        .bind(project)
        .bind(i32::from(port))
        .execute(pool)
        .await;
    match kept {
        Ok(done) => {
            anyhow::ensure!(done.rows_affected() == 1, "project {project} is gone");
            Ok(Kept::Yes)
        }
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Ok(Kept::OtherProjects),
        Err(e) => Err(e.into()),
    }
}
