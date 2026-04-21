//! CLI subcommand implementations. Each module is a verb; most are
//! thin wrappers over an HTTP call to the dispatcher.

pub mod new;
pub mod build;
pub mod run;
pub mod follow;
pub mod stop;
pub mod activate;
pub mod deactivate;
pub mod ps;
pub mod rm;
pub mod logs;
pub mod daemon;
pub mod infra;
pub mod add;
pub mod describe_nodes;
pub mod executions;
pub mod token;

pub struct Ctx {
    pub dispatcher: Option<String>,
}

impl Ctx {
    pub fn client(&self) -> crate::client::DispatcherClient {
        crate::client::DispatcherClient::new(
            crate::client::resolve_dispatcher_url(self.dispatcher.as_deref())
        )
    }
}
