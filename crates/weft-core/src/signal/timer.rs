//! Scheduled fire. The listener spawns a tokio task that enqueues
//! a FireSignal broker task when the timer fires; a dispatcher
//! Pod claims it and runs the same dispatch path a stateless fire
//! takes. `After` and `At` are single-shot; `Cron` recurs until
//! torn down.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timer {
    pub spec: TimerSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimerSpec {
    After {
        duration_ms: u64,
    },
    At {
        when: chrono::DateTime<chrono::Utc>,
    },
    Cron {
        expression: String,
    },
}

impl Signal for Timer {
    const TAG: &'static str = "timer";

    fn validate(&self) -> Result<(), String> {
        match &self.spec {
            TimerSpec::Cron { expression } => cron::Schedule::from_str(expression)
                .map(|_| ())
                .map_err(|e| format!("invalid cron expression '{expression}': {e}")),
            TimerSpec::After { duration_ms } => {
                if *duration_ms == 0 {
                    return Err("timer.after.duration_ms must be > 0".into());
                }
                Ok(())
            }
            TimerSpec::At { when } => {
                if *when <= chrono::Utc::now() {
                    return Err(format!(
                        "timer.at must be in the future: got {}",
                        when.to_rfc3339()
                    ));
                }
                Ok(())
            }
        }
    }
}

crate::register_signal_kind!(Timer);
