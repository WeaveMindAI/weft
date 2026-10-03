//! How many holders run: the copies of the listener that keep the
//! outside connections some signals need open between fires.
//!
//! A held connection must live somewhere that stays up, and that costs
//! money for as long as it runs. So holders run only while there is
//! something to hold, and as many as the held signals need: none when no
//! signal holds a connection. The dispatcher, which registers and drops
//! signals, sets the count; each holder claims what it holds through a
//! lease, so they share the work and take over from one that went.

use async_trait::async_trait;

#[async_trait]
pub trait HolderPool: Send + Sync {
    /// Run `copies` holders; 0 stops every one. Setting the count it
    /// already has changes nothing.
    async fn resize(&self, copies: u32) -> anyhow::Result<()>;
}

/// The holders a count of held signals needs: none for none, then one
/// per `per_copy` signals, rounded up.
pub fn copies_for(held: u64, per_copy: u32) -> u32 {
    let per_copy = u64::from(per_copy.max(1));
    u32::try_from(held.div_ceil(per_copy)).unwrap_or(u32::MAX)
}

/// Records every resize.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    #[derive(Default)]
    pub struct FakeHolderPool {
        pub sizes: parking_lot::Mutex<Vec<u32>>,
    }

    #[async_trait]
    impl HolderPool for FakeHolderPool {
        async fn resize(&self, copies: u32) -> anyhow::Result<()> {
            self.sizes.lock().push(copies);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_held_signal_runs_no_holder_and_more_run_more() {
        assert_eq!(copies_for(0, 200), 0);
        assert_eq!(copies_for(1, 200), 1);
        assert_eq!(copies_for(200, 200), 1);
        assert_eq!(copies_for(201, 200), 2);
        assert_eq!(copies_for(5, 0), 5, "a zero capacity is read as one per copy, never a division by zero");
    }
}
