//! The host port a local install keeps for each port of its infra units
//! that it publishes on the machine: a process unit's ports, and every
//! port of a container unit, which its agent publishes on loopback.
//!
//! weft picks the host port the first time the unit starts and keeps it
//! until the unit is removed or its copy terminated, so the address an
//! endpoint was given stays its address across restarts, stops and
//! upgrades: a container made again for a new version, or brought back by
//! Docker after the machine or Docker itself restarted, binds the same
//! port. A port picked on purpose is never one this machine hands out on
//! its own (`weft_core::ports::NAMED`), so nothing that merely connects
//! somewhere takes it between two starts, and never one any install on the
//! machine saved or keeps (`weft_core::ports::PortsTurn::free_ports`). A
//! process unit's port carried over from a weft before kept ports
//! (`crate::process_units`) is kept where it was, which may be in the
//! machine's own range: its address stays, at that risk.
//!
//! On disk, the install's `infra-ports.json` (`weft_core::ports::KeptInfraPort`):
//! every kept port, by copy, unit and port.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use weft_core::infra::NodeRef;
pub use weft_core::ports::UnitPort;
use weft_core::ports::KeptInfraPort;

/// Where new host ports come from: the machine's turn to hand them out,
/// held while a pick is made and saved.
pub trait PickPorts: Send + Sync {
    /// Wait for the turn; it is held until what is answered is dropped.
    fn turn(&self) -> anyhow::Result<Box<dyn Turn + '_>>;
}

/// The turn to hand out ports, held.
pub trait Turn {
    /// A port nothing on this machine listens on right now, none of
    /// `besides`.
    fn free_port(&self, besides: &[u16]) -> anyhow::Result<u16>;
}

/// The machine's own: ports from `weft_core::ports::NAMED`, in the
/// machine's turn to hand them out (`weft_core::ports::PortsTurn`), so
/// none is one an install saved or keeps.
pub struct MachinePorts;

impl PickPorts for MachinePorts {
    fn turn(&self) -> anyhow::Result<Box<dyn Turn + '_>> {
        Ok(Box::new(weft_core::ports::PortsTurn::take().map_err(anyhow::Error::msg)?))
    }
}

impl Turn for weft_core::ports::PortsTurn {
    fn free_port(&self, besides: &[u16]) -> anyhow::Result<u16> {
        let [port] = self.free_ports(besides).map_err(anyhow::Error::msg)?;
        Ok(port)
    }
}

/// What a start of `unit` of `node`'s copy fails with when `host`, the
/// port of this machine kept for the unit's `port`, is held by another
/// program: how to find what holds it, and how to let the unit move to
/// another port instead. The port stays kept either way.
pub fn taken_by_another_program(node: &NodeRef, unit: &str, port: &UnitPort, host: u16) -> String {
    let port = match port {
        UnitPort::Container(number) => number.to_string(),
        UnitPort::Process(name) => format!("'{name}'"),
    };
    format!(
        "unit '{unit}' of '{}' cannot start: port {host} of this machine, which weft keeps for its port {port}, \
         is taken by another program. Free it (`lsof -i :{host}` names what holds it), then start the infra \
         again (`weft infra start`). If you would rather the unit moved to another port, `weft infra terminate` \
         lets go of every port the infra keeps (and removes what it stored: its disks, all but those it keeps on \
         terminate, and its processes' folders), and its next start picks new ones, so every address of it changes",
        weft_core::project::plain_id(&node.node)
    )
}

fn is(k: &KeptInfraPort, node: &NodeRef, unit: &str) -> bool {
    k.copy_id == node.copy_id && k.unit == unit
}

/// The host ports one install keeps.
pub struct KeptPorts {
    /// The install's folder, where its `infra-ports.json` is.
    dir: PathBuf,
    pick: Arc<dyn PickPorts>,
    /// Held across every read and write of the file, so two calls of this
    /// install's never interleave theirs; another install never writes it.
    changing: parking_lot::Mutex<()>,
}

impl std::fmt::Debug for KeptPorts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeptPorts").field("dir", &self.dir).finish_non_exhaustive()
    }
}

impl KeptPorts {
    /// The ports the install whose folder is `dir` keeps, new ones picked
    /// from `pick`.
    pub fn new(dir: PathBuf, pick: Arc<dyn PickPorts>) -> Self {
        Self { dir, pick, changing: parking_lot::Mutex::new(()) }
    }

    fn load(&self) -> anyhow::Result<Vec<KeptInfraPort>> {
        KeptInfraPort::read(&self.dir).map_err(anyhow::Error::msg)
    }

    fn save(&self, all: &[KeptInfraPort]) -> anyhow::Result<()> {
        KeptInfraPort::save(&self.dir, all).map_err(|e| anyhow::anyhow!("save {}: {e}", KeptInfraPort::path(&self.dir).display()))
    }

    /// The host port for each of `ports` of `unit` of `node`'s copy: the
    /// one kept for it, or a new one picked for it and kept from now on,
    /// saved before the machine's turn is let go so no pick anywhere on
    /// the machine can take it meanwhile. A port the unit kept and no
    /// longer has (its spec dropped it) is let go.
    pub fn keep(&self, node: &NodeRef, unit: &str, ports: &[UnitPort]) -> anyhow::Result<BTreeMap<UnitPort, u16>> {
        let _changing = self.changing.lock();
        let all = self.load()?;
        let (mut mine, mut others): (Vec<KeptInfraPort>, Vec<KeptInfraPort>) = all.iter().cloned().partition(|k| is(k, node, unit));
        mine.retain(|k| ports.contains(&k.port));
        let missing: Vec<&UnitPort> = ports.iter().filter(|p| !mine.iter().any(|k| &k.port == *p)).collect();
        // Held until the picks are saved, below.
        let turn = if missing.is_empty() { None } else { Some(self.pick.turn()?) };
        for port in missing {
            let turn = turn.as_ref().expect("taken for every missing port");
            let besides: Vec<u16> = mine.iter().map(|k| k.host).collect();
            let host = turn.free_port(&besides)?;
            mine.push(KeptInfraPort { copy_id: node.copy_id.clone(), unit: unit.to_string(), port: port.clone(), host });
        }
        let out = mine.iter().map(|k| (k.port.clone(), k.host)).collect();
        others.extend(mine);
        if others != all {
            self.save(&others)?;
        }
        drop(turn);
        Ok(out)
    }

    /// Keep `ports` (each port and the host port it is at) for `unit` of
    /// `node`'s copy, as they are: ports an older weft picked and kept
    /// elsewhere, carried here.
    pub fn carry(&self, node: &NodeRef, unit: &str, ports: impl IntoIterator<Item = (UnitPort, u16)>) -> anyhow::Result<()> {
        let _changing = self.changing.lock();
        let mut all = self.load()?;
        for (port, host) in ports {
            all.retain(|k| !(is(k, node, unit) && k.port == port));
            all.push(KeptInfraPort { copy_id: node.copy_id.clone(), unit: unit.to_string(), port, host });
        }
        self.save(&all)
    }

    /// The host port kept for `port` of `unit` of `node`'s copy, if any.
    pub fn kept(&self, node: &NodeRef, unit: &str, port: &UnitPort) -> anyhow::Result<Option<u16>> {
        let _changing = self.changing.lock();
        Ok(self.load()?.into_iter().find(|k| is(k, node, unit) && &k.port == port).map(|k| k.host))
    }

    /// Let go of every port kept for `unit` of `node`'s copy.
    pub fn release_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        self.release(|k| is(k, node, unit))
    }

    /// Let go of every port kept for `node`'s copy.
    pub fn release_copy(&self, node: &NodeRef) -> anyhow::Result<()> {
        self.release(|k| k.copy_id == node.copy_id)
    }

    fn release(&self, gone: impl Fn(&KeptInfraPort) -> bool) -> anyhow::Result<()> {
        let _changing = self.changing.lock();
        let all = self.load()?;
        let kept: Vec<KeptInfraPort> = all.iter().filter(|k| !gone(k)).cloned().collect();
        if kept.len() != all.len() {
            self.save(&kept)?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    /// Ports handed out one after another from `first`, skipping
    /// `besides`, and every one handed out once, never again: a fake for
    /// a host whose ports nothing binds (a fake Docker).
    pub struct CountingPorts {
        next: parking_lot::Mutex<u16>,
        /// How many turns were taken.
        pub turns: std::sync::atomic::AtomicUsize,
    }

    impl CountingPorts {
        pub fn from(first: u16) -> Self {
            Self { next: parking_lot::Mutex::new(first), turns: Default::default() }
        }
    }

    impl PickPorts for CountingPorts {
        fn turn(&self) -> anyhow::Result<Box<dyn Turn + '_>> {
            self.turns.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::new(self))
        }
    }

    impl Turn for &CountingPorts {
        fn free_port(&self, besides: &[u16]) -> anyhow::Result<u16> {
            let mut next = self.next.lock();
            while besides.contains(&*next) {
                *next += 1;
            }
            *next += 1;
            Ok(*next - 1)
        }
    }

    /// A store in `dir`, its ports from [`CountingPorts`].
    pub fn counting_in(dir: &std::path::Path, first: u16) -> Arc<KeptPorts> {
        Arc::new(KeptPorts::new(dir.to_path_buf(), Arc::new(CountingPorts::from(first))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(copy: &str) -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(5), node: "db".into(), copy_id: copy.into() }
    }

    /// A port is picked once, in a turn of its own, and kept; a port the
    /// unit no longer has is let go; a release lets go of the unit's or the whole copy's.
    #[test]
    fn a_port_is_picked_once_and_kept_until_let_go() {
        let dir = tempfile::tempdir().unwrap();
        let pick = Arc::new(fake::CountingPorts::from(20000));
        let kept = KeptPorts::new(dir.path().to_path_buf(), pick.clone());
        let turns = || pick.turns.load(std::sync::atomic::Ordering::SeqCst);
        let (a, b) = (node("wn-a"), node("wn-b"));
        let pg = UnitPort::Container(5432);
        let first = kept.keep(&a, "main", std::slice::from_ref(&pg)).unwrap();
        assert_eq!((first[&pg], turns()), (20000, 1));
        assert_eq!(kept.keep(&a, "main", std::slice::from_ref(&pg)).unwrap(), first, "kept");
        assert_eq!(turns(), 1, "nothing to pick, so no turn");
        // A store opened again on the same folder, as the next life of the
        // runtime does, keeps it too.
        let again = KeptPorts::new(dir.path().to_path_buf(), Arc::new(fake::CountingPorts::from(20000)));
        assert_eq!(again.kept(&a, "main", &pg).unwrap(), Some(20000));
        let other = kept.keep(&b, "main", std::slice::from_ref(&pg)).unwrap();

        let http = UnitPort::Process("http".into());
        again.keep(&a, "main", std::slice::from_ref(&http)).unwrap();
        assert_eq!(again.kept(&a, "main", &pg).unwrap(), None, "a port the unit dropped is let go");
        again.keep(&a, "side", std::slice::from_ref(&pg)).unwrap();
        again.release_unit(&a, "main").unwrap();
        assert_eq!(again.kept(&a, "main", &http).unwrap(), None);
        assert!(again.kept(&a, "side", &pg).unwrap().is_some(), "another unit of the copy keeps its own");
        again.release_copy(&a).unwrap();
        assert_eq!(again.kept(&a, "side", &pg).unwrap(), None);
        assert_eq!(again.kept(&b, "main", &pg).unwrap(), Some(other[&pg]), "another copy keeps its own");
    }

    /// A pick is saved while the machine's turn is still held: what the
    /// next holder of the turn reads already has it.
    #[test]
    fn a_pick_is_saved_before_the_turn_is_let_go() {
        struct Watching {
            dir: PathBuf,
            seen: parking_lot::Mutex<Vec<Vec<KeptInfraPort>>>,
        }
        struct Held<'a>(&'a Watching);
        impl Turn for Held<'_> {
            fn free_port(&self, _besides: &[u16]) -> anyhow::Result<u16> {
                Ok(20000)
            }
        }
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                self.0.seen.lock().push(KeptInfraPort::read(&self.0.dir).unwrap());
            }
        }
        impl PickPorts for Watching {
            fn turn(&self) -> anyhow::Result<Box<dyn Turn + '_>> {
                Ok(Box::new(Held(self)))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let pick = Arc::new(Watching { dir: dir.path().to_path_buf(), seen: Default::default() });
        KeptPorts::new(dir.path().to_path_buf(), pick.clone()).keep(&node("wn-a"), "main", &[UnitPort::Container(5432)]).unwrap();
        let seen = pick.seen.lock();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].iter().map(|k| k.host).collect::<Vec<_>>(), [20000], "saved when the turn went");
    }

    #[test]
    fn a_file_weft_cannot_read_says_what_to_do() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(KeptInfraPort::path(dir.path()), "{").unwrap();
        let err = fake::counting_in(dir.path(), 20000).kept(&node("wn-a"), "main", &UnitPort::Container(1)).unwrap_err();
        assert!(err.to_string().contains("delete it"), "{err}");
    }
}
