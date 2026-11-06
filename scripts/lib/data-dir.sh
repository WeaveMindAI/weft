# The folder weft keeps its files in on this machine: the default
# install at its root, each named install (a branch's, a test cell)
# under installs/<name>, and what every install shares (the run journal,
# the downloads, the machine-wide locks, the tools weft installs itself
# under needs/) beside them.
# SYNC: weft_state_dir <-> crates/weft-core/src/infra/install.rs (data_dir, Install::dir), extension-vscode/src/localInstall.ts (dataDir)
weft_state_dir="${HOME:?is not set: weft keeps the files of every install under \$HOME/.local/share/weft}/.local/share/weft"
