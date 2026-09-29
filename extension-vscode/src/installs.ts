// The installs a project can be looked at on: the local one, and each
// target its `weft.toml` names (`[targets.<name>]`). The editor's install
// switch reads them here, and everything it needs from a target (its
// address, this person's key for it, the program it holds) comes through
// the CLI, which owns `weft.toml` and `~/.config/weft/credentials.toml`,
// so the editor never reads either on its own.

import * as fs from 'node:fs/promises';
import * as path from 'node:path';

import { LOCAL_INSTALL } from '../../packages/weft-graph/src/protocol';
import { runWeftJson } from './cli';

/// One row of `weft target list --json`.
// SYNC: InstallTarget <-> crates/weft-cli/src/commands/target.rs (TargetAction::List)
export interface InstallTarget {
  name: string;
  url: string;
  loggedIn: boolean;
}

/// What `weft --on <name> target key` answers: where requests go and the
/// key they carry.
// SYNC: InstallAccess <-> crates/weft-cli/src/commands/target.rs key
export interface InstallAccess {
  url: string;
  operatorKey: string | null;
}

/// The program an install holds, written into a folder by
/// `weft running-source`.
// SYNC: RunningSource <-> crates/weft-cli/src/commands/running_source.rs run
export interface RunningSource {
  version: string;
  dir: string;
}

/// The `--on` a command for `install` carries: none for the local one.
export function onArgs(install: string): string[] {
  return install === LOCAL_INSTALL ? [] : ['--on', install];
}

/// The project's installs, the local one first.
export function listTargets(projectRoot: string): Promise<InstallTarget[]> {
  return runWeftJson<InstallTarget[]>(['target', 'list', '--json'], projectRoot);
}

/// Where the editor reaches `install` and the key it carries there.
export function installAccess(projectRoot: string, install: string): Promise<InstallAccess> {
  return runWeftJson<InstallAccess>([...onArgs(install), 'target', 'key'], projectRoot);
}

/// Write the program `install` holds into a fresh folder under `cacheRoot`
/// (one per project and install), with the project's own canvas layouts
/// beside it so the graph keeps the positions the person gave it. The
/// previous copies of that install go once the new one is whole.
export async function fetchRunningSource(
  projectRoot: string,
  projectId: string,
  install: string,
  cacheRoot: string,
): Promise<RunningSource> {
  const parent = path.join(cacheRoot, 'installs', projectId, install);
  await fs.mkdir(parent, { recursive: true });
  const dir = path.join(parent, String(Date.now()));
  const running = await runWeftJson<RunningSource>(
    [...onArgs(install), 'running-source', dir, '--json'],
    projectRoot,
  );
  await copyLayouts(projectRoot, running.dir);
  // The copy is what the install runs, never something to edit: a text
  // tab of it cannot be saved over.
  await setFilesWritable(running.dir, false);
  for (const old of await fs.readdir(parent)) {
    const oldDir = path.join(parent, old);
    if (oldDir !== running.dir) {
      // Some platforms refuse to delete a read-only file.
      await setFilesWritable(oldDir, true);
      await fs.rm(oldDir, { recursive: true, force: true });
    }
  }
  return running;
}

/// Make every file under `dir` writable by its owner, or read-only for
/// everyone. Folders keep their mode, so files can still be added and
/// removed.
async function setFilesWritable(dir: string, writable: boolean): Promise<void> {
  for (const entry of await fs.readdir(dir, { withFileTypes: true, recursive: true })) {
    if (!entry.isFile()) continue;
    await fs.chmod(path.join(entry.parentPath, entry.name), writable ? 0o644 : 0o444);
  }
}

/// `layouts/` is not part of a version (a canvas drag is not a program
/// change), so the copy has none; the local ones are the person's.
async function copyLayouts(projectRoot: string, dir: string): Promise<void> {
  const from = path.join(projectRoot, 'layouts');
  try {
    await fs.cp(from, path.join(dir, 'layouts'), { recursive: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
  }
}
