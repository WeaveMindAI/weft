// Where the local install answers on this machine when `weft.dispatcherUrl`
// is unset: the public port it saved in its `ports.json` when it started,
// read the way the CLI reads it, so a port moved with `WEFT_PUBLIC_PORT`
// is found here too.

import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

// The default install's address before its first start.
// SYNC: 14111 <-> crates/weft-core/src/ports.rs (PUBLIC, LOCAL_PUBLIC_URL),
// setup.sh (weft_local_url), deploy/terraform/gcp/network.tf,
// extension-browser/src/entrypoints/popup/App.svelte
export const DEFAULT_LOCAL_URL = 'http://127.0.0.1:14111';

/// The folder an install keeps its files in: weft's data folder for the
/// default install, `installs/<name>` under it for the one `WEFT_INSTALL`
/// names.
// SYNC: <-> crates/weft-core/src/infra/install.rs (data_dir, Install::dir, Install::from_env)
export function installDir(env: NodeJS.ProcessEnv = process.env, home: string = os.homedir()): string {
  const root = path.join(env.HOME ?? home, '.local', 'share', 'weft');
  const name = env.WEFT_INSTALL?.trim();
  return name ? path.join(root, 'installs', name) : root;
}

/// The local install's address. A default install never started takes
/// the default port on its first start; a named one never started has no
/// address, and pointing at the default install instead would talk to the
/// wrong one, so that throws.
// SYNC: <-> crates/weft-core/src/ports.rs (InstallPorts, local_public_url_in)
export function localInstallUrl(env: NodeJS.ProcessEnv = process.env, home: string = os.homedir()): string {
  const dir = installDir(env, home);
  const file = path.join(dir, 'ports.json');
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error(`cannot read ${file}: ${e}`);
    const name = env.WEFT_INSTALL?.trim();
    if (!name) return DEFAULT_LOCAL_URL;
    throw new Error(`install '${name}' has no ports yet (${file} does not exist); start it with \`weft daemon start\``);
  }
  let port: unknown;
  try {
    port = (JSON.parse(raw) as { public?: unknown } | null)?.public;
  } catch (e) {
    throw new Error(`${file} is not valid: ${e}`);
  }
  if (typeof port !== 'number' || !Number.isInteger(port)) throw new Error(`${file} is not valid: no numeric \`public\` port`);
  return `http://127.0.0.1:${port}`;
}

// What `weft.dispatcherUrl` defaulted to before the extension read
// `ports.json`. VS Code wrote it into the settings of anybody who edited
// the field, and there it would beat the saved port while pointing at an
// address no install answers on, so it counts as unset.
export const RETIRED_DEFAULT_URL = 'http://localhost:9999';

/// Where the local install is, from the `weft.dispatcherUrl` setting and
/// the install's `ports.json`. `ignoredSetting` says the setting held the
/// retired default and was passed over.
export type LocalAddress =
  | { url: string; ignoredSetting: boolean }
  | { error: string; ignoredSetting: boolean };

export function localAddress(
  setting: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
  home: string = os.homedir(),
): LocalAddress {
  const ignoredSetting = setting === RETIRED_DEFAULT_URL;
  if (setting && !ignoredSetting) return { url: setting, ignoredSetting };
  try {
    return { url: localInstallUrl(env, home), ignoredSetting };
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e), ignoredSetting };
  }
}

/// What to check when the local install does not answer: the two places
/// its address comes from.
export function unreachableHint(setting: string | undefined, env: NodeJS.ProcessEnv = process.env, home: string = os.homedir()): string {
  const file = path.join(installDir(env, home), 'ports.json');
  return setting && setting !== RETIRED_DEFAULT_URL
    ? `the address comes from the \`weft.dispatcherUrl\` setting (${setting}); clear it to use ${file}`
    : `the address comes from ${file}; is the install started (\`weft daemon start\`), or should \`weft.dispatcherUrl\` point elsewhere?`;
}
