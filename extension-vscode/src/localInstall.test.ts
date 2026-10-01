import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { DEFAULT_LOCAL_URL, RETIRED_DEFAULT_URL, installDir, localAddress, localInstallUrl } from './localInstall';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'weft-local-install-'));
const root = path.join(home, '.local', 'share', 'weft');

afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

function save(dir: string, contents: string) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'ports.json'), contents);
}

describe('localInstallUrl', () => {
  it('is the default port until the default install saved one', () => {
    expect(localInstallUrl({ HOME: home })).toBe(DEFAULT_LOCAL_URL);
    save(root, '{"public":15000,"internal":14113,"outside":14112,"postgres":14114}');
    expect(localInstallUrl({ HOME: home })).toBe('http://127.0.0.1:15000');
  });

  it('reads a named install from its own folder and refuses one never started', () => {
    const env = { HOME: home, WEFT_INSTALL: 'cell1' };
    expect(installDir(env)).toBe(path.join(root, 'installs', 'cell1'));
    expect(() => localInstallUrl(env)).toThrow('weft daemon start');
    save(path.join(root, 'installs', 'cell1'), '{"public":15100}');
    expect(localInstallUrl(env)).toBe('http://127.0.0.1:15100');
  });

  it('fails loudly on a file it cannot read', () => {
    save(root, '{"public":"x"}');
    expect(() => localInstallUrl({ HOME: home })).toThrow('is not valid');
  });
});

describe('localAddress', () => {
  it('takes a set address over the saved port', () => {
    save(root, '{"public":15000}');
    expect(localAddress('http://10.0.0.2:9000', { HOME: home })).toEqual({ url: 'http://10.0.0.2:9000', ignoredSetting: false });
  });

  it('passes over the retired default and says so', () => {
    save(root, '{"public":15000}');
    expect(localAddress(RETIRED_DEFAULT_URL, { HOME: home })).toEqual({ url: 'http://127.0.0.1:15000', ignoredSetting: true });
  });

  it('returns the reason instead of throwing when there is no address', () => {
    const got = localAddress(undefined, { HOME: home, WEFT_INSTALL: 'cell1' });
    expect('error' in got && got.error).toContain('weft daemon start');
  });
});
