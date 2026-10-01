import { describe, expect, it } from 'vitest';

import { onArgs } from './installs';

describe('onArgs', () => {
  it('names a target and leaves the local install to the default', () => {
    expect(onArgs('prod')).toEqual(['--on', 'prod']);
    expect(onArgs('local')).toEqual([]);
  });
});
