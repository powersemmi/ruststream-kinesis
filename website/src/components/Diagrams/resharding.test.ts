import {describe, expect, it} from 'vitest';
import {PARENTS, readable, resharding} from './resharding';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('a child shard', () => {
  it('is drawn with an arrow from each of its parents and from nothing else', () => {
    const {links} = resharding(words);
    for (const [child, parents] of Object.entries(PARENTS)) {
      const into = links.filter((link) => link.to === child).map((link) => link.from);
      expect(into.sort(), child).toEqual([...parents].sort());
    }
  });

  it('is read only once every parent is read to its end', () => {
    expect(readable('s1', new Set())).toBe(false);
    expect(readable('s1', new Set(['s0']))).toBe(true);
    // A merge has two parents, and one finished parent is not enough.
    expect(readable('s3', new Set(['s0', 's1']))).toBe(false);
    expect(readable('s3', new Set(['s0', 's1', 's2']))).toBe(true);
  });

  it('has no parent when the stream was created with it', () => {
    expect(readable('s0', new Set())).toBe(true);
  });
});
