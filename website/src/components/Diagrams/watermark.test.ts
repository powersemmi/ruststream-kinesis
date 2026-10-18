import {describe, expect, it} from 'vitest';
import {DELIVERED, checkpointAt, watermark} from './watermark';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe("a shard's checkpoint", () => {
  it('stops at the last record of the settled run from the front, whatever settled after it', () => {
    expect(checkpointAt(DELIVERED)).toBe(2);
  });

  it('reaches the last delivered record once nothing is outstanding', () => {
    const settled = DELIVERED.map((delivery) => ({...delivery, settled: true}));
    expect(checkpointAt(settled)).toBe(6);
  });

  it('is not written while the first delivery is still running', () => {
    const first = DELIVERED.map((delivery, index) => ({...delivery, settled: index !== 0}));
    expect(checkpointAt(first)).toBeUndefined();
  });

  it('is drawn pointing at that record', () => {
    const {links} = watermark(words);
    expect(links.find((link) => link.from === 'checkpoint' && link.to.startsWith('r'))?.to).toBe(
      'r2',
    );
  });
});
