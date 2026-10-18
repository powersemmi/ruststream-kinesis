import {describe, expect, it} from 'vitest';
import type {Box} from './flow';
import {LAYOUTS, type DiagramName, type Layout} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The drawing area every diagram is laid out on: phone-first, 360 units at 390 px.
const WIDTH = 360;

const layout = (name: DiagramName): Layout => {
  const make: (words: Words) => Layout = LAYOUTS[name];
  return make(words);
};

const overlap = (a: Box, b: Box): boolean =>
  a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

const names = Object.keys(LAYOUTS) as DiagramName[];

describe('the diagrams of the Architecture pages', () => {
  it('are the ones the pages name', () => {
    expect([...names].sort()).toEqual(
      [
        'stream',
        'resharding',
        'iterator',
        'leases',
        'design',
        'lifecycle',
        'path',
        'watermark',
        'retries',
        'lanes',
      ].sort(),
    );
  });

  it.each(names)('%s fits the drawing area', (name) => {
    for (const box of layout(name).boxes) {
      expect(box.x, `${name}, ${box.id}`).toBeGreaterThanOrEqual(0);
      expect(box.y, `${name}, ${box.id}`).toBeGreaterThanOrEqual(0);
      expect(box.x + box.w, `${name}, ${box.id}`).toBeLessThanOrEqual(WIDTH);
    }
  });

  it.each(names)('%s draws no box over another', (name) => {
    // A frame holds other boxes by design; every other box stands on its own.
    const boxes = layout(name).boxes.filter((box) => box.kind !== 'group');
    for (const [index, a] of boxes.entries()) {
      for (const b of boxes.slice(index + 1)) {
        expect(overlap(a, b), `${name}: ${a.id} and ${b.id}`).toBe(false);
      }
    }
  });

  it.each(names)('%s links only boxes it draws, each box named once', (name) => {
    const {boxes, links} = layout(name);
    const ids = boxes.map((box) => box.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const link of links) {
      expect(ids, `${name}: ${link.from} → ${link.to}`).toContain(link.from);
      expect(ids, `${name}: ${link.from} → ${link.to}`).toContain(link.to);
    }
  });
});
