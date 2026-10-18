import {expect, it} from 'vitest';
import {RECORDS, stream} from './stream';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('numbers the records of each shard on their own, and lets every consumer read the whole stream', () => {
  const {boxes, links} = stream(words);
  RECORDS.forEach((count, shard) => {
    const cells = boxes.filter((box) => box.id.startsWith(`s${shard}r`));
    expect(cells.map((cell) => cell.lines[0])).toEqual(
      Array.from({length: count}, (_, index) => String(index + 1)),
    );
  });
  expect(links.filter((link) => link.from === 'stream').map((link) => link.to)).toEqual([
    'first',
    'second',
  ]);
});
