import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The records each shard holds in the drawing: shards fill apart, so their sequences do.
export const RECORDS = [5, 3, 4] as const;

const CELL = 40;
const STEP = 48;

/*
 * A stream: producers put records with a partition key, the key's hash picks one of the shards,
 * each shard keeps its records in order, and every consumer reads every shard for itself.
 */
export function stream(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'producers', x: 90, y: 0, w: 180, h: 44, kind: 'service', lines: lines(words, 'producers')},
    {id: 'stream', x: 0, y: 72, w: 360, h: 150, kind: 'group', lines: lines(words, 'stream')},
  ];
  RECORDS.forEach((count, shard) => {
    const y = 98 + shard * 40;
    boxes.push({
      id: `s${shard}`,
      x: 12,
      y,
      w: 56,
      h: 30,
      kind: 'broker',
      lines: lines(words, `s${shard}`),
    });
    for (let record = 0; record < count; record += 1) {
      boxes.push({
        id: `s${shard}r${record}`,
        x: 78 + record * STEP,
        y,
        w: CELL,
        h: 30,
        kind: 'step',
        lines: [String(record + 1)],
      });
    }
  });
  boxes.push(
    {id: 'first', x: 0, y: 256, w: 172, h: 62, kind: 'core', lines: lines(words, 'first')},
    {id: 'second', x: 188, y: 256, w: 172, h: 62, kind: 'core', lines: lines(words, 'second')},
    {id: 'retention', x: 0, y: 334, w: 360, h: 44, kind: 'note', lines: lines(words, 'retention')},
  );
  const links: Link[] = [
    {from: 'producers', to: 'stream', label: word(words, 'puts')},
    {from: 'stream', to: 'first', exitAt: 86 / 360, label: word(words, 'reads')},
    {from: 'stream', to: 'second', exitAt: 274 / 360, label: word(words, 'reads')},
  ];
  return {boxes, links};
}
