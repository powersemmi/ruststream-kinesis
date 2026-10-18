import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Leases: one row per shard in a table both instances write to. Each instance renews the rows
 * it holds; a row nobody renews lapses, and the other instance takes it with a conditional
 * write and resumes after the checkpoint the row holds.
 */
export function leases(words: Words): {boxes: Box[]; links: Link[]} {
  const row = (id: string, y: number): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind: 'core',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {id: 'a', x: 0, y: 0, w: 172, h: 58, kind: 'service', lines: lines(words, 'a')},
    {id: 'table', x: 0, y: 110, w: 360, h: 132, kind: 'group', lines: lines(words, 'table')},
    row('row0', 136),
    row('row1', 188),
    {id: 'b', x: 188, y: 274, w: 172, h: 58, kind: 'service', lines: lines(words, 'b')},
    {id: 'takeover', x: 0, y: 356, w: 360, h: 58, kind: 'note', lines: lines(words, 'takeover')},
  ];
  const links: Link[] = [
    {from: 'a', to: 'row0', enterAt: (86 - 12) / 336, label: word(words, 'renews')},
    {from: 'b', to: 'row1', enterAt: (274 - 12) / 336, label: word(words, 'renews')},
  ];
  return {boxes, links};
}
