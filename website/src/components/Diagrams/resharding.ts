import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/** The parents of each shard of the drawing: a split gives one, a merge two. */
export const PARENTS: Record<string, readonly string[]> = {
  s1: ['s0'],
  s2: ['s0'],
  s3: ['s1', 's2'],
};

/** Whether a reader may start on `shard`: every parent is read to its end, or it has none. */
export function readable(shard: string, finished: ReadonlySet<string>): boolean {
  return (PARENTS[shard] ?? []).every((parent) => finished.has(parent));
}

/*
 * Resharding: a split closes one shard and opens two children over its hash key range, a merge
 * closes two adjacent shards and opens one, and a child is read only after its parents.
 */
export function resharding(words: Words): {boxes: Box[]; links: Link[]} {
  const shard = (id: string, x: number, y: number, w: number): Box => ({
    id,
    x,
    y,
    w,
    h: 58,
    kind: 'broker',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    shard('s0', 90, 0, 180),
    shard('s1', 0, 110, 170),
    shard('s2', 190, 110, 170),
    shard('s3', 90, 220, 180),
    {id: 'gate', x: 0, y: 306, w: 360, h: 58, kind: 'note', lines: lines(words, 'gate')},
  ];
  const links: Link[] = [
    {from: 's0', to: 's1', exitAt: 0.2, label: word(words, 'split')},
    {from: 's0', to: 's2', exitAt: 0.8, label: word(words, 'split')},
    {from: 's1', to: 's3', enterAt: 0.2, label: word(words, 'merge')},
    {from: 's2', to: 's3', enterAt: 0.8, label: word(words, 'merge')},
  ];
  return {boxes, links};
}
