import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The shards of the drawing, each read by a reader of its own.
export const SHARDS = 3;

const COLUMN = 104;
const columnX = (index: number): number => 12 + index * (COLUMN + 12);

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: a reader per shard it holds, one channel that joins them, and
 * `workers(n, by_key)` keying each record to a lane by its partition key.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind'], code = false): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    code,
    lines: lines(words, id),
  });
  const boxes: Box[] = [];
  for (let shard = 0; shard < SHARDS; shard += 1) {
    boxes.push(
      {
        id: `shard${shard}`,
        x: columnX(shard),
        y: 0,
        w: COLUMN,
        h: 30,
        kind: 'broker',
        lines: lines(words, `shard${shard}`),
      },
      {
        id: `reader${shard}`,
        x: columnX(shard),
        y: 58,
        w: COLUMN,
        h: 44,
        kind: 'broker',
        lines: lines(words, `reader${shard}`),
      },
    );
  }
  boxes.push(
    wide('channel', 134, 'broker'),
    wide('keyed', 206, 'core', true),
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 286,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {id: 'single', x: 0, y: 366, w: 360, h: 44, kind: 'note', lines: lines(words, 'single')},
    {id: 'instances', x: 0, y: 418, w: 360, h: 44, kind: 'note', lines: lines(words, 'instances')},
  );
  const centre = (x: number, w: number): number => (x + w / 2 - 12) / 336;
  const links: Link[] = [];
  for (let shard = 0; shard < SHARDS; shard += 1) {
    links.push(
      {from: `shard${shard}`, to: `reader${shard}`},
      {from: `reader${shard}`, to: 'channel', enterAt: centre(columnX(shard), COLUMN)},
    );
  }
  links.push(
    {from: 'channel', to: 'keyed', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({from: 'keyed', to: `lane${lane}`, exitAt: centre(x, 160)})),
  );
  return {boxes, links};
}
