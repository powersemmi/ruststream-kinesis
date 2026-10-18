import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The deliveries of one shard in the drawing, numbered in the order they arrived, and whether
// each has settled.
export const DELIVERED: ReadonlyArray<{record: number; settled: boolean}> = [
  {record: 1, settled: true},
  {record: 2, settled: true},
  {record: 3, settled: false},
  {record: 4, settled: true},
  {record: 5, settled: true},
  {record: 6, settled: false},
];

/**
 * The record a shard's checkpoint names: the last of the run of settled deliveries that starts
 * at the first one, or `undefined` while the first delivery is still running.
 */
export function checkpointAt(delivered: typeof DELIVERED): number | undefined {
  let checkpoint: number | undefined;
  for (const delivery of delivered) {
    if (!delivery.settled) {
      break;
    }
    checkpoint = delivery.record;
  }
  return checkpoint;
}

const STEP = 56;

/*
 * The watermark on one shard: acks arrive out of order from concurrent lanes, and the checkpoint
 * stops at the last record before the first delivery still running.
 */
export function watermark(words: Words): {boxes: Box[]; links: Link[]} {
  const checkpoint = checkpointAt(DELIVERED);
  const boxes: Box[] = [
    {id: 'shard', x: 0, y: 0, w: 360, h: 20, kind: 'note', lines: lines(words, 'shard')},
    ...DELIVERED.map(
      ({record, settled}, index): Box => ({
        id: `r${record}`,
        x: 12 + index * STEP,
        y: 28,
        w: 48,
        h: 44,
        kind: settled ? 'good' : 'step',
        lines: [String(record), word(words, settled ? 'settled' : 'unsettled')],
      }),
    ),
    {
      id: 'checkpoint',
      x: 12,
      y: 112,
      w: 160,
      h: 58,
      kind: 'core',
      lines: lines(words, 'checkpoint'),
    },
    {
      id: 'store',
      x: 12,
      y: 212,
      w: 160,
      h: 58,
      kind: 'broker',
      code: true,
      lines: lines(words, 'store'),
      href: '/architecture/kinesis/leases/',
    },
    {id: 'next', x: 188, y: 112, w: 172, h: 158, kind: 'note', lines: lines(words, 'next')},
  ];
  const links: Link[] = [
    {from: 'checkpoint', to: `r${checkpoint}`, label: word(words, 'at')},
    {from: 'checkpoint', to: 'store', label: word(words, 'writes')},
  ];
  return {boxes, links};
}
