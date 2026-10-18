import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The records of the shard in the drawing, and the cell after the last one: where the next
// record will land.
const RECORDS = 6;
const STEP = 48;
const CELL = 42;

// Each iterator type and the cell it points at, left to right along the shard, so no two
// arrows cross.
export const TYPES = [
  {id: 'horizon', cell: 0},
  {id: 'timestamp', cell: 1},
  {id: 'atSequence', cell: 3},
  {id: 'afterSequence', cell: 4},
  {id: 'latest', cell: RECORDS},
] as const;

/*
 * Reading a shard: an iterator type places the first read on the shard's log, and every
 * `GetRecords` answers with the records and the iterator of the next read.
 */
export function iterator(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'shard', x: 0, y: 0, w: 360, h: 150, kind: 'group', lines: lines(words, 'shard')},
  ];
  for (let cell = 0; cell <= RECORDS; cell += 1) {
    const tip = cell === RECORDS;
    boxes.push({
      id: `r${cell}`,
      x: 12 + cell * STEP,
      y: 26,
      w: CELL,
      h: 30,
      kind: tip ? 'note' : 'step',
      lines: tip ? lines(words, 'next') : [String(cell + 1)],
    });
  }
  TYPES.forEach(({id}, index) => {
    boxes.push({
      id,
      x: 12 + index * 68,
      y: 78,
      w: 64,
      h: 62,
      kind: 'core',
      code: true,
      lines: lines(words, id),
    });
  });
  boxes.push(
    {
      id: 'getIterator',
      x: 12,
      y: 178,
      w: 260,
      h: 44,
      kind: 'broker',
      code: true,
      lines: lines(words, 'getIterator'),
    },
    {
      id: 'getRecords',
      x: 12,
      y: 250,
      w: 260,
      h: 44,
      kind: 'broker',
      code: true,
      lines: lines(words, 'getRecords'),
    },
    {id: 'answer', x: 12, y: 322, w: 260, h: 58, kind: 'step', lines: lines(words, 'answer')},
    {id: 'expiry', x: 0, y: 404, w: 360, h: 44, kind: 'note', lines: lines(words, 'expiry')},
  );
  const links: Link[] = [
    ...TYPES.map(({id, cell}) => ({from: id, to: `r${cell}`})),
    {from: 'shard', to: 'getIterator', exitAt: 142 / 360, label: word(words, 'type')},
    {from: 'getIterator', to: 'getRecords', label: word(words, 'iterator')},
    {from: 'getRecords', to: 'answer'},
    {
      from: 'answer',
      to: 'getRecords',
      exit: 'right',
      enter: 'right',
      dashed: true,
    },
  ];
  return {boxes, links};
}
