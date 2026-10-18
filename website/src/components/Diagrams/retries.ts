import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on Kinesis, which holds no record back and counts no deliveries: the runtime settles
 * the original, reads its own count off a header, and publishes a copy to the subscription's
 * stream, to the dead-letter stream, or nowhere, checkpointing past the record.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind']): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    wide('record', 0, 'broker'),
    wide('outcome', 72, 'service'),
    wide('cap', 144, 'core'),
    {id: 'copy', x: 0, y: 220, w: 112, h: 104, kind: 'step', lines: lines(words, 'copy')},
    {
      id: 'deadLetter',
      x: 124,
      y: 220,
      w: 112,
      h: 104,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
    },
    {id: 'past', x: 248, y: 220, w: 112, h: 104, kind: 'bad', lines: lines(words, 'past')},
    {id: 'tip', x: 0, y: 352, w: 236, h: 58, kind: 'broker', lines: lines(words, 'tip')},
    {id: 'crash', x: 0, y: 432, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'record', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'copy', exitAt: along(56), label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: along(180), label: word(words, 'atCap')},
    {from: 'cap', to: 'past', exitAt: along(304)},
    {from: 'copy', to: 'tip', enterAt: 56 / 236},
  ];
  return {boxes, links};
}
