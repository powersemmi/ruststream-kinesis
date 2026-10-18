import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The two columns: a subscription that takes one record at a time, and one that takes batches.
const COLUMNS = {single: 92, batch: 268} as const;
const W = 160;
const H = 44;
const STEP = 66;

/*
 * The path of one record through a service, in the two shapes a subscription takes: one record
 * at a time, with the reply it answers with, and a batch that one read's limit sizes.
 */
export function path(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (
    id: string,
    column: keyof typeof COLUMNS,
    row: number,
    fields: Partial<Box> = {},
  ): Box => ({
    id,
    x: COLUMNS[column] - W / 2,
    y: 28 + row * STEP,
    w: W,
    h: H,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    step('receive', 'single', 0, {
      kind: 'broker',
      code: true,
      href: '/architecture/kinesis/reading/',
    }),
    step('channel', 'single', 1, {kind: 'broker'}),
    step('message', 'single', 2, {kind: 'broker', code: true}),
    step('handler', 'single', 3, {kind: 'service', href: '/architecture/lanes/'}),
    step('reply', 'single', 4, {kind: 'broker'}),
    step('ack', 'single', 5, {kind: 'core', href: '/architecture/settlement/'}),
    step('batchReceive', 'batch', 0, {kind: 'broker', code: true}),
    step('batchChannel', 'batch', 1, {kind: 'broker'}),
    step('batchMessages', 'batch', 2, {kind: 'broker'}),
    step('batchHandler', 'batch', 3, {kind: 'service'}),
    step('batchAck', 'batch', 5, {kind: 'core', href: '/architecture/settlement/'}),
  ];
  const chain = (ids: string[]): Link[] =>
    ids.slice(1).map((id, index) => ({from: ids[index]!, to: id}));
  const links: Link[] = [
    ...chain(['receive', 'channel', 'message', 'handler', 'reply', 'ack']),
    ...chain(['batchReceive', 'batchChannel', 'batchMessages', 'batchHandler', 'batchAck']),
  ];
  links[0]!.label = word(words, 'batch');
  const lanes: Lane[] = [
    {x: COLUMNS.single, label: word(words, 'single')},
    {x: COLUMNS.batch, label: word(words, 'batches')},
  ];
  return {boxes, links, lanes};
}
