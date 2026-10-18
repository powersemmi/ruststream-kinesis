import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the AWS SDK, and Kinesis and DynamoDB at the bottom. The left column of the crate
 * talks to Kinesis, the right one configures it and keeps the leases, which reach DynamoDB when
 * the store is the DynamoDB one.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The left column stands off the crate's edge, so a link from it to the Kinesis client runs
  // down a gutter of its own, past the boxes in between.
  const left = 44;
  const right = 204;
  const column = 140;
  const crate = (id: string, x: number, y: number, href?: string): Box =>
    box(id, x, y, column, 58, {kind: 'broker', code: true, href});
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 298, {kind: 'group'}),
    crate('subscriber', left, 226, '/architecture/kinesis/shards/'),
    crate('message', left, 294, '/architecture/settlement/'),
    crate('publisher', left, 362, '/architecture/path/'),
    crate('descriptor', right, 226, '/architecture/kinesis/'),
    crate('broker', right, 294, '/architecture/lifecycle/'),
    crate('policy', right, 362),
    crate('leases', right, 430, '/architecture/kinesis/leases/'),
    box('sdk', 0, 522, 360, 80, {kind: 'group'}),
    box('kinesisClient', left, 548, column, 44, {kind: 'broker'}),
    box('dynamoClient', right, 548, column, 44, {kind: 'broker'}),
    box('kinesis', left, 626, column, 44, {kind: 'broker'}),
    box('dynamodb', right, 626, column, 44, {kind: 'broker'}),
    box('inProcess', 0, 690, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {
      from: 'runtime',
      to: 'subscriber',
      exitAt: (left + column / 2 - 12) / 336,
      label: word(words, 'polls'),
    },
    {from: 'subscriber', to: 'kinesisClient', exit: 'left', enter: 'left'},
    {from: 'publisher', to: 'kinesisClient', exit: 'left', enter: 'left'},
    {from: 'leases', to: 'dynamoClient'},
    {from: 'kinesisClient', to: 'kinesis', label: word(words, 'https')},
    {from: 'dynamoClient', to: 'dynamodb', label: word(words, 'https')},
  ];
  return {boxes, links};
}
