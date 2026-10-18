import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's states as types on the left, and on the right what each transition does on AWS:
 * the configuration loaded at connect, the calls each subscription makes at startup, the work
 * that runs while the service does, and the flag that shutdown sets.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, y: number, h: number, href?: string): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 500, kind: 'group', lines: lines(words, 'states')},
    {id: 'aws', x: 184, y: 0, w: 176, h: 500, kind: 'group', lines: lines(words, 'aws')},
    left('unconnected', 28),
    left('connected', 224),
    left('closed', 430),
    right('config', 28, 58),
    right('load', 100, 72),
    right('subscriptions', 196, 118, '/architecture/kinesis/shards/'),
    right('running', 338, 74, '/architecture/kinesis/leases/'),
    right('flag', 430, 58),
    {
      id: 'afterShutdown',
      x: 0,
      y: 522,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'load'},
    {from: 'load', to: 'subscriptions'},
    {from: 'subscriptions', to: 'running'},
    {from: 'running', to: 'flag'},
  ];
  return {boxes, links};
}
