import {expect, it} from 'vitest';
import {SHARDS, lanes} from './lanes';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('reads each shard with a reader of its own, and joins every reader into one channel', () => {
  const {links} = lanes(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  for (let shard = 0; shard < SHARDS; shard += 1) {
    expect(from(`shard${shard}`)).toEqual([`reader${shard}`]);
    expect(from(`reader${shard}`)).toEqual(['channel']);
  }
  expect(from('channel')).toEqual(['keyed']);
  expect(from('keyed')).toEqual(['lane0', 'lane1']);
});
