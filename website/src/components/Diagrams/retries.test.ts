import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('lets the attempt cap choose between a copy to the stream, the dead-letter stream and a checkpoint past the record', () => {
  const {links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['copy', 'deadLetter', 'past']);
  // Only the copy goes back to the stream, where it lands at the tip.
  expect(from('copy')).toEqual(['tip']);
  expect([...from('deadLetter'), ...from('past')]).toEqual([]);
});
