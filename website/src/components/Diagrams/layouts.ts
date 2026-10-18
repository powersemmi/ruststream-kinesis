import type {Box, Lane, Link} from './flow';
import {iterator} from './iterator';
import {leases} from './leases';
import {resharding} from './resharding';
import {retries} from './retries';
import {stream} from './stream';
import {watermark} from './watermark';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  stream,
  resharding,
  iterator,
  leases,
  watermark,
  retries,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
