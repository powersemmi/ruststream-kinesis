import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {iterator} from './iterator';
import {lanes} from './lanes';
import {leases} from './leases';
import {lifecycle} from './lifecycle';
import {path} from './path';
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
  design,
  lifecycle,
  path,
  watermark,
  retries,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
