/*
 * This crate's results, as the pages of this site show them. The document is the one the
 * benchmark recipe committed beside the pages, read when the site is built: a version of the
 * documentation shows the numbers measured for it, and an archive keeps its own. Its scenarios
 * also carry the share of a round trip each message costs, which the model does not render and
 * the methodology page explains.
 */

import document from '../../../../docs/benchmarks/results.json';
import {BROKERS, type Broker, type Loaded, type Results} from './model';

export const KINESIS: Broker = BROKERS.find((broker) => broker.repo === 'ruststream-kinesis')!;

export const kinesis: Loaded = {broker: KINESIS, results: document as Results};
