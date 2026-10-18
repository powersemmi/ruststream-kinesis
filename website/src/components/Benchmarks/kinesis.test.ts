import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {SCHEMAS, tabContent, type Labels, type Results} from './model';
import {KINESIS, kinesis} from './kinesis';

// The document as the benchmark recipe wrote it, read apart from the module under test, so a
// test compares what the page shows with what the file says.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as Results;

// Every word is its own key, so the tests read the rules and never a page's wording.
const labels = new Proxy({}, {get: (_, key) => String(key)}) as Labels;

describe("this crate's results", () => {
  it('are the document committed beside the pages, under the catalog entry of this crate', () => {
    expect(kinesis.broker).toBe(KINESIS);
    expect(KINESIS.repo).toBe('ruststream-kinesis');
    expect(kinesis.results).toEqual(document);
  });

  it('declare a schema the model renders', () => {
    expect(SCHEMAS).toContain(kinesis.results!.schema);
  });

  it('draw one comparison of every scenario the document measured, in its order', () => {
    const {comparison, code, pending} = tabContent(kinesis, 'en', labels);
    expect(comparison).toHaveLength(1);
    expect(comparison[0]!.rows.map((row) => row.scenario)).toEqual(
      document.scenarios!.map((scenario) => scenario.name),
    );
    expect(code!.rows.map((row) => row.scenario)).toEqual(
      document.code!.map((scenario) => scenario.name),
    );
    expect(pending).toBeNull();
  });
});

describe('the Kinesis scenarios', () => {
  it('carry the share of a round trip each message costs, beside the keys the model renders', () => {
    for (const scenario of document.scenarios!) {
      expect(typeof (scenario as {round_trips_per_message?: unknown}).round_trips_per_message).toBe(
        'number',
      );
    }
  });
});
