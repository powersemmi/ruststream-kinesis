// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// Pages that moved within the site. The broker page of the earlier site taught the whole crate;
// its topics are the guides now, whose overview is the front page of the version.
const pages: Record<string, string> = {
  kinesis: '/',
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(_docsRs: string): Record<string, string> {
  return {...pages};
}
