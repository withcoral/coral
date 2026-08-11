// Shared URL predicates.
//
// These live outside the source modules because the preset query parser and the
// create flow have to agree on what counts as a usable URL. A string prefix test is
// not the same question: `https://`, `https://?a=1` and `https://#frag` all pass
// `startsWith('https://')` while being no URL at all.

/** True when `value` parses as a URL with an `https:` scheme. */
export function isHttpsUrl(value: string): boolean {
  try {
    return new URL(value.trim()).protocol === 'https:'
  } catch {
    return false
  }
}
