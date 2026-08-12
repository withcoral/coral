// Reduction of a caller-supplied return target to a path this origin can safely
// redirect to.
//
// Login and session expiry both take a destination from the request and hand it
// back as a `Location` once the round trip completes. That destination is
// attacker-supplied — `/login?returnTo=…` is public — so it is a redirect target
// only after it has been proven internal.

// The longest return target worth preserving. A destination this long is not a
// real Reef route, and a bound keeps a hostile input from being carried through
// a cookie and back out into a header.
const MAX_LENGTH = 2048

// Parsing needs a base, and the base must be an origin no input can also name —
// otherwise an absolute URL naming it would be accepted as internal. `.invalid`
// is reserved by RFC 2606 precisely so it can never resolve.
const SENTINEL_ORIGIN = 'https://reef.invalid'

// A path that stays on this origin: one leading slash, and the next character
// neither a slash nor a backslash.
//
// Applied to the *output*, which is the whole point. `//evil.example` is a
// scheme-relative URL, and a browser sends `Location: //evil.example` to
// evil.example over the current scheme — so a leading `//` is an off-origin
// redirect wearing a path's clothing. Backslash is included because browsers
// fold it to a slash before resolving.
const INTERNAL_PATH = /^\/(?![/\\])/

/**
 * Returns `value` as an internal path, or `/` when it is not one.
 *
 * The order matters, and it is the order an earlier pair of near-identical
 * helpers got wrong. Both rejected a literal leading `//` on the *input* and
 * then returned `URL`'s normalized pathname unexamined — but normalization is
 * itself capable of *producing* a leading `//`: in `/..//evil.example` the `..`
 * pops the empty first segment, leaving `//evil.example`. The input passed every
 * check, because at input time the value was genuinely a relative path.
 *
 * So the origin check proves the *input* names no other host, and
 * `INTERNAL_PATH` proves the *output* still names none after `URL` has had its
 * say. Neither check subsumes the other.
 */
export function safeInternalPath(value: string | null | undefined): string {
  if (!value || value.length > MAX_LENGTH || !value.startsWith('/')) return '/'

  let path: string
  try {
    const parsed = new URL(value, SENTINEL_ORIGIN)
    if (parsed.origin !== SENTINEL_ORIGIN) return '/'
    path = `${parsed.pathname}${parsed.search}${parsed.hash}`
  } catch {
    return '/'
  }

  return INTERNAL_PATH.test(path) ? path : '/'
}
