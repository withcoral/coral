// A parameter whose name holds one of these is treated as secret. Matching
// parts of names errs toward hiding: `author` hides too, `api_key` never shows.
const SECRET_PARAMETER =
  /key|token|secret|pass|pwd|auth|sig|session|credential|cookie|jwt|bearer|^code$/i

/**
 * A source URL safe to show, with its host for a glance: no embedded
 * credentials, no fragment, and no value of a query parameter that looks
 * secret. Other parameters stay as they are so the reader can recognise the
 * address. Null when the value is not a URL, since it could then hold anything.
 */
export function redactSourceUrl(value: string): { url: string; host: string } | null {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    return null
  }
  const query = [...url.searchParams]
    .map(
      ([name, parameter]) =>
        `${encodeURIComponent(name)}=${
          SECRET_PARAMETER.test(name) ? '[redacted]' : encodeURIComponent(parameter)
        }`,
    )
    .join('&')
  return {
    url: `${url.protocol}//${url.host}${url.pathname}${query ? `?${query}` : ''}`,
    host: url.host,
  }
}
