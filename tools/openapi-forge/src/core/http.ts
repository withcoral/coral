/**
 * Networking, used only by the `fetch` stage.
 *
 * Kept small and in one place so it is obvious that nothing else in the forge
 * reaches the network — that is what makes `build` reproducible.
 */

const USER_AGENT = 'coral-openapi-forge'
const DEFAULT_CONCURRENCY = 8
const RETRIES = 3

export class FetchError extends Error {}

export async function fetchBytes(url: string): Promise<Uint8Array> {
  let lastError: unknown
  for (let attempt = 1; attempt <= RETRIES; attempt += 1) {
    try {
      const response = await fetch(url, { headers: { 'user-agent': USER_AGENT } })
      if (!response.ok) {
        // A 404 will not fix itself; only transient failures are worth retrying.
        if (response.status < 500 && response.status !== 429) {
          throw new FetchError(`${url} returned HTTP ${response.status}`)
        }
        throw new Error(`${url} returned HTTP ${response.status}`)
      }
      return new Uint8Array(await response.arrayBuffer())
    } catch (error) {
      if (error instanceof FetchError) {
        throw error
      }
      lastError = error
      if (attempt < RETRIES) {
        await delay(250 * 2 ** (attempt - 1))
      }
    }
  }
  throw new FetchError(
    `failed to fetch ${url} after ${RETRIES} attempts: ${
      lastError instanceof Error ? lastError.message : String(lastError)
    }`,
  )
}

export async function fetchText(url: string): Promise<string> {
  return new TextDecoder().decode(await fetchBytes(url))
}

export interface FetchAllOptions {
  concurrency?: number
  /**
   * Treat a "not found" as an absent result rather than a failure.
   *
   * For inputs a provider links to but does not always publish — Slack links
   * to a reference page for `identity:read` that returns 404 — the alternative
   * is failing the whole fetch over one missing page.
   */
  optional?: boolean
}

/**
 * Fetch many URLs with a bounded number in flight.
 *
 * Results are keyed by URL rather than returned in order, so callers cannot
 * accidentally depend on completion order.
 */
export async function fetchAllBytes(
  urls: readonly string[],
  options: FetchAllOptions = {},
): Promise<Map<string, Uint8Array>> {
  const results = new Map<string, Uint8Array>()
  const queue = [...new Set(urls)]

  const workers = Array.from(
    { length: Math.min(options.concurrency ?? DEFAULT_CONCURRENCY, queue.length) },
    async () => {
      for (;;) {
        const url = queue.shift()
        if (url === undefined) {
          return
        }
        try {
          results.set(url, await fetchBytes(url))
        } catch (error) {
          if (!(options.optional === true && error instanceof FetchError)) {
            throw error
          }
        }
      }
    },
  )

  await Promise.all(workers)
  return results
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds)
  })
}
