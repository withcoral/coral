import type { ActivityPage } from '@/views/activity/types'

/**
 * Marks a request for one more page of a loaded Activity list. The loader then
 * reads that page from `before` or `after` instead of redirecting the old page link.
 */
export const activityPageFetchKey = 'load'
const failed = { activityPageFailed: true } as const

/**
 * Runs a route's server loader for the reader. A failed page load must not replace
 * the loaded list with the route's error boundary, so it resolves to a failure the
 * reader shows with a retry action. Redirects, such as sign in, still navigate.
 */
export async function loadActivityPage<Data>(
  request: Request,
  serverLoader: () => Promise<Data>,
): Promise<Data> {
  if (new URL(request.url).searchParams.get(activityPageFetchKey) !== 'page') return serverLoader()
  try {
    return await serverLoader()
  } catch (error) {
    if (error instanceof Response) throw error
    // Only the reader's fetcher reads this result, and it checks for the failure.
    return failed as Data
  }
}

/** The page in a page-load result: the Activity route's data, or an Agent route's activity. */
export function fetchedActivityPage(data: unknown): ActivityPage | null {
  if (!data || typeof data !== 'object' || 'activityPageFailed' in data) return null
  const page = 'activity' in data ? data.activity : data
  return page && typeof page === 'object' && 'rows' in page ? (page as ActivityPage) : null
}
