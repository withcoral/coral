import type { CatalogEntry } from './sources'

/**
 * Result of describing one pasted manifest. It lives here rather than in the
 * route so components and views can name the type without naming a route.
 */
export type SourceDescribeData =
  | { entry: CatalogEntry; status: 'success' }
  | { message: string; status: 'error' }
