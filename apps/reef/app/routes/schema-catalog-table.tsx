import { loadSchemaTableRoute } from './schema-table-loader.server'

import type { Route } from './+types/schema-catalog-table'

export function loader(args: Route.LoaderArgs) {
  return loadSchemaTableRoute(args)
}

export {
  SchemaTableRoute as default,
  SchemaTableError as ErrorBoundary,
} from './schema-table-route'
