export interface MiddlewareScopesView {
  run: boolean
  function: boolean
  operation: boolean
}
/** One Workspace Middleware as an Owner manages it. */
export interface MiddlewareView {
  /** Coral resource name. */
  name: string
  /** Reef path of the source dialog. */
  path: string
  displayName: string
  description: string
  enabled: boolean
  scopes: MiddlewareScopesView
  /** Zero-based chain position, outermost first. */
  position: number
  /** ISO time of the latest accepted change; empty when unknown. */
  updatedAt: string
}
export interface MiddlewareDiagnosticView {
  category: 'error' | 'warning' | 'suggestion' | 'message'
  code: number
  message: string
  line: number | null
  column: number | null
}
export interface MiddlewareDetailView extends MiddlewareView {
  source: string
  diagnostics: MiddlewareDiagnosticView[]
}
