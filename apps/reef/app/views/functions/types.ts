/** One of the current User's Saved Functions in a list. It carries no source or diagnostics. */
export interface SavedFunctionSummaryView {
  path: string
  description: string
  /** ISO time of the latest accepted save. */
  savedAt: string
  href: string
}
export interface SavedFunctionDiagnosticView {
  category: 'error' | 'warning' | 'suggestion' | 'message'
  code: number
  message: string
  line: number | null
  column: number | null
}
export interface SavedFunctionView {
  path: string
  description: string
  declaration: string
  source: string
  diagnostics: SavedFunctionDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  diagnosticsTruncated: boolean
  savedAt: string
}
