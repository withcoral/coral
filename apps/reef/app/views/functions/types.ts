import type { PolicyState } from '@/components/policy-toggle'

/** One of the current User's Private Tools in a list. It carries no source or diagnostics. */
export interface PrivateToolSummaryView {
  path: string
  description: string
  /** ISO time of the latest accepted save. */
  savedAt: string
  href: string
  /** The policy Coral applies to the function now; absent when Coral sent none. */
  policy?: PolicyState
}
export interface PrivateToolDiagnosticView {
  category: 'error' | 'warning' | 'suggestion' | 'message'
  code: number
  message: string
  line: number | null
  column: number | null
}
export interface PrivateToolView {
  path: string
  description: string
  declaration: string
  source: string
  diagnostics: PrivateToolDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  diagnosticsTruncated: boolean
  savedAt: string
}
