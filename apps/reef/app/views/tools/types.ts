import type { SourceChoice } from '@/lib/provider-recipes/choice'
import type {
  ActionResult,
  FormContext,
  ProviderToolDetailView,
  ProviderView,
  ToolCallCounts,
  WorkspaceView,
} from '@/views/providers/types'

export interface PrivateToolDiagnosticView {
  category: 'error' | 'warning' | 'suggestion' | 'message'
  code: number
  message: string
  line: number | null
  column: number | null
}

/** One of your own private tools as Coral saved it, for the drawer. */
export interface PrivateToolDetailView {
  kind: 'private'
  /** Its path under `coral.private`. */
  relativePath: string
  description: string
  declaration: string
  source: string
  diagnostics: PrivateToolDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  diagnosticsTruncated: boolean
  /** ISO time of the latest accepted save. */
  savedAt: string
}

/** What the drawer shows of the one tool the address names. */
export type ToolDetailView = ProviderToolDetailView | PrivateToolDetailView

export interface ToolsPageProps {
  workspace: WorkspaceView
  recipes?: readonly SourceChoice[]
  /** Every group of tools: Coral, each provider, and the private tools. */
  providers: ProviderView[]
  selectedProvider: string | null
  /** The relative path of the open tool. In the private group, it names your own. */
  selectedTool: string | null
  /** What the drawer shows of `selectedTool`, which `providers` leaves out. */
  selectedToolDetail?: ToolDetailView | null
  /** Absent when Coral cannot count calls now; the page then shows no counts. */
  toolCalls?: ToolCallCounts | null
  form: FormContext
  result?: ActionResult
  pending?: boolean
}
