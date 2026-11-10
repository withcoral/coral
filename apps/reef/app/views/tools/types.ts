import type { PolicyState } from '@/components/policy-toggle'
import type { SourceChoice } from '@/lib/provider-recipes/choice'
import type { PersonView } from '@/views/management/types'
import type {
  ActionResult,
  FormContext,
  ProviderToolDetailView,
  ProviderView,
  ToolCallCounts,
  WorkspaceView,
} from '@/views/providers/types'

/** One diagnostic of a private or a shared tool's source. */
export interface ToolDiagnosticView {
  category: 'error' | 'warning' | 'suggestion' | 'message'
  code: number
  message: string
  line: number | null
  column: number | null
}

/** What Coral keeps of a private or a shared tool's source. */
export interface ToolSourceView {
  declaration: string
  source: string
  diagnostics: ToolDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  diagnosticsTruncated: boolean
}

/** One private tool as Coral saved it, for the drawer: your own, or an agent's. */
export interface PrivateToolDetailView extends ToolSourceView {
  kind: 'private'
  /** The identifier of the agent that owns it; null for your own. */
  agent: string | null
  /** Its path under `coral.private`. */
  relativePath: string
  description: string
  /** ISO time of the latest accepted save. */
  savedAt: string
}

/** One revision of a shared tool, as the list of its revisions shows it. */
export interface SharedToolRevisionView {
  revision: number
  description: string
  /** Who published it; null when they are no longer an owner of the workspace. */
  publisher: PersonView | null
  /** ISO time of the publish. */
  publishedAt: string
  /** The earlier revision it restored; null when it copied a private tool. */
  restoredFrom: number | null
}

/** One shared tool with its current revision and its revisions, for the drawer. */
export interface SharedToolDetailView extends ToolSourceView {
  kind: 'shared'
  /** Its path under `coral.shared`. */
  relativePath: string
  description: string
  /** The current revision, which every new run links. */
  revision: number
  /** Who published the current revision; null when they are no longer an owner. */
  publisher: PersonView | null
  /** ISO time of that publish. */
  publishedAt: string
  /**
   * Every revision, newest first. Each publish makes one, so they are the
   * shared tool's history too.
   */
  revisions: SharedToolRevisionView[]
}

/** What a publish copies: a private tool, or an earlier revision to restore. */
export type PublishOrigin =
  | { kind: 'private'; path: string; agent: string | null }
  | { kind: 'restore'; revision: number }

/** One side of a publish's diffs: the revision current at the path. */
export interface PublishedRevisionView {
  revision: number
  description: string
  source: string
  declaration: string
  /** Who published it; null when they are no longer an owner of the workspace. */
  publisher: PersonView | null
  /** ISO time of the publish. */
  publishedAt: string
}

/**
 * What an owner reviews before a publish. Coral computes it from the exact
 * description and source, which the publish then sends unchanged.
 */
export interface PublishReviewView extends ToolSourceView {
  origin: PublishOrigin
  /** The shared tool path the review is for. */
  path: string
  description: string
  /** The revision the publish replaces; null when it creates the shared tool. */
  previous: PublishedRevisionView | null
  /** Each call path the source calls by a literal path, in path order. */
  calledTools: string[]
  /** The shared tools among them, with the revision each has now. */
  sharedTools: { path: string; revision: number }[]
  /** The start of each tool path the source builds at runtime. */
  runtimeBuiltPaths: string[]
  /** The policy the dialog starts on; null when no revision sets one. */
  defaultPolicy: PolicyState | null
}

/** A review, or why Coral refuses the publish as the form describes it. */
export type PublishReviewResult =
  | { review: PublishReviewView; refusal: null }
  | { review: null; refusal: string; path: string }

/** What the drawer shows of the one tool the address names. */
export type ToolDetailView = ProviderToolDetailView | PrivateToolDetailView | SharedToolDetailView

export interface ToolsPageProps {
  workspace: WorkspaceView
  recipes?: readonly SourceChoice[]
  /** Every group of tools: Coral, the shared tools, each provider, and the private tools. */
  providers: ProviderView[]
  selectedProvider: string | null
  /** The relative path of the open tool. */
  selectedTool: string | null
  /** The identifier of the agent whose private tool is open; absent or null for any other tool. */
  selectedToolAgent?: string | null
  /** What the drawer shows of `selectedTool`, which `providers` leaves out. */
  selectedToolDetail?: ToolDetailView | null
  /** Absent when Coral cannot count calls now; the page then shows no counts. */
  toolCalls?: ToolCallCounts | null
  form: FormContext
  result?: ActionResult
  pending?: boolean
}
