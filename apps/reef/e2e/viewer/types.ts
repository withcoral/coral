export interface Result {
  id: string
  scenarioId: string
  actor?: string
  label: string
  purpose: 'setup' | 'interaction'
  sequence: number
  attempt: number
  scenario: string
  source: string
  target: string
  status: 'passed' | 'failed' | 'skipped'
  startedAt: string
  durationMs: number
  failure?: string
  steps: { label: string; screenshot: string; elapsedMs: number }[]
  recording?: string
  trace?: string
  warnings: string[]
  browserClosed: boolean
}

export interface ScenarioResult {
  id: string
  scenario: string
  source: string
  target: string
  status: 'pending' | 'passed' | 'failed' | 'skipped'
  startedAt?: string
  durationMs?: number
  retries: number
  repeats: number
  failure?: string
  note?: string
}

export type SessionResult = { path: string; result: Result }
export type ScenarioEntry = { path: string; result: ScenarioResult }
export interface TargetSummary {
  name: string
  state: 'waiting' | 'starting' | 'ready' | 'closed' | 'failed' | 'unavailable'
}
export interface RunSummary {
  id: string
  startedAt?: string
  commit?: string
  dirty: boolean
  film: boolean
  probes: boolean
  targets: TargetSummary[]
  results: ScenarioEntry[]
  setupError?: string
  warning?: string
}
export interface SessionPageProps {
  result: Result
  source?: string
  hasFailureScreenshot: boolean
}
export interface ScenarioPageProps {
  result: ScenarioResult
  source?: string
  sessions: SessionResult[]
}
