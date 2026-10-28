import type { ExecutionRow, InvocationOutcome } from './types'

/**
 * How an ended Tool call reads, in words you read. Its tone and the card under it keep
 * Coral's exact distinction. A complete response reads only as completed here; the selected
 * call's own response can prove more.
 */
export const invocationOutcomeLabels = {
  upstream_outcome: 'completed',
  coral_failure: 'failed',
  run_terminated: 'run ended',
  indeterminate: 'result unknown',
  unknown: 'result unknown',
} satisfies Record<InvocationOutcome, string>

/** A call whose completion Coral did not record, neither success nor failure. */
export const resultUnknown = 'result unknown'

/**
 * Who asked for a Run's work: its agent's name, or you, then its workspace's name. A missing
 * name is left out, never shown as an ID or a placeholder; an agent always stays an agent.
 */
export function requestedBy(run: Pick<ExecutionRow, 'actorLabel' | 'agent' | 'workspaceLabel'>) {
  const actor = run.actorLabel || (run.agent ? 'Unknown agent' : 'You')
  return run.workspaceLabel ? `${actor} · ${run.workspaceLabel}` : actor
}
