type SettledApprovalState = 'approved' | 'declined' | 'expired' | 'closed'

const settledApprovals: Record<SettledApprovalState, { label: string }> = {
  approved: { label: 'Approved' },
  declined: { label: 'Denied' },
  expired: { label: 'Expired' },
  closed: { label: 'Closed' },
}

/** How Activity names a settled approval, in review and after a decision. Coral sends the
 * state as a string, so pending and states this build does not know return undefined. */
export function settledApproval(state: string) {
  return Object.hasOwn(settledApprovals, state)
    ? settledApprovals[state as SettledApprovalState]
    : undefined
}
