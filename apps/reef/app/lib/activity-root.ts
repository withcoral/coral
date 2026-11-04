import type { ActivityRow } from '@/views/activity/types'

/** The Task or standalone Run whose timeline holds `row`: the root the Activity page opens. */
export function rootOf(row: ActivityRow): string {
  switch (row.kind) {
    case 'task':
      return row.name
    case 'tool':
      return row.task
    case 'run':
      return row.task || row.name
    case 'invocation':
    case 'output':
      return row.task || row.run
  }
}
