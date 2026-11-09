import { CodeChip } from '@/wax/components/code-chip'
import type { ParentInvocation } from './types'

/** The tool call whose body made a call, by its call path and, for a shared tool, the revision
 * that ran. The call path never names the program: a tool's calls are its own. */
export function ParentInvocationName({ parent }: { parent: ParentInvocation }) {
  return (
    <>
      <CodeChip>{parent.callPath}</CodeChip>
      {parent.sharedToolRevision && `, revision ${parent.sharedToolRevision}`}
    </>
  )
}

/** "Called from" and the tool call whose body made a call, as a line of its own. */
export function CalledFrom({ parent }: { parent: ParentInvocation }) {
  return (
    <span>
      Called from <ParentInvocationName parent={parent} />
    </span>
  )
}
