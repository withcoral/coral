import { createContext, useCallback, useContext, useEffect, useId, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useFetcher } from 'react-router'

import { policyLabels, type PolicyState } from '@/components/policy-toggle'
import { ActionDialog } from '@/views/management/forms'
import { usePolicyMutations } from '@/views/providers/policy-mutations'
import type { FormContext, ProviderView, ToolView } from '@/views/providers/types'
import { Banner, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import * as DescriptionList from '@/wax/components/description-list'

import { subAgentConsentPath } from './addresses'
import type { SubAgentConsentRead } from './types'

/** The call path of the tool, as people and programs name it. */
export const SUB_AGENT_CALL_PATH = 'coral.agent'

// The Tool Key that the catalog's `BuiltinTool::Agent` has.
const SUB_AGENT_KEY = 'agent'

/** Whether this is `coral.agent` itself. A provider can name a tool `agent` too. */
function isSubAgent(provider: Pick<ProviderView, 'kind'>, tool: Pick<ToolView, 'key'>) {
  return provider.kind === 'coral' && tool.key === SUB_AGENT_KEY
}

/** `coral.agent`, once the organization has a model for it. */
export function subAgentOf(providers: readonly ProviderView[]) {
  for (const provider of providers)
    for (const tool of provider.kind === 'coral' ? provider.tools : [])
      if (isSubAgent(provider, tool)) return { provider, tool }
  return null
}

/** Whether a policy change turns on a tool that is off. */
export function turnsOn(from: PolicyState, to: PolicyState) {
  return from === 'disabled' && to !== 'disabled'
}

/** Whether a policy change needs an owner to see first where the sub-agent sends their data. */
export function needsSubAgentConsent(
  provider: Pick<ProviderView, 'kind'>,
  tool: Pick<ToolView, 'key'>,
  from: PolicyState,
  to: PolicyState,
) {
  return isSubAgent(provider, tool) && turnsOn(from, to)
}

/** The policy that turns `coral.agent` on, and the tool as the page lists it. */
interface Ask {
  provider: ProviderView
  tool: ToolView
  access: PolicyState
}

const Context = createContext<((ask: Ask) => void) | null>(null)

/** Opens the dialog that turns `coral.agent` on. The function keeps its identity. */
export function useAskSubAgentConsent() {
  const ask = useContext(Context)
  if (!ask) throw new Error('A policy control requires SubAgentConsent')
  return ask
}

/**
 * The page's one dialog for turning `coral.agent` on. A workspace owner confirms the model that
 * the dialog shows, by its id. Coral refuses another one, and the dialog then shows that.
 */
export function SubAgentConsent({
  workspacePath,
  form,
  children,
}: {
  workspacePath: string
  form: FormContext
  children: ReactNode
}) {
  const id = useId()
  const mutations = usePolicyMutations()
  // Each opening reads the model anew, so it never starts on one that was shown before.
  const [opening, setOpening] = useState<(Ask & { number: number }) | null>(null)
  const [open, setOpen] = useState(false)
  const read = useFetcher<SubAgentConsentRead>({ key: `${id}-${opening?.number ?? 0}` })
  const { load } = read
  const path = subAgentConsentPath(workspacePath)
  useEffect(() => {
    if (open) void load(path)
  }, [open, load, path])
  const asker = useRef<Element | null>(null)
  const ask = useCallback((next: Ask) => {
    asker.current = document.activeElement
    setOpening((last) => ({ ...next, number: (last?.number ?? 0) + 1 }))
    setOpen(true)
  }, [])
  // The row that the arrow keys act on takes focus but is no tab stop. Left to itself, the dialog
  // gives focus to the checkbox in the row, which keeps the arrow keys.
  const returnFocus = useCallback(() => {
    queueMicrotask(() => {
      if (asker.current instanceof HTMLElement) asker.current.focus({ preventScroll: true })
    })
    return false
  }, [])
  const shown = read.state === 'idle' ? read.data : undefined
  return (
    <Context value={ask}>
      {children}
      <ActionDialog
        open={open}
        onOpenChange={setOpen}
        title={`Turn on ${SUB_AGENT_CALL_PATH}`}
        description="Tool results from this workspace go to the host below. This includes conversations in progress."
        submit="Turn on"
        form={form}
        intent="turnOn"
        action={path}
        disabled={!shown?.model}
        finalFocus={returnFocus}
        // A refusal can mean that the model changed, so the dialog shows the one to confirm.
        onError={() => void load(path)}
        // The row can still show that Coral refused a change made without the dialog.
        onSuccess={() => opening && mutations.supersede(opening.provider.id, opening.tool.key)}
      >
        <input type="hidden" name="access" value={opening?.access ?? ''} />
        {!shown ? (
          <Typography.BodySmall as="p" role="status">
            Loading the model…
          </Typography.BodySmall>
        ) : shown.model ? (
          <>
            <input type="hidden" name="modelId" value={shown.model.id} />
            <DescriptionList.List>
              <DescriptionList.Row label="Host">{shown.model.host}</DescriptionList.Row>
              <DescriptionList.Row label="Model">{shown.model.name}</DescriptionList.Row>
              <DescriptionList.Row label="Arguments">
                {shown.model.requestArgs ? (
                  <CodeBlock value={shown.model.requestArgs} format="json" />
                ) : (
                  'None'
                )}
              </DescriptionList.Row>
              <DescriptionList.Row label="Policy">
                {opening && policyLabels[opening.access]}
              </DescriptionList.Row>
            </DescriptionList.List>
          </>
        ) : (
          <Banner variant="error">{shown.problem}</Banner>
        )}
      </ActionDialog>
    </Context>
  )
}
