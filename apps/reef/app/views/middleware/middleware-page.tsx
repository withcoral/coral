import type { ReactNode } from 'react'
import { useSubmit } from 'react-router'
import { Banner, Button, Inputs, Table, Typography } from '@/wax/components'
import { Switch } from '@/wax/components/inputs/switch/switch'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { EmptyPage } from '@/components/empty-page'
import { ConfirmAction } from '@/views/management/forms'
import { ArchivedBanner, PageIntro, trail } from '@/views/management/layout'
import { PageLayout } from '@/wax/components/page-layout'
import type { ActionResult, FormContext, WorkspaceView } from '@/views/management/types'
import type { MiddlewareScopesView, MiddlewareView } from './types'
import * as styles from './middleware.css'

export interface MiddlewarePageProps {
  workspace: WorkspaceView
  middleware: MiddlewareView[]
  canChange: boolean
  form: FormContext
  pending?: boolean
  result?: ActionResult
  /** The source dialog route, when one is open. */
  children?: ReactNode
}

const scopeLabels = {
  run: 'Executions',
  function: 'Function calls',
  operation: 'Operations',
} as const satisfies Record<keyof MiddlewareScopesView, string>

/** Owners review Middleware, choose what it wraps, and order and enable it. */
export function MiddlewarePage({
  workspace: w,
  middleware,
  canChange,
  form,
  pending = false,
  result,
  children,
}: MiddlewarePageProps) {
  const submit = useSubmit()
  const table = Table.useRowCursor()
  const disabled = !canChange || pending
  const send = (intent: string, item: MiddlewareView, values: Record<string, string>) => {
    const data = new FormData()
    data.set('csrf', form.csrf)
    data.set('requestId', form.requestId)
    data.set('intent', intent)
    data.set('name', item.name)
    for (const [key, value] of Object.entries(values)) data.set(key, value)
    void submit(data, { method: 'post', preventScrollReset: true })
  }
  const setScope = (item: MiddlewareView, scope: keyof MiddlewareScopesView, on: boolean) => {
    const next = { ...item.scopes, [scope]: on }
    send(
      'scopes',
      item,
      Object.fromEntries(
        Object.entries(next)
          .filter(([, value]) => value)
          .map(([key]) => [key, 'on']),
      ),
    )
  }
  return (
    // The table takes the full content width: its scope and control columns stay
    // readable beside long descriptions.
    <PageLayout variant="wide" breadcrumbs={trail([], 'Middleware')}>
      <PageIntro>
        <Typography.Body as="p">
          Choose what each middleware wraps: executions, function calls, or operations. The top row
          runs first. When you change code, scopes, or order, Coral disables that middleware. Review
          it, then enable it again.
        </Typography.Body>
      </PageIntro>
      {w.state !== 'active' && (
        <ArchivedBanner workspace={w}>
          You cannot change middleware until you restore the workspace.
        </ArchivedBanner>
      )}
      {result?.error && <Banner variant="error">{result.error}</Banner>}
      {!result?.error && result?.message && <Banner variant="info">{result.message}</Banner>}
      {middleware.length === 0 ? (
        <EmptyPage
          iconName="Layers"
          title="No middleware yet"
          description="Use the save_middleware tool in your MCP client to add middleware. Then review it and enable it here."
        />
      ) : (
        <Table.Container
          ref={table}
          ariaLabel="Middleware"
          layout="scroll"
          columns={[
            { label: 'Order', width: 'max-content' },
            { label: 'Name', width: 'minmax(220px, 1fr)' },
            ...Object.values(scopeLabels).map((label) => ({
              label,
              width: 'max-content' as const,
            })),
            { label: 'Enabled', width: 'max-content' },
            { ariaLabel: 'Actions', width: 'max-content' },
          ]}
        >
          <Table.Head />
          <Table.Body>
            {middleware.map((item, index) => (
              <Table.Row key={item.name} to={item.path} preventScrollReset>
                <Table.Cell>
                  <div className={styles.order}>
                    <Typography.BodySmall>{index + 1}</Typography.BodySmall>
                    <Button.IconButton
                      name="ArrowUp"
                      ariaLabel={`Move ${item.displayName} up`}
                      disabled={disabled || index === 0}
                      onClick={() => send('move', item, { direction: 'up' })}
                    />
                    <Button.IconButton
                      name="ArrowDown"
                      ariaLabel={`Move ${item.displayName} down`}
                      disabled={disabled || index === middleware.length - 1}
                      onClick={() => send('move', item, { direction: 'down' })}
                    />
                  </div>
                </Table.Cell>
                <Table.Cell wrap>
                  <div className={styles.name}>
                    <Table.RowLink to={item.path}>{item.displayName}</Table.RowLink>
                    <Typography.BodySmall>{item.description}</Typography.BodySmall>
                  </div>
                </Table.Cell>
                {(Object.keys(scopeLabels) as (keyof MiddlewareScopesView)[]).map((scope) => (
                  <Table.Cell key={scope} control>
                    <Inputs.Checkbox
                      checked={item.scopes[scope]}
                      disabled={disabled}
                      onCheckedChange={(on) => setScope(item, scope, on)}
                    >
                      <span className={visuallyHidden}>
                        {`${scopeLabels[scope]} for ${item.displayName}`}
                      </span>
                    </Inputs.Checkbox>
                  </Table.Cell>
                ))}
                <Table.Cell control>
                  <div className={styles.enabled}>
                    <Switch
                      aria-label={`Enable ${item.displayName}`}
                      checked={item.enabled}
                      disabled={disabled}
                      onCheckedChange={(on) =>
                        send('enable', item, { enabled: on ? 'true' : 'false' })
                      }
                    />
                    <Typography.BodySmall>{item.enabled ? 'On' : 'Off'}</Typography.BodySmall>
                  </div>
                </Table.Cell>
                <Table.Cell actions>
                  <ConfirmAction
                    form={form}
                    intent="delete"
                    etag=""
                    name={item.name}
                    label={`Delete ${item.displayName}`}
                    confirm="Delete middleware"
                    explanation="New executions stop using this middleware. Running executions keep the middleware they started with."
                    pending={disabled}
                  />
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Container>
      )}
      {children}
    </PageLayout>
  )
}
