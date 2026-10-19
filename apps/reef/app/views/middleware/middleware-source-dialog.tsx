import { useLocation, useNavigate } from 'react-router'
import { Banner, Dialog, Table, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Section } from '@/views/management/layout'
import type { MiddlewareDetailView, MiddlewareDiagnosticView } from './types'
import * as styles from './middleware.css'

const categories: Record<MiddlewareDiagnosticView['category'], string> = {
  error: 'Error',
  warning: 'Warning',
  suggestion: 'Suggestion',
  message: 'Message',
}

function location(d: MiddlewareDiagnosticView) {
  if (d.line === null) return 'Whole source'
  return d.column === null ? `Line ${d.line}` : `Line ${d.line}, column ${d.column}`
}

/** A modal over the list whose closing returns to the list's address. */
function SourceModal({
  listPath,
  title,
  children,
}: {
  listPath: string
  title: string
  children: React.ReactNode
}) {
  const navigate = useNavigate()
  const modal = Dialog.useCloseAfterExit(useLocation().pathname, () =>
    navigate(listPath, { preventScrollReset: true }),
  )
  return (
    <Dialog.Root {...modal}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl">
          <Dialog.Title>{title}</Dialog.Title>
          <div className={styles.stack}>{children}</div>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

const scopeNames = {
  run: 'executions',
  function: 'function calls',
  operation: 'operations',
} as const

export interface MiddlewareSourceDialogProps {
  listPath: string
  middleware: MiddlewareDetailView
}

/** One Middleware's accepted source. Reef only shows it; save_middleware changes it. */
export function MiddlewareSourceDialog({ listPath, middleware: m }: MiddlewareSourceDialogProps) {
  const scopes = (Object.keys(scopeNames) as (keyof typeof scopeNames)[])
    .filter((scope) => m.scopes[scope])
    .map((scope) => scopeNames[scope])
  return (
    <SourceModal listPath={listPath} title={m.displayName}>
      <Typography.Body as="p">{m.description}</Typography.Body>
      <Typography.BodySmall as="p">
        {m.enabled ? 'Enabled.' : 'Disabled.'} It runs in position {m.position + 1} from the
        outside.{' '}
        {scopes.length > 0
          ? `It wraps ${scopes.join(', ')}.`
          : 'Select a scope on the middleware page to make it wrap a call.'}
      </Typography.BodySmall>
      <Section
        title="Source"
        description="To change the code, use save_middleware in your MCP client."
      >
        <CodeBlock value={m.source} format="typescript" framed />
      </Section>
      {m.diagnostics.length > 0 && (
        <Section
          title="Diagnostics"
          description="Coral recorded these diagnostics when the middleware was saved. They do not stop it from running."
        >
          <Table.Container
            ariaLabel="Diagnostics"
            layout="fixed"
            columns={[
              { label: 'Category', width: 120 },
              { label: 'Location', width: 160 },
              { label: 'Message', width: 'fill' },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {m.diagnostics.map((d, index) => (
                <Table.Row key={index}>
                  <Table.Cell>{categories[d.category]}</Table.Cell>
                  <Table.Cell>{location(d)}</Table.Cell>
                  <Table.Cell wrap>{`TS${d.code}: ${d.message}`}</Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        </Section>
      )}
    </SourceModal>
  )
}

export function MiddlewareSourceError({ status, listPath }: { status: number; listPath: string }) {
  const message =
    status === 404
      ? 'This middleware does not exist. Someone may have deleted it.'
      : status === 403
        ? 'Only workspace owners can open middleware.'
        : status === 401
          ? 'Sign in again to continue.'
          : 'Coral is unavailable. Close this window and try again.'
  return (
    <SourceModal listPath={listPath} title={status === 404 ? 'Middleware not found' : 'Error'}>
      <Banner variant="error">{message}</Banner>
    </SourceModal>
  )
}
