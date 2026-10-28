import { useLocation, useNavigate } from 'react-router'
import { Banner, Button, Dialog, Table, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import { Markdown } from '@/wax/components/markdown'
import { SettingsRemoval } from '@/wax/components/settings-layout'
import { ActionDialog } from '@/views/management/forms'
import { Section } from '@/views/management/layout'
import type { FormContext } from '@/views/management/types'
import { SavedAt } from './saved-at'
import type { SavedFunctionDiagnosticView, SavedFunctionView } from './types'
import * as styles from './functions.css'

// The drawer caps the source and diagnostics; the rest scroll in place.
const SOURCE_LINES = 20
const DIAGNOSTIC_ROWS = 8

const categories: Record<SavedFunctionDiagnosticView['category'], string> = {
  error: 'Error',
  warning: 'Warning',
  suggestion: 'Suggestion',
  message: 'Message',
}

function location(d: SavedFunctionDiagnosticView) {
  if (d.line === null) return 'Whole source'
  return d.column === null ? `Line ${d.line}` : `Line ${d.line}, column ${d.column}`
}

/** A drawer over the list whose closing returns to the list's address. */
function Drawer({
  listPath,
  title,
  children,
}: {
  listPath: string
  title: string
  children: React.ReactNode
}) {
  const navigate = useNavigate()
  // The path names the function, so moving to another one while closing opens it.
  const drawer = Dialog.useCloseAfterExit(useLocation().pathname, () =>
    navigate(listPath, { preventScrollReset: true }),
  )
  return (
    <Dialog.Root {...drawer}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl" placement="right">
          <Dialog.Title>{title}</Dialog.Title>
          <div className={styles.stack}>{children}</div>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

export interface FunctionDrawerProps {
  listPath: string
  savedFunction: SavedFunctionView
  form: FormContext
  /** False in an archived workspace, which accepts no changes. */
  canDelete: boolean
}

/** One of the current User's Saved Functions, as Coral saved it. You can read or delete it. */
export function FunctionDrawer({
  listPath,
  savedFunction: f,
  form,
  canDelete,
}: FunctionDrawerProps) {
  const callPath = `coral.functions.${f.path}`
  return (
    <Drawer listPath={listPath} title={f.path}>
      <Markdown>{f.description}</Markdown>
      <div className={styles.callPath}>
        <CodeChip>{callPath}</CodeChip>
        <Button.CopyButton
          textToCopy={callPath}
          ariaLabel="Copy call path"
          size="22"
          variant="bare"
        />
      </div>
      <Typography.BodySmall as="p">
        Last saved <SavedAt at={f.savedAt} />
      </Typography.BodySmall>
      <Section title="Declaration">
        <CodeBlock value={f.declaration} format="typescript" framed />
      </Section>
      <Section title="Source">
        <CodeBlock value={f.source} format="typescript" framed maxLines={SOURCE_LINES} />
      </Section>
      <Section
        title="Diagnostics"
        description="Coral recorded these diagnostics when you saved the function. They do not stop it from running."
      >
        {f.diagnostics.length === 0 ? (
          <Typography.Body as="p">Coral found no diagnostics in this function.</Typography.Body>
        ) : (
          <Table.Container
            ariaLabel="Diagnostics"
            layout="fixed"
            maxRows={DIAGNOSTIC_ROWS}
            columns={[
              { label: 'Type', width: { fill: 1 } },
              { label: 'Location', width: { fill: 1.4 } },
              { label: 'Message', width: { fill: 4 } },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {f.diagnostics.map((d, index) => (
                <Table.Row key={index}>
                  <Table.Cell>{categories[d.category]}</Table.Cell>
                  <Table.Cell>{location(d)}</Table.Cell>
                  <Table.Cell wrap>{`TS${d.code}: ${d.message}`}</Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        )}
        {f.diagnosticsTruncated && (
          <Typography.BodySmall as="p" variant="secondary">
            {`Showing the first ${f.diagnostics.length.toLocaleString('en-US')} diagnostics only.`}
          </Typography.BodySmall>
        )}
      </Section>
      {canDelete && (
        <SettingsRemoval
          title="Delete function"
          description="Programs that call this function fail after you delete it."
          action={
            <ActionDialog
              title="Delete function"
              submit="Delete function"
              form={form}
              intent="delete"
              destructive
              description={`After you delete it, programs that call ${callPath} fail. You cannot undo this.`}
            />
          }
        />
      )}
    </Drawer>
  )
}

/** The drawer's failure, shown over the list that stays usable. */
export function FunctionDrawerError({ status, listPath }: { status: number; listPath: string }) {
  const message =
    status === 404
      ? 'This function does not exist. It may have been deleted.'
      : status === 403
        ? 'You do not have permission to open this function.'
        : status === 401
          ? 'Sign in again to continue.'
          : 'Coral is unavailable. Close this panel and try again.'
  return (
    <Drawer listPath={listPath} title={status === 404 ? 'Function not found' : 'Error'}>
      <Banner variant="error">{message}</Banner>
    </Drawer>
  )
}
