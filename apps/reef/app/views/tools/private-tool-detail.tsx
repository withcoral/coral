import { Table, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { SettingsRemoval } from '@/wax/components/settings-layout'
import { ActionDialog } from '@/views/management/forms'
import { Section } from '@/views/management/layout'
import type { FormContext } from '@/views/management/types'
import type { PrivateToolDetailView, PrivateToolDiagnosticView } from './types'

// The drawer caps the source and diagnostics; the rest scroll in place.
const SOURCE_LINES = 20
const DIAGNOSTIC_ROWS = 8

const categories: Record<PrivateToolDiagnosticView['category'], string> = {
  error: 'Error',
  warning: 'Warning',
  suggestion: 'Suggestion',
  message: 'Message',
}

function location(d: PrivateToolDiagnosticView) {
  if (d.line === null) return 'Whole source'
  return d.column === null ? `Line ${d.line}` : `Line ${d.line}, column ${d.column}`
}

/**
 * What Coral saved of one of your own private tools: its declaration, source
 * and diagnostics. With `canDelete`, you can delete it.
 */
export function PrivateToolDetail({
  detail,
  callPath,
  form,
  canDelete,
}: {
  detail: PrivateToolDetailView
  callPath: string
  form: FormContext
  /** False in an archived workspace, which accepts no changes. */
  canDelete: boolean
}) {
  return (
    <>
      <Section title="Declaration">
        <CodeBlock value={detail.declaration} format="typescript" framed />
      </Section>
      <Section title="Source">
        <CodeBlock value={detail.source} format="typescript" framed maxLines={SOURCE_LINES} />
      </Section>
      <Section
        title="Diagnostics"
        description="These diagnostics are from the last save. They do not stop the private tool from running."
      >
        {detail.diagnostics.length === 0 ? (
          <Typography.Body as="p">This private tool has no diagnostics.</Typography.Body>
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
              {detail.diagnostics.map((d, index) => (
                <Table.Row key={index}>
                  <Table.Cell>{categories[d.category]}</Table.Cell>
                  <Table.Cell>{location(d)}</Table.Cell>
                  <Table.Cell wrap>{`TS${d.code}: ${d.message}`}</Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        )}
        {detail.diagnosticsTruncated && (
          <Typography.BodySmall as="p" variant="secondary">
            {`Showing the first ${detail.diagnostics.length.toLocaleString('en-US')} diagnostics only.`}
          </Typography.BodySmall>
        )}
      </Section>
      {canDelete && (
        <SettingsRemoval
          title="Delete private tool"
          description="Programs that call this private tool fail after you delete it."
          action={
            <ActionDialog
              title="Delete private tool"
              submit="Delete private tool"
              form={form}
              intent="deletePrivateTool"
              destructive
              description={`After you delete it, programs that call ${callPath} fail. You cannot undo this.`}
            >
              <input type="hidden" name="path" value={detail.relativePath} />
            </ActionDialog>
          }
        />
      )}
    </>
  )
}
