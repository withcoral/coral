import { Table, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Section } from '@/views/management/layout'
import type { ToolDiagnosticView, ToolSourceView } from './types'

// The drawer caps the source and diagnostics; the rest scroll in place.
const SOURCE_LINES = 20
const DIAGNOSTIC_ROWS = 8

const categories: Record<ToolDiagnosticView['category'], string> = {
  error: 'Error',
  warning: 'Warning',
  suggestion: 'Suggestion',
  message: 'Message',
}

function location(d: ToolDiagnosticView) {
  if (d.line === null) return 'Whole source'
  return d.column === null ? `Line ${d.line}` : `Line ${d.line}, column ${d.column}`
}

/**
 * The diagnostics Coral kept of a source, or `none` when it kept none. They
 * are advisory: a source with diagnostics still runs.
 */
export function Diagnostics({
  diagnostics,
  truncated,
  none,
}: {
  diagnostics: ToolDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  truncated: boolean
  none: string
}) {
  return (
    <>
      {diagnostics.length === 0 ? (
        <Typography.Body as="p">{none}</Typography.Body>
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
            {diagnostics.map((d, index) => (
              <Table.Row key={index}>
                <Table.Cell>{categories[d.category]}</Table.Cell>
                <Table.Cell>{location(d)}</Table.Cell>
                <Table.Cell wrap>{`TS${d.code}: ${d.message}`}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Container>
      )}
      {truncated && (
        <Typography.BodySmall as="p" variant="secondary">
          {`Showing the first ${diagnostics.length.toLocaleString('en-US')} diagnostics only.`}
        </Typography.BodySmall>
      )}
    </>
  )
}

/**
 * What Coral keeps of a private or a shared tool's source: its declaration,
 * the source, and the diagnostics of the save or the publish that made it.
 */
export function ToolSource({
  source,
  diagnosticsNote,
  noDiagnostics,
}: {
  source: ToolSourceView
  /** Says which save or publish the diagnostics are from. */
  diagnosticsNote: string
  noDiagnostics: string
}) {
  return (
    <>
      <Section title="Declaration">
        <CodeBlock value={source.declaration} format="typescript" framed />
      </Section>
      <Section title="Source">
        <CodeBlock value={source.source} format="typescript" framed maxLines={SOURCE_LINES} />
      </Section>
      <Section title="Diagnostics" description={diagnosticsNote}>
        <Diagnostics
          diagnostics={source.diagnostics}
          truncated={source.diagnosticsTruncated}
          none={noDiagnostics}
        />
      </Section>
    </>
  )
}
