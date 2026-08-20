import { useOutletContext, useParams } from 'react-router'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { Table } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'
import { findSchemaTableFunction, tableFunctionTreeArguments } from './schema'
import { SchemaColumnsTable } from './schema-table'

const ARGUMENT_COLUMNS: Table.Column[] = [
  { label: 'name', width: 'content' },
  { label: 'allowed values', width: 'fill' },
]

export function SchemaTableFunctionView() {
  const schema = useOutletContext<SchemaResponse>()
  const params = useParams()
  const schemaName = params.schemaName ?? ''
  const functionName = params.functionName ?? ''
  const tableFunction = findSchemaTableFunction(schema, schemaName, functionName)

  if (!tableFunction) {
    return (
      <div className={styles.detailEmpty}>
        <Typography.Body variant="secondary">Table function not found.</Typography.Body>
      </div>
    )
  }

  return (
    <div className={styles.detailContent}>
      <div className={styles.detailHeader}>
        <div>
          <Typography.HeadingSmall as="h2">
            {schemaName}.{tableFunction.name}
            <Typography.Body>{tableFunctionTreeArguments(tableFunction)}</Typography.Body>
          </Typography.HeadingSmall>
          {tableFunction.description ? (
            <Typography.BodySmall as="p" className={styles.description}>
              {tableFunction.description}
            </Typography.BodySmall>
          ) : null}
        </div>
      </div>

      <div className={styles.section}>
        <Typography.BodySmallStrong>Arguments</Typography.BodySmallStrong>
        {tableFunction.arguments.length === 0 ? (
          <Typography.BodySmall className={styles.emptyInline} variant="tertiary">
            This table function accepts no arguments.
          </Typography.BodySmall>
        ) : (
          <Table.Container columns={ARGUMENT_COLUMNS} density="compact">
            <Table.Head />
            <Table.Body>
              {tableFunction.arguments.map((argument) => (
                <Table.Row key={argument.name}>
                  <Table.Cell mono>
                    {argument.name}
                    {argument.required ? (
                      <Tooltip content="Required argument" side="top">
                        <span
                          aria-label="Required argument"
                          className={styles.requiredStar}
                          tabIndex={0}
                        >
                          *
                        </span>
                      </Tooltip>
                    ) : null}
                  </Table.Cell>
                  <Table.Cell mono>
                    {argument.values.length > 0 ? argument.values.join(', ') : '-'}
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        )}
      </div>

      <div className={styles.section}>
        <Typography.BodySmallStrong>Result columns</Typography.BodySmallStrong>
        <SchemaColumnsTable
          columns={tableFunction.resultColumns}
          emptyMessage="No result columns reported for this table function."
        />
      </div>
    </div>
  )
}
