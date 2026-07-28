import { CodeBlock } from '@/components/code-block'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Table } from '@/wax/components/table'
import { Typography } from '@/wax/components/typography'

import * as styles from './function-details.css'
import { FunctionSources } from './function-sources'

export interface FunctionDetailsArgument {
  dataType: string
  name: string
}

export interface FunctionDetailsResultColumn {
  dataType: string
  name: string
  nullable: boolean
}

export interface FunctionDetailsProps {
  arguments: FunctionDetailsArgument[]
  body?: string
  description: string
  name: string
  resultColumns: FunctionDetailsResultColumn[]
  sources: string[]
}

interface FunctionShapeItem {
  dataType: string
  name: string
  nullable?: boolean
}

export function FunctionDetails({
  arguments: functionArguments,
  body,
  description,
  name,
  resultColumns,
  sources,
}: FunctionDetailsProps) {
  return (
    <div aria-label={`Function ${name}`} className={styles.root}>
      <ScrollArea
        className={styles.scrollBody}
        constrainWidth
        fade="none"
        fillContent
        scrollDirection="vertical"
      >
        <div className={styles.content}>
          <header className={styles.intro}>
            <Typography.CodeLarge as="h1" className={styles.name} variant="primary">
              {name}
            </Typography.CodeLarge>
            {description ? (
              <Typography.Body as="p" className={styles.description} variant="secondary">
                {description}
              </Typography.Body>
            ) : null}
            <div className={styles.sources}>
              <Typography.BodySmall variant="tertiary">Sources</Typography.BodySmall>
              <div className={styles.sourcePills}>
                <FunctionSources sources={sources} />
              </div>
            </div>
          </header>

          <div className={styles.shapeGrid}>
            <FunctionShape
              emptyMessage="This function accepts no arguments."
              items={functionArguments}
              title="Arguments"
            />
            <FunctionShape
              emptyMessage="No result columns reported."
              items={resultColumns}
              title="Returns"
            />
          </div>

          {body ? (
            <section className={styles.section}>
              <Typography.BodySmallStrong as="h2">Definition</Typography.BodySmallStrong>
              <CodeBlock code={body} language="sql" />
            </section>
          ) : null}
        </div>
      </ScrollArea>
    </div>
  )
}

function FunctionShape({
  emptyMessage,
  items,
  title,
}: {
  emptyMessage: string
  items: FunctionShapeItem[]
  title: string
}) {
  return (
    <section className={styles.shapeSection}>
      <Typography.BodySmallStrong as="h2">{title}</Typography.BodySmallStrong>
      {items.length > 0 ? (
        <Table.Wrapper className={styles.shapeTable} style="compact">
          <Table.Root className={styles.shapeTableRoot}>
            <Table.Body>
              {items.map((item) => (
                <Table.Row key={item.name}>
                  <Table.Cell mono title={item.name}>
                    {item.name}
                  </Table.Cell>
                  <Table.Cell
                    align="right"
                    mono
                    title={`${item.dataType}${item.nullable ? '?' : ''}`}
                  >
                    {item.dataType}
                    {item.nullable ? '?' : ''}
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Root>
        </Table.Wrapper>
      ) : (
        <div className={styles.shapeEmpty}>
          <Typography.BodySmall variant="tertiary">{emptyMessage}</Typography.BodySmall>
        </div>
      )}
    </section>
  )
}
