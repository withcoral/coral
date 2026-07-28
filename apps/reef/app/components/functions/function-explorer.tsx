import { PageHeader } from '@/views/traces/page-header'
import { Container as ButtonContainer } from '@/wax/components/button'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'

import { FunctionDetails, type FunctionDetailsProps } from './function-details'
import * as styles from './function-explorer.css'

export interface FunctionExplorerProps {
  functions: FunctionDetailsProps[]
  onSelect: (name: string) => void
  selectedName?: string
}

export function FunctionExplorer({ functions, onSelect, selectedName }: FunctionExplorerProps) {
  const selectedFunction = functions.find((fn) => fn.name === selectedName)

  return (
    <section aria-label="Functions explorer" className={styles.root}>
      <PageHeader title="Functions" />

      <div className={styles.body}>
        <nav aria-label="Functions" className={styles.listPanel}>
          <ScrollArea className={styles.listContent} constrainWidth>
            {functions.length === 0 ? (
              <div className={styles.listEmpty}>
                <Typography.BodySmall variant="tertiary">
                  No functions available.
                </Typography.BodySmall>
              </div>
            ) : (
              <div className={styles.list}>
                {functions.map((fn) => {
                  const selected = fn.name === selectedName
                  return (
                    <ButtonContainer
                      aria-pressed={selected}
                      className={styles.listRow}
                      fullWidth
                      isActive={selected}
                      key={fn.name}
                      onClick={() => onSelect(fn.name)}
                      size="22"
                      variant="bare"
                    >
                      <Typography.BodyStrong className={styles.functionName}>
                        {fn.name}
                      </Typography.BodyStrong>
                    </ButtonContainer>
                  )
                })}
              </div>
            )}
          </ScrollArea>
        </nav>

        <div className={styles.detailPanel}>
          {selectedFunction ? (
            <FunctionDetails {...selectedFunction} />
          ) : (
            <div className={styles.detailEmpty}>
              <Typography.Body variant="secondary">
                Select a function to inspect it.
              </Typography.Body>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
