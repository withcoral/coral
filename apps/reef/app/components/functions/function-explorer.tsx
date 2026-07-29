import { useCallback, useRef, useState } from 'react'

import { PageHeader } from '@/views/traces/page-header'
import { Container as ButtonContainer } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'

import { FunctionDetails, type FunctionDetailsProps } from './function-details'
import * as styles from './function-explorer.css'

export interface FunctionExplorerProps {
  functions: FunctionDetailsProps[]
  onSelect: (name: string) => void
  selectedName?: string
}

interface FunctionNamespace {
  functions: FunctionDetailsProps[]
  name: string
}

export function FunctionExplorer({ functions, onSelect, selectedName }: FunctionExplorerProps) {
  const selectedFunction = functions.find((fn) => fn.name === selectedName)
  const [search, setSearch] = useState('')
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [expandedNamespaces, setExpandedNamespaces] = useState<Set<string>>(() => new Set())
  const normalizedSearch = search.trim().toLowerCase()
  const filteredFunctions = functions.filter((fn) =>
    fn.name.toLowerCase().includes(normalizedSearch),
  )
  const namespaces = groupFunctionsByNamespace(filteredFunctions)

  const onSearchShortcut = useCallback((event: KeyboardEvent) => {
    const input = searchInputRef.current
    if (!input) return

    event.preventDefault()
    input.focus()
    input.select()
  }, [])

  const toggleNamespace = (name: string) => {
    setExpandedNamespaces((previous) => {
      const next = new Set(previous)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  return (
    <section aria-label="Functions explorer" className={styles.root}>
      <KeyboardShortcut handler={onSearchShortcut} shortcut="$mod+f" />
      <PageHeader title="Functions">
        <TextInput
          ariaLabel="Filter functions"
          icon="Search"
          onChange={setSearch}
          placeholder="Filter functions"
          ref={searchInputRef}
          value={search}
        />
      </PageHeader>

      <div className={styles.body}>
        <nav aria-label="Functions" className={styles.listPanel}>
          <ScrollArea className={styles.listContent} constrainWidth>
            {filteredFunctions.length === 0 ? (
              <div className={styles.listEmpty}>
                <Typography.BodySmall variant="tertiary">
                  {normalizedSearch ? `No results for "${search}"` : 'No functions available.'}
                </Typography.BodySmall>
              </div>
            ) : (
              <div className={styles.list}>
                {namespaces.map((namespace) => {
                  const expanded = normalizedSearch !== '' || expandedNamespaces.has(namespace.name)
                  const namespaceChildrenId = `function-namespace-${namespace.name}-items`
                  return (
                    <div key={namespace.name}>
                      <ButtonContainer
                        aria-controls={expanded ? namespaceChildrenId : undefined}
                        aria-expanded={expanded}
                        className={styles.listRow}
                        fullWidth
                        onClick={() => {
                          if (!normalizedSearch) toggleNamespace(namespace.name)
                        }}
                        size="22"
                        variant="bare"
                      >
                        <Icon
                          color="secondary"
                          name={expanded ? 'ChevronDown' : 'ChevronRight'}
                          size="14"
                        />
                        <Typography.BodyStrong className={styles.namespaceName}>
                          {namespace.name}
                        </Typography.BodyStrong>
                        <Typography.BodySmall
                          className={styles.namespaceFunctionCount}
                          variant="tertiary"
                        >
                          {namespace.functions.length}
                        </Typography.BodySmall>
                      </ButtonContainer>

                      {expanded ? (
                        <div className={styles.namespaceFunctions} id={namespaceChildrenId}>
                          {namespace.functions.map((fn) => {
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
                      ) : null}
                    </div>
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

function groupFunctionsByNamespace(functions: FunctionDetailsProps[]): FunctionNamespace[] {
  const grouped = new Map<string, FunctionDetailsProps[]>()

  for (const fn of functions) {
    const namespace = grouped.get(fn.namespace)
    if (namespace) namespace.push(fn)
    else grouped.set(fn.namespace, [fn])
  }

  return [...grouped.entries()]
    .map(([name, namespaceFunctions]) => ({
      functions: namespaceFunctions.toSorted((left, right) => left.name.localeCompare(right.name)),
      name,
    }))
    .toSorted((left, right) => left.name.localeCompare(right.name))
}
