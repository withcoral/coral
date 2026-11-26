import { Tabs as BaseTabs } from '@base-ui/react/tabs'
import classNames from 'classnames'
import { useEffect, useRef } from 'react'

import { Container as ScrollArea } from '@/wax/components/scroll-area'

import * as styles from './tabs.css'

export function List({
  className,
  inset = 'page',
  ...props
}: React.ComponentPropsWithoutRef<typeof BaseTabs.List> & {
  /** `page` lines the tabs up with page content; `none` with the content beside them. */
  inset?: 'page' | 'none'
}) {
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const list = listRef.current
    if (!list) return

    const scrollSelectedIntoView = (behavior: ScrollBehavior = 'smooth') => {
      const tab = list.querySelector<HTMLElement>('[aria-selected="true"]')
      tab?.scrollIntoView({ behavior, block: 'nearest', inline: 'nearest' })
    }

    // Delay initial scroll to allow fonts to load and tab dimensions to stabilise
    const timeout = setTimeout(() => scrollSelectedIntoView('instant'), 100)

    const observer = new MutationObserver(() => {
      scrollSelectedIntoView()
    })

    observer.observe(list, { attributeFilter: ['aria-selected'], attributes: true, subtree: true })

    return () => {
      clearTimeout(timeout)
      observer.disconnect()
    }
  }, [])

  return (
    <ScrollArea
      className={styles.listRoot}
      fade="horizontal"
      height="auto"
      scrollDirection="horizontal"
      viewportClassName={styles.listViewport}
    >
      <BaseTabs.List
        className={classNames(styles.list, inset === 'none' && styles.listFlush, className)}
        ref={listRef}
        {...props}
      />
    </ScrollArea>
  )
}
