import { ScrollArea as BaseScrollArea } from '@base-ui/react/scroll-area'
import { Tabs as BaseTabs } from '@base-ui/react/tabs'
import classNames from 'classnames'
import { useEffect, useRef } from 'react'

import * as styles from './tabs.css'

export function List({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof BaseTabs.List>) {
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
    <BaseScrollArea.Root className={styles.listRoot}>
      <BaseScrollArea.Viewport className={styles.listViewport}>
        <BaseScrollArea.Content>
          <BaseTabs.List className={classNames(styles.list, className)} ref={listRef} {...props} />
        </BaseScrollArea.Content>
      </BaseScrollArea.Viewport>
    </BaseScrollArea.Root>
  )
}
