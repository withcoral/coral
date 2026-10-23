import { createContext, useContext } from 'react'
import { LinkProps } from 'react-router'

import { Container, InternalLink, Text } from '../button'
import { PageTitle } from '../page-title'
import { Tooltip } from '../tooltip'
import { Typography } from '../typography'
import * as styles from './breadcrumbs.css'

export type Segment = LinkSegment | AnchorSegment | TextSegment

interface BreadcrumbsProps {
  items: Segment[]
  ariaLabel?: string
  /** Mark the current page's segment as the page's h1, for a page with no other title. */
  heading?: boolean
}

type LinkSegment = Omit<LinkProps, 'children'> & {
  id: string
  text: string
  type: 'link'
}

/** A browser-native link for documents without a React Router runtime. */
interface AnchorSegment {
  id: string
  text: string
  type: 'anchor'
  href: string
}

interface TextSegment {
  id: string
  text: string
  type: 'text'
}

const RootContext = createContext<LinkSegment | null>(null)

/**
 * Starts every breadcrumb below it with one root segment, such as the current
 * workspace. A page with no segments of its own shows the root as its title.
 */
export const BreadcrumbRoot = RootContext.Provider

export function Breadcrumbs({
  items: own,
  ariaLabel = 'breadcrumb',
  heading = false,
}: BreadcrumbsProps) {
  const root = useContext(RootContext)
  const items: Segment[] = !root
    ? own
    : own.length
      ? [root, ...own]
      : [{ id: root.id, text: root.text, type: 'text' }]
  return (
    <nav aria-label={ariaLabel}>
      {/* The trail names the page, so it also titles the tab, most specific first. */}
      <PageTitle>
        {items
          .map((item) => item.text)
          .toReversed()
          .join(' · ')}
      </PageTitle>
      <ol className={styles.breadcrumbList}>
        {items.map((item, index) => {
          const as = heading && index === items.length - 1 ? 'h1' : 'span'
          return (
            <li className={styles.breadcrumbItem} key={item.id}>
              {item.type === 'link' ? (
                <InternalLink className={styles.breadcrumbItemContent} size="small" to={item.to}>
                  {item.text}
                </InternalLink>
              ) : item.type === 'anchor' ? (
                <Container
                  as="a"
                  href={item.href}
                  className={styles.breadcrumbItemContent}
                  size="32"
                  variant="linkSubtle"
                >
                  <Text>{item.text}</Text>
                </Container>
              ) : (
                <Tooltip content={item.text}>
                  <Typography.ButtonStrong as={as} truncate variant="secondary">
                    {item.text}
                  </Typography.ButtonStrong>
                </Tooltip>
              )}
            </li>
          )
        })}
      </ol>
    </nav>
  )
}
