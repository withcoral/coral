import { createContext, useContext, type ReactNode } from 'react'
import { LinkProps } from 'react-router'

import { Container, InternalLink, Text } from '../button'
import { Tooltip } from '../tooltip'
import { Typography } from '../typography'
import * as styles from './breadcrumbs.css'

export type Segment = LinkSegment | AnchorSegment | TextSegment

interface BreadcrumbsProps {
  items: Segment[]
  ariaLabel?: string
  /** Mark the current page's segment as the page's h1, for a page with no other title. */
  heading?: boolean
  /** Shown in place of the current segment, for a title that carries its own control. */
  current?: ReactNode
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
  current,
}: BreadcrumbsProps) {
  const root = useContext(RootContext)
  const items: Segment[] = !root
    ? own
    : own.length
      ? [root, ...own]
      : [{ id: root.id, text: root.text, type: 'text' }]
  return (
    <nav aria-label={ariaLabel}>
      <ol className={styles.breadcrumbList}>
        {items.map((item, index) => {
          const as = heading && index === items.length - 1 ? 'h1' : 'span'
          return (
            <li className={styles.breadcrumbItem} key={item.id}>
              {current && index === items.length - 1 ? (
                current
              ) : item.type === 'link' ? (
                <InternalLink className={styles.breadcrumbItemContent} to={item.to}>
                  {item.text}
                </InternalLink>
              ) : item.type === 'anchor' ? (
                <Container
                  as="a"
                  href={item.href}
                  className={styles.breadcrumbItemContent}
                  size="36"
                  variant="linkSubtle"
                >
                  <Text>{item.text}</Text>
                </Container>
              ) : (
                <Tooltip content={item.text}>
                  <Typography.BodyLargeStrong as={as} truncate variant="secondary">
                    {item.text}
                  </Typography.BodyLargeStrong>
                </Tooltip>
              )}
            </li>
          )
        })}
      </ol>
    </nav>
  )
}
