import { LinkProps } from 'react-router'

import { InternalLink } from '../button'
import { Tooltip } from '../tooltip'
import { Typography } from '../typography'
import * as styles from './breadcrumbs.css'

export type Segment = LinkSegment | TextSegment

interface BreadcrumbsProps {
  items: Segment[]
}

type LinkSegment = Omit<LinkProps, 'children'> & {
  id: string
  text: string
  type: 'link'
}

interface TextSegment {
  id: string
  text: string
  type: 'text'
}

export function Breadcrumbs({ items }: BreadcrumbsProps) {
  return (
    <nav aria-label="breadcrumb">
      <ol className={styles.breadcrumbList}>
        {items.map((item) => {
          return (
            <li className={styles.breadcrumbItem} key={item.id}>
              {item.type === 'link' ? (
                <InternalLink className={styles.breadcrumbItemContent} size="small" to={item.to}>
                  {item.text}
                </InternalLink>
              ) : (
                <Tooltip content={item.text}>
                  {items.length > 1 ? (
                    <Typography.ButtonStrong truncate variant="secondary">
                      {item.text}
                    </Typography.ButtonStrong>
                  ) : (
                    <Typography.Body truncate variant="secondary">
                      {item.text}
                    </Typography.Body>
                  )}
                </Tooltip>
              )}
            </li>
          )
        })}
      </ol>
    </nav>
  )
}
