import { Typography } from '@/wax/components/typography'

import * as s from '../traces-page.css'

export function PageHeader({ children, searchExpanded, title }: { children?: React.ReactNode; searchExpanded?: boolean; title: React.ReactNode }) {
  return (
    <header className={s.header} data-search-expanded={searchExpanded ? 'true' : undefined}>
      <div className={s.headerTitle}>{typeof title === 'string' ? <Typography.BodyStrong as="span" variant="secondary">{title}</Typography.BodyStrong> : title}</div>
      {children}
    </header>
  )
}
