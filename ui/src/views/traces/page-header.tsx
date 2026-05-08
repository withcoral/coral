import { Typography } from '@/wax/components/typography'

import * as s from '../traces-page.css'

export function PageHeader({ children, title }: { children?: React.ReactNode; title: React.ReactNode }) {
  return (
    <header className={s.header}>
      <div className={s.headerTitle}>{typeof title === 'string' ? <Typography.BodyStrong as="span" variant="secondary">{title}</Typography.BodyStrong> : title}</div>
      {children}
    </header>
  )
}
