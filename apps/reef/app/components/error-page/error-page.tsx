import { Typography } from '@/wax/components'
import { PageTitle } from '@/wax/components/page-title'

import * as styles from './error-page.css'

export interface ErrorPageProps {
  title: string
  details: string
  stack?: string
}

/** Root failures must render without loader data or the authenticated app shell. */
export function ErrorPage({ title, details, stack }: ErrorPageProps) {
  return (
    <main className={styles.page}>
      <PageTitle>{title}</PageTitle>
      <Typography.HeadingLarge as="h1">{title}</Typography.HeadingLarge>
      <Typography.Body as="p" role="alert">
        {details}
      </Typography.Body>
      {stack && (
        <Typography.Code as="pre" className={styles.stack}>
          {stack}
        </Typography.Code>
      )}
    </main>
  )
}
