import { ComponentProps, PropsWithChildren } from 'react'

import { Typography } from '../typography'
import * as styles from './list.css'

type FooterProps = PropsWithChildren<
  Partial<Pick<ComponentProps<typeof Typography.BodySmall>, 'style' | 'truncate'>>
>

export function Footer({ children, ...props }: FooterProps) {
  return (
    <Typography.BodySmall className={styles.footer} variant="tertiary" {...props}>
      {children}
    </Typography.BodySmall>
  )
}
