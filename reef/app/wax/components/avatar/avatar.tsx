import { Avatar as BaseAvatar } from '@base-ui/react/avatar'
import classnames from 'classnames'

import * as styles from './avatar.css'
import { getAvatarColorFromSeed } from './utils/get-avatar-color'

export interface AvatarProps {
  className?: string
  /**
   * Delay in ms before showing the fallback. Useful to avoid flickering
   * when an image loads quickly.
   * @default 80
   */
  delayMs?: number
  /**
   * The name to display as fallback (first character) and for the alt text.
   */
  name: string
  ref?: React.Ref<HTMLSpanElement>
  /**
   * Seed used to generate a deterministic fallback color.
   * If not provided, the name is used as the seed.
   */
  seed?: string
  /**
   * @default "20"
   */
  size?: AvatarSize
  /**
   * Image source URL. If not provided or if loading fails, the fallback is shown.
   */
  src?: null | string
}

export type AvatarSize = '20'

export function Avatar({
  className,
  delayMs = 80,
  name,
  ref,
  seed,
  size = '20',
  src,
  ...props
}: AvatarProps) {
  const color = getAvatarColorFromSeed(seed ?? name)

  return (
    <BaseAvatar.Root
      className={classnames(styles.avatarContainer({ size }), className)}
      ref={ref}
      {...props}
    >
      <BaseAvatar.Image alt={name} className={styles.avatarImage} src={src ?? undefined} />
      <BaseAvatar.Fallback
        className={styles.avatarFallback({ color, size })}
        delay={src ? delayMs : undefined}
      >
        {name.charAt(0)}
      </BaseAvatar.Fallback>
      <span className={styles.avatarBorderOverlay} />
    </BaseAvatar.Root>
  )
}
