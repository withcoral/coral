import { Avatar as BaseAvatar } from '@base-ui/react/avatar'
import classnames from 'classnames'

import { getAvatarColorFromSeed } from '@/wax/components/avatar/utils/get-avatar-color'

import * as styles from './logo.css'

export type LogoSize = '18' | '20' | '32' | '40'

/** Image URLs by theme. `light` serves both themes when `dark` is absent. */
export interface LogoIcons {
  light: string
  dark?: string
}

export interface LogoProps {
  className?: string
  /** Names the logo's owner. The first letter stands in when there is no image. */
  name: string
  /** @default "32" */
  size?: LogoSize
  /** When absent, or when an image fails to load, the first letter shows. */
  icons?: LogoIcons | null
}

/**
 * A third-party logo on a rounded square plate from the theme. It is
 * decorative: show the name beside it.
 */
export function Logo({ icons, ...props }: LogoProps) {
  if (!icons?.dark) return <Plate {...props} src={icons?.light} />
  // Both render and the theme class picks one, so the server-rendered page
  // already shows the right logo before hydration.
  return (
    <>
      <Plate
        {...props}
        src={icons.light}
        className={classnames(styles.lightOnly, props.className)}
      />
      <Plate {...props} src={icons.dark} className={classnames(styles.darkOnly, props.className)} />
    </>
  )
}

function Plate({ className, name, size = '32', src }: Omit<LogoProps, 'icons'> & { src?: string }) {
  return (
    <BaseAvatar.Root className={classnames(styles.logoContainer({ size }), className)}>
      <BaseAvatar.Image alt="" className={styles.logoImage} src={src} />
      <BaseAvatar.Fallback
        aria-hidden
        className={styles.logoFallback({ color: getAvatarColorFromSeed(name), size })}
        delay={src ? 80 : undefined}
      >
        {name.trim().charAt(0).toUpperCase() || '?'}
      </BaseAvatar.Fallback>
      <span className={styles.logoBorderOverlay} />
    </BaseAvatar.Root>
  )
}
