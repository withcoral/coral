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
   * The name to display as fallback (its first letters) and for the alt text.
   * Pass a real name: a placeholder such as "Local user" would show initials.
   */
  name: string
  ref?: React.Ref<HTMLSpanElement>
  /**
   * Seed used to generate a deterministic fallback color.
   * If not provided, the name is used as the seed.
   */
  seed?: string
  /**
   * A circle is a person and shows two initials. A square is a place, such as
   * a workspace, and shows one letter.
   * @default "circle"
   */
  shape?: AvatarShape
  /**
   * @default "20"
   */
  size?: AvatarSize
  /**
   * Image source URL. If not provided or if loading fails, the fallback is shown.
   */
  src?: null | string
}

export type AvatarSize = '20' | '28'
export type AvatarShape = 'circle' | 'square'

/**
 * The letters that stand in for an image. CSS sets them in capitals. A square
 * shows the first letter of the name. A circle shows two: the initials of the
 * first and last words that start with a letter or a digit, so
 * "Jane Doe (Acme)" gives "JD". One word, or a name with no such word, gives
 * its first two characters.
 */
function fallbackLetters(name: string, shape: AvatarShape) {
  // By character, not UTF-16 unit, so a letter outside the basic plane stays whole.
  const all = name
    .trim()
    .split(/\s+/)
    .map((word) => Array.from(word))
  if (shape === 'square') return all[0][0] ?? ''
  const words = all.filter((word) => /^[\p{L}\p{N}]/u.test(word[0] ?? ''))
  if (words.length > 1) return words[0][0] + words.at(-1)![0]
  return (words[0] ?? all[0]).slice(0, 2).join('')
}

export function Avatar({
  className,
  delayMs = 80,
  name,
  ref,
  seed,
  shape = 'circle',
  size = '20',
  src,
  ...props
}: AvatarProps) {
  const color = getAvatarColorFromSeed(seed ?? name)

  return (
    <BaseAvatar.Root
      className={classnames(styles.avatarContainer({ shape, size }), className)}
      ref={ref}
      {...props}
    >
      <BaseAvatar.Image alt={name} className={styles.avatarImage} src={src ?? undefined} />
      {/* The initial repeats the name, so screen readers skip it. */}
      <BaseAvatar.Fallback
        aria-hidden
        className={styles.avatarFallback({ color, size })}
        delay={src ? delayMs : undefined}
      >
        {fallbackLetters(name, shape)}
      </BaseAvatar.Fallback>
      <span className={styles.avatarBorderOverlay} />
    </BaseAvatar.Root>
  )
}
