const AVATAR_COLOR_VARIANTS = ['amber', 'blue', 'green', 'purple', 'red'] as const

export type AvatarColorVariant = (typeof AVATAR_COLOR_VARIANTS)[number]

/**
 * Gets a deterministic color variant based on a seed string.
 * The same seed will always return the same color.
 */
export function getAvatarColorFromSeed(seed: string): AvatarColorVariant {
  const hash = hashString(seed)
  const index = hash % AVATAR_COLOR_VARIANTS.length
  return AVATAR_COLOR_VARIANTS[index]
}

/**
 * Simple hash function to convert a string to a number.
 */
function hashString(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = (hash << 5) - hash + char
    hash = hash & hash // Convert to 32-bit integer
  }
  return Math.abs(hash)
}
