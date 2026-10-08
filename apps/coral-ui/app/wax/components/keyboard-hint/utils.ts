const isMacOs =
  typeof navigator !== 'undefined' &&
  ((navigator as Navigator & { userAgentData?: { platform: string } }).userAgentData?.platform ===
    'macOS' ||
    /mac/i.test(navigator.platform))

const prettyKeyboardKey: Record<string, string> = {
  $mod: isMacOs ? '⌘' : 'Ctrl',
  alt: isMacOs ? '⌥' : 'Alt',
  backspace: '⌫',
  control: 'Ctrl',
  ctrl: 'Ctrl',
  down: '↓',
  enter: '↩',
  esc: 'Esc',
  left: '←',
  meta: isMacOs ? '⌘' : 'Ctrl',
  mod: isMacOs ? '⌘' : 'Ctrl',
  return: '↩',
  right: '→',
  shift: '⇧',
  space: '␣',
  tab: '⇥',
  up: '↑',
}

/**
 * Formats a keyboard key to its display symbol.
 * - Modifier keys (mod, alt, shift, ctrl) are converted to symbols
 * - Single letters are uppercased
 * - Other keys are returned as-is
 */
export function formatKey(key: string): string {
  const lowerKey = key.toLowerCase()
  if (lowerKey in prettyKeyboardKey) {
    return prettyKeyboardKey[lowerKey]
  }
  // Single letter keys should be uppercase
  if (key.length === 1) {
    return key.toUpperCase()
  }
  return key
}

/**
 * Parses a keyboard shortcut string into an array of keys.
 * Supports formats like "mod+k", "mod k", "Cmd+Shift+K"
 */
export function parseShortcut(shortcut: string): string[] {
  // Split by + or space, filter empty strings
  return shortcut.split(/[+\s]/).filter(Boolean)
}
