const isMacOs =
  typeof navigator !== 'undefined' &&
  ((navigator as Navigator & { userAgentData?: { platform: string } }).userAgentData?.platform === 'macOS' ||
    /mac/i.test(navigator.platform))

const prettyKeyboardKey: Record<string, string> = {
  $mod: isMacOs ? '⌘' : 'Ctrl',
  alt: isMacOs ? '⌥' : 'Alt',
  arrowdown: '↓',
  arrowleft: '←',
  arrowright: '→',
  arrowup: '↑',
  backspace: '⌫',
  control: 'Ctrl',
  ctrl: 'Ctrl',
  down: '↓',
  enter: '↩',
  esc: 'Esc',
  escape: 'Esc',
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

export function formatKey(key: string): string {
  const lowerKey = key.toLowerCase()
  if (lowerKey in prettyKeyboardKey) return prettyKeyboardKey[lowerKey]
  if (key.length === 1) return key.toUpperCase()
  return key
}

export function parseShortcut(shortcut: string): string[] {
  return shortcut.split(/[+\s]/).filter(Boolean)
}
