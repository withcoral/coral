import { useEffect } from 'react'
import type React from 'react'

import { KeyboardHint } from '@/wax/components/keyboard-hint'
import { Tooltip } from '@/wax/components/tooltip'

import { parseShortcut } from '../keyboard-hint'

export type KeyboardShortcutProps = KeyboardShortcutWithoutTooltipProps | KeyboardShortcutWithTooltipProps

interface KeyboardShortcutBaseProps {
  handler: (event: KeyboardEvent) => void
  shortcut: string
  target?: HTMLElement | Window
}

interface KeyboardShortcutWithoutTooltipProps extends KeyboardShortcutBaseProps {
  children?: never
  tooltipContent?: never
  tooltipSide?: never
}

interface KeyboardShortcutWithTooltipProps extends KeyboardShortcutBaseProps {
  children: React.ReactElement<Record<string, unknown>>
  tooltipContent: string
  tooltipSide?: 'bottom' | 'left' | 'right' | 'top'
}

function isMacOs() {
  return typeof navigator !== 'undefined' && /mac/i.test(navigator.platform)
}

function normalizeKey(key: string) {
  const lowerKey = key.toLowerCase()
  if (lowerKey === 'esc') return 'escape'
  if (lowerKey === 'up') return 'arrowup'
  if (lowerKey === 'down') return 'arrowdown'
  if (lowerKey === 'left') return 'arrowleft'
  if (lowerKey === 'right') return 'arrowright'
  if (lowerKey === 'mod' || lowerKey === '$mod') return isMacOs() ? 'meta' : 'control'
  return lowerKey
}

function shortcutMatches(event: KeyboardEvent, shortcut: string) {
  const keys = parseShortcut(shortcut).map(normalizeKey)
  const mainKey = keys.at(-1)
  if (!mainKey || normalizeKey(event.key) !== mainKey) return false

  const wantsAlt = keys.includes('alt')
  const wantsControl = keys.includes('control') || keys.includes('ctrl')
  const wantsMeta = keys.includes('meta') || keys.includes('$mod')
  const wantsShift = keys.includes('shift')

  return event.altKey === wantsAlt &&
    event.ctrlKey === wantsControl &&
    event.metaKey === wantsMeta &&
    event.shiftKey === wantsShift
}

export function KeyboardShortcut({
  children,
  handler,
  shortcut,
  target = window,
  tooltipContent,
  tooltipSide = 'top',
}: KeyboardShortcutProps) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (shortcutMatches(event, shortcut)) handler(event)
    }

    target.addEventListener('keydown', handleKeyDown as EventListener)
    return () => target.removeEventListener('keydown', handleKeyDown as EventListener)
  }, [target, shortcut, handler])

  if (children && tooltipContent) {
    return (
      <Tooltip
        content={
          <>
            {tooltipContent} <KeyboardHint shortcut={shortcut} />
          </>
        }
        side={tooltipSide}
      >
        {children}
      </Tooltip>
    )
  }

  return null
}
