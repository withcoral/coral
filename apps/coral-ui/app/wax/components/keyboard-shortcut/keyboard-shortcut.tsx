import { useEffect } from 'react'
import { tinykeys } from 'tinykeys'

import { KeyboardHint } from '@/wax/components/keyboard-hint'
import { Tooltip } from '@/wax/components/tooltip'

export type KeyboardShortcutProps =
  | KeyboardShortcutWithoutTooltipProps
  | KeyboardShortcutWithTooltipProps

interface KeyboardShortcutBaseProps {
  handler: (event: KeyboardEvent) => void
  /** Keyboard shortcut string, for example "$mod+b" or "g i". */
  shortcut: string
  /** Element to attach the listener to. Defaults to window in the browser. */
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

export function KeyboardShortcut({
  children,
  handler,
  shortcut,
  target,
  tooltipContent,
  tooltipSide = 'top',
}: KeyboardShortcutProps) {
  useEffect(() => {
    const shortcutTarget = target ?? (typeof window === 'undefined' ? undefined : window)
    if (!shortcutTarget) return

    return tinykeys(shortcutTarget, {
      [shortcut]: (event) => {
        if (document.querySelector('[role="dialog"][data-open]')) return
        handler(event)
      },
    })
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
