import { useEffect, useRef } from 'react'

const ROW_LINK = '[data-row-link]'

// Controls that read the arrows themselves keep them. A text field keeps them
// too, except the table's own filter.
const ARROW_CONTROLS =
  'input, textarea, select, [contenteditable="true"], [role="menu"], [role="menubar"], [role="listbox"], [role="combobox"], [role="radiogroup"], [role="slider"], [role="tablist"], [role="tree"], [role="grid"], [role="dialog"], [role="alertdialog"]'

/**
 * Lets ↑ and ↓ move keyboard focus between the `Table.RowLink`s of a table.
 * From a row, or a control in it, they step to the next row; from the filter
 * field, ↓ reaches the first row; from anywhere else on the page, ↓ reaches
 * the first row and ↑ the last. Enter is the link's own, and the row with the
 * focused link shows it, so Tab and the arrows share one cursor.
 * Pass the returned ref to `Table.Container`, or to an element around several
 * tables to move through their rows as one list. A table inside a `hidden`
 * element ignores the arrows.
 */
export function useRowCursor({ filterId }: { filterId?: string } = {}) {
  const table = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.isComposing) return
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return
      const step = event.key === 'ArrowDown' ? 1 : event.key === 'ArrowUp' ? -1 : 0
      const target = event.target
      if (step === 0 || !table.current || !(target instanceof Element)) return
      // A table kept mounted in a hidden panel leaves the arrows to the visible one.
      if (table.current.closest('[hidden]')) return
      const links = [...table.current.querySelectorAll<HTMLElement>(ROW_LINK)]
      const filter = filterId ? document.getElementById(filterId) : null
      const row = table.current.contains(target) ? target.closest('[role="row"]') : null
      const at = row ? links.findIndex((link) => row.contains(link)) : -1
      let next: HTMLElement | null | undefined
      if (at >= 0) next = at + step < 0 ? filter : links[at + step]
      else if (filter && target === filter) next = step > 0 ? links[0] : undefined
      else if (!target.closest(ARROW_CONTROLS)) next = step > 0 ? links[0] : links.at(-1)
      // At either end with nowhere to go, the key keeps its default.
      if (!next) return
      event.preventDefault()
      next.focus()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [filterId])
  return table
}
