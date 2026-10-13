/**
 * Scrolls a `layout="scroll"` table the least distance that shows all of `row`
 * beneath the table head and the group headings pinned above it. A row already
 * in full view stays where it is. For a cursor the keyboard moves, whose rows
 * are all mounted: `useVirtualRows().revealIndex` reveals a virtualized row.
 */
export function revealRow(row: HTMLElement) {
  const port = row.closest<HTMLElement>('[role="table"]')
  if (!port) return
  const portTop = port.getBoundingClientRect().top
  const head = port.querySelector('[role="rowgroup"]:has([role="columnheader"])')
  let visibleTop = head?.getBoundingClientRect().bottom ?? portTop
  // Each group around the row pins its heading below the ones around it.
  for (let group = row.parentElement; group && group !== port; group = group.parentElement) {
    const heading = group.querySelector(':scope > [data-pinned-heading]')
    if (heading && heading !== row) visibleTop += heading.getBoundingClientRect().height
  }
  // A pinned heading's own box stays put, so the row starts at its group's top.
  const { height, top } = row.getBoundingClientRect()
  const start =
    row.matches('[data-pinned-heading]') && row.parentElement
      ? row.parentElement.getBoundingClientRect().top
      : top
  const visibleBottom = portTop + port.clientHeight
  if (start < visibleTop || height > visibleBottom - visibleTop)
    port.scrollTop += start - visibleTop
  else if (start + height > visibleBottom) port.scrollTop += start + height - visibleBottom
}
