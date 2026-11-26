/**
 * What the records table shows as selected, by label: the selected record's own row, or
 * for an Event, its mark on its Run's row. A story's play reads the selection through this.
 */
export function selectedRecords(table: Element): (string | null | undefined)[] {
  const marks = [...table.querySelectorAll('[data-timeline-mark][aria-current="true"]')]
  if (marks.length) return marks.map((mark) => mark.getAttribute('aria-label'))
  return [...table.querySelectorAll('[role="row"][aria-selected="true"]')].map(
    (row) => row.querySelector('a[href]')?.textContent,
  )
}
