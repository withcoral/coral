// Controls that a press activates: they keep Enter and Space.
const ACTIVATED_CONTROLS = 'a, button, input, label, [role="radio"], [role="checkbox"], [tabindex]'

// Controls that read these keys themselves: a radio group or menu moves with
// the arrows, and a field types the space.
const KEY_CONTROLS =
  'textarea, select, [contenteditable="true"], [role="radiogroup"], [role="menu"], [role="listbox"], [role="combobox"], [role="slider"], [role="tablist"], input'

/**
 * Whether a key belongs to a table's keyboard cursor rather than to what has
 * focus. A control that reads the key keeps it; so does any dialog unless
 * `inDialog` says the caller is the dialog the cursor drives. A field whose id
 * is `field` hands the key on. With `overControls`, the key also works from a
 * focused button, link, or checkbox, which otherwise keep the Enter and Space
 * that activate them. `within`, when given, narrows that activated-control
 * exclusion to controls inside it: a button or link elsewhere on the page —
 * such as one the navigation that opened this table left focused — keeps its
 * own Enter and Space, but does not block the table's keys.
 */
export function cursorKeyApplies(
  event: KeyboardEvent,
  {
    overControls,
    inDialog = false,
    field,
    within,
  }: { overControls: boolean; inDialog?: boolean; field?: string; within?: Element | null },
) {
  if (event.defaultPrevented || event.isComposing) return false
  const target = event.target instanceof Element ? event.target : null
  // The table scrolls, so it takes focus, but it is not a control in a row.
  if (!target || target === document.body || target.matches('[role="table"]')) return true
  if (field && target.id === field) return true
  const activated = target.closest(ACTIVATED_CONTROLS)
  if (!overControls && activated && (!within || within.contains(activated))) return false
  if (target.closest(KEY_CONTROLS)) return false
  return inDialog || !target.closest('[role="dialog"], [role="alertdialog"]')
}
