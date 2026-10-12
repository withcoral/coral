import { createElement } from 'react'
import {
  ReadingPosition as AreaReadingPosition,
  type ReadingPositionProps,
} from '../reading-area/reading-position'
import { windowRowHeight } from './table.css'

export { ReadingContext } from '../reading-area/reading-position'

/** Table geometry participates in the same pre-mutation reading owner as ordinary scroll areas. */
export function ReadingPosition(
  props: Pick<ReadingPositionProps, 'element' | 'options' | 'children'>,
) {
  return createElement(AreaReadingPosition, {
    ...props,
    headingSelector: '[role=rowgroup]:has([role=columnheader])',
    // Table.Group marks its heading row; reading position measures it at its group's top.
    pinnedSelector: '[data-pinned-heading]',
    windowRowHeight: windowRowHeight.slice(4, -1),
  })
}
