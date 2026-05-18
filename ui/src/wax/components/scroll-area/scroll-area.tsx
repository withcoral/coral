import { ScrollArea as BaseScrollArea } from '@base-ui-components/react/scroll-area'
import classNames from 'classnames'

import * as styles from './scroll-area.css'

interface ScrollAreaBaseProps extends React.HTMLAttributes<HTMLDivElement> {
  children: React.ReactNode
  /** Constrains content width to viewport. Useful when you don't need horizontal scrolling. Defaults to false. */
  constrainWidth?: boolean
  /** Adds gradient fade effect at scroll edges. */
  fade?: 'both' | 'bottom' | 'none' | 'top'
  /** Whether to show the horizontal scrollbar. Defaults to false. */
  horizontal?: boolean
  ref?: React.Ref<HTMLDivElement>
  /** Ref to the scrollable viewport element. Useful for virtualization libraries. */
  viewportRef?: React.Ref<HTMLDivElement>
  /** Defaults to '100%'. */
  width?: string
}

type ScrollAreaProps = ScrollAreaWithHeight | ScrollAreaWithMaxHeight

interface ScrollAreaWithHeight extends ScrollAreaBaseProps {
  /** Fixed height. Defaults to '100%'. */
  height?: string
  maxHeight?: never
}

interface ScrollAreaWithMaxHeight extends ScrollAreaBaseProps {
  height?: never
  maxHeight: string
}

/**
 * A scrollable area with styled scrollbars.
 *
 * @example
 * ```tsx
 * <ScrollArea.Container height="300px">
 *   <p>Scrollable content...</p>
 * </ScrollArea.Container>
 *
 * // With virtualization
 * <ScrollArea.Container viewportRef={scrollContainerRef}>
 *   <VirtualizedList />
 * </ScrollArea.Container>
 * ```
 */
export function Container({
  children,
  className,
  constrainWidth = false,
  fade = 'both',
  height = '100%',
  horizontal = false,
  maxHeight,
  ref,
  style,
  viewportRef,
  width = '100%',
  ...rest
}: ScrollAreaProps) {
  return (
    <BaseScrollArea.Root
      className={classNames(styles.root, className)}
      ref={ref}
      style={{ height: maxHeight ? undefined : height, maxHeight, width, ...style }}
      {...rest}
    >
      <BaseScrollArea.Viewport className={classNames(styles.viewport, styles.viewportFade[fade])} ref={viewportRef}>
        <BaseScrollArea.Content className={styles.content} style={constrainWidth ? { minWidth: 0 } : undefined}>
          {children}
        </BaseScrollArea.Content>
      </BaseScrollArea.Viewport>
      <BaseScrollArea.Scrollbar className={styles.scrollbar} orientation="vertical">
        <BaseScrollArea.Thumb className={styles.thumb} />
      </BaseScrollArea.Scrollbar>
      {horizontal && (
        <BaseScrollArea.Scrollbar className={styles.scrollbar} orientation="horizontal">
          <BaseScrollArea.Thumb className={styles.thumb} />
        </BaseScrollArea.Scrollbar>
      )}
      {horizontal && <BaseScrollArea.Corner className={styles.corner} />}
    </BaseScrollArea.Root>
  )
}
