import { ScrollArea as BaseScrollArea } from '@base-ui/react/scroll-area'
import classNames from 'classnames'

import * as styles from './scroll-area.css'

interface ScrollAreaBaseProps extends React.HTMLAttributes<HTMLDivElement> {
  /** Constrains content width to viewport. Useful when you don't need horizontal scrolling. Defaults to false. */
  constrainWidth?: boolean
  /** Makes the internal content wrapper at least as tall as the scroll viewport. Defaults to false. */
  fillContent?: boolean
  /** Adds a gradient fade effect at the selected scroll edges. */
  fade?: 'both' | 'bottom' | 'horizontal' | 'none' | 'top'
  /** Whether to show the horizontal scrollbar. Defaults to false. */
  horizontal?: boolean
  ref?: React.Ref<HTMLDivElement>
  /** Axes that may scroll. Defaults to 'both'. */
  scrollDirection?: 'both' | 'horizontal' | 'vertical'
  /** Ref to the scrollable viewport element. Useful for virtualization libraries. */
  viewportRef?: React.Ref<HTMLDivElement>
  /** Class name applied to the scrollable viewport element. */
  viewportClassName?: string
  /** Defaults to '100%'. */
  width?: string
}

type ScrollAreaProps = (ScrollAreaWithHeight | ScrollAreaWithMaxHeight) &
  (ScrollAreaContentProps | ScrollAreaNativeViewportProps)

interface ScrollAreaContentProps {
  children: React.ReactNode
  renderViewport?: never
}

interface ScrollAreaNativeViewportProps {
  children?: never
  /** Native element that owns its own scrollable content, such as a textarea. */
  renderViewport: React.ReactElement
}

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
  fillContent = false,
  height = '100%',
  horizontal = false,
  maxHeight,
  ref,
  renderViewport,
  scrollDirection = 'both',
  style,
  viewportClassName,
  viewportRef,
  width = '100%',
  ...rest
}: ScrollAreaProps) {
  const contentStyle: React.CSSProperties | undefined =
    constrainWidth || fillContent
      ? {
          ...(constrainWidth ? { minWidth: 0 } : {}),
          ...(fillContent ? { display: 'flex', minHeight: '100%' } : {}),
        }
      : undefined

  const viewport = renderViewport ? (
    <BaseScrollArea.Viewport
      className={classNames(styles.viewport, styles.nativeViewportFade[fade], viewportClassName)}
      ref={viewportRef}
      render={renderViewport}
      style={getViewportStyle(scrollDirection)}
    />
  ) : (
    <BaseScrollArea.Viewport
      className={classNames(styles.viewport, styles.viewportFade[fade], viewportClassName)}
      ref={viewportRef}
      style={getViewportStyle(scrollDirection)}
    >
      <BaseScrollArea.Content className={styles.content} style={contentStyle}>
        {children}
      </BaseScrollArea.Content>
    </BaseScrollArea.Viewport>
  )

  return (
    <BaseScrollArea.Root
      className={classNames(styles.root, className)}
      ref={ref}
      style={{ height: maxHeight ? undefined : height, maxHeight, width, ...style }}
      {...rest}
    >
      {viewport}
      {scrollDirection !== 'horizontal' && (
        <BaseScrollArea.Scrollbar className={styles.scrollbar} orientation="vertical">
          <BaseScrollArea.Thumb className={styles.thumb} />
        </BaseScrollArea.Scrollbar>
      )}
      {horizontal && scrollDirection !== 'vertical' && (
        <BaseScrollArea.Scrollbar className={styles.scrollbar} orientation="horizontal">
          <BaseScrollArea.Thumb className={styles.thumb} />
        </BaseScrollArea.Scrollbar>
      )}
      {horizontal && scrollDirection === 'both' && (
        <BaseScrollArea.Corner className={styles.corner} />
      )}
    </BaseScrollArea.Root>
  )
}

function getViewportStyle(scrollDirection: NonNullable<ScrollAreaBaseProps['scrollDirection']>) {
  if (scrollDirection === 'horizontal') {
    return { overflowY: 'hidden' } as const
  }
  if (scrollDirection === 'vertical') {
    return { overflowX: 'hidden' } as const
  }
  return undefined
}
