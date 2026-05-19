import { useEffect, useState } from 'react'

/**
 * Detects whether an element's content is truncated (either by text-overflow: ellipsis
 * for single-line or -webkit-line-clamp for multi-line).
 *
 * @param elementRef - A ref to the element to observe
 * @param enabled - Whether to enable truncation detection (default: true)
 * @returns true if the element's content is truncated, false otherwise
 */
export function useTruncationDetection(elementRef: React.RefObject<HTMLElement | null>, enabled = true): boolean {
  const [isTruncated, setIsTruncated] = useState(false)

  useEffect(() => {
    if (!enabled) {
      return
    }

    const element = elementRef.current
    if (!element) {
      return
    }

    const checkTruncation = () => {
      const isHorizontallyTruncated = element.scrollWidth > element.clientWidth
      const isVerticallyTruncated = element.scrollHeight > element.clientHeight
      setIsTruncated(isHorizontallyTruncated || isVerticallyTruncated)
    }

    checkTruncation()

    const resizeObserver = new ResizeObserver(checkTruncation)
    resizeObserver.observe(element)

    const mutationObserver = new MutationObserver(checkTruncation)
    mutationObserver.observe(element, {
      characterData: true,
      childList: true,
      subtree: true,
    })

    return () => {
      resizeObserver.disconnect()
      mutationObserver.disconnect()
    }
  }, [elementRef, enabled])

  return isTruncated
}
