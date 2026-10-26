import { useEffect, useState } from 'react'

/**
 * Detects whether an element's content is truncated (either by text-overflow: ellipsis
 * for single-line or -webkit-line-clamp for multi-line).
 *
 * Pass the element itself, held in state through a callback ref, so an element
 * that mounts later is still observed.
 *
 * @param element - The element to observe, or null before it mounts
 * @param enabled - Whether to enable truncation detection (default: true)
 * @returns true if the element's content is truncated, false otherwise
 */
export function useTruncationDetection(element: HTMLElement | null, enabled = true): boolean {
  const [isTruncated, setIsTruncated] = useState(false)

  useEffect(() => {
    if (!enabled || !element) {
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
  }, [element, enabled])

  return isTruncated
}
