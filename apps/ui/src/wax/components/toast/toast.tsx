import type React from 'react'

import { Bounce, toast, type ToastOptions } from 'react-toastify'

import { ToastContent } from './toast-content'

export const DEFAULT_TOAST_DURATION_MS = 4000

export interface ToastAction {
  label: string
  onClick: () => void
}

export interface ToastResult {
  dismiss: () => void
  id: number | string
}

export interface ToastValues {
  /**
   * Optional action button displayed below the toast message.
   */
  action?: ToastAction
  description?: React.ReactNode
  durationMs?: number
  onDismiss?: () => void
  /**
   * Set the percentage for the controlled progress bar. Value must be between 0 and 1.
   */
  progress?: number
  title: React.ReactNode
}

export type ToastVariant = 'error' | 'neutral' | 'success' | 'warning'

/**
 * Display a toast notification with title and optional description.
 *
 * @param variant - The type of toast: 'error', 'success', 'warning', or 'neutral'
 * @param options - Toast configuration options
 * @param options.title - Main message to display (required)
 * @param options.description - Optional secondary message
 * @param options.durationMs - Auto-close duration in milliseconds (default: 4000)
 * @param options.onDismiss - Callback when toast is dismissed
 * @param options.progress - Optional progress value between 0 and 1 to show progress bar
 * @returns ToastResult with dismiss() method and toast id
 *
 * @example
 * ```tsx
 * addToast('success', { title: 'Saved!' })
 * addToast('error', { title: 'Failed to save', description: 'Please try again' })
 * addToast('neutral', { title: 'Processing...', progress: 0.5 })
 * ```
 */
export function addToast(variant: ToastVariant, options: ToastValues): ToastResult {
  const {
    action,
    description,
    durationMs = DEFAULT_TOAST_DURATION_MS,
    onDismiss,
    progress,
    title,
  } = options

  const toastOptions: ToastOptions = {
    autoClose: durationMs === Infinity ? false : durationMs,
    closeOnClick: true,
    draggable: true,
    hideProgressBar: progress === undefined,
    onClose: onDismiss,
    pauseOnHover: true,
    position: 'top-right',
    progress: progress,
    theme: 'dark',
    transition: Bounce,
  }

  let id: number | string

  const renderToast = () => (
    <ToastContent
      action={action}
      description={description}
      onClose={() => toast.dismiss(id)}
      title={title}
      variant={variant}
    />
  )

  switch (variant) {
    case 'error':
      id = toast.error(renderToast, toastOptions)
      break
    case 'neutral':
      id = toast.info(renderToast, toastOptions)
      break
    case 'success':
      id = toast.success(renderToast, toastOptions)
      break
    case 'warning':
      id = toast.warning(renderToast, toastOptions)
      break
  }

  return {
    dismiss: () => toast.dismiss(id),
    id,
  }
}

export function dismissAllToasts() {
  toast.dismiss()
}
