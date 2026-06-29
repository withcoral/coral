import { addToast } from '@/wax/components/toast'

export async function copyTextToClipboard(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text)
    addToast('success', { title: 'Link copied' })
  } catch (err) {
    console.error('Failed to copy:', err)
    addToast('error', { title: 'Failed to copy link' })
  }
}
