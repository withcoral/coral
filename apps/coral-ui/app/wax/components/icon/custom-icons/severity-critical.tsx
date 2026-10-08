import type { LucideProps } from 'lucide-react'

import { theme } from '@/wax/theme/theme.css'

export function SeverityCriticalIcon({ size = 24, ...props }: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      viewBox="0 0 18 18"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path
        d="M9 0.875C13.4873 0.875 17.125 4.51269 17.125 9C17.125 13.4873 13.4873 17.125 9 17.125C4.51269 17.125 0.875 13.4873 0.875 9C0.875 4.51269 4.51269 0.875 9 0.875ZM9 11.125C8.51675 11.125 8.125 11.5168 8.125 12C8.125 12.4832 8.51675 12.875 9 12.875H9.00781C9.49092 12.8748 9.88281 12.4831 9.88281 12C9.88281 11.5169 9.49092 11.1252 9.00781 11.125H9ZM9 5.125C8.51675 5.125 8.125 5.51675 8.125 6V9C8.125 9.48325 8.51675 9.875 9 9.875C9.48325 9.875 9.875 9.48325 9.875 9V6C9.875 5.51675 9.48325 5.125 9 5.125Z"
        fill={theme.content.error}
      />
    </svg>
  )
}
