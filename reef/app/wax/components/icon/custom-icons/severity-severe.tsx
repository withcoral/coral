import type { LucideProps } from 'lucide-react'

import { theme } from '@/wax/theme/theme.css'

export function SeveritySevereIcon({ size = 24, ...props }: LucideProps) {
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
        d="M3.75 15.75V11.25"
        stroke={theme.content.warning}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2.5"
      />
      <path
        d="M9 15.75V6.75"
        stroke={theme.content.warning}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2.5"
      />
      <path
        d="M14.25 15.75V2.25"
        stroke={theme.content.warning}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2.5"
      />
    </svg>
  )
}
