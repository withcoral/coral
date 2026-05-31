import type { LucideProps } from 'lucide-react'

export function XIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      stroke={color}
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="1.25"
      viewBox="0 0 18 18"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path d="M13.5 4.5L4.5 13.5M4.5 4.5L13.5 13.5" />
    </svg>
  )
}
