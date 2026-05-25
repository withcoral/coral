import type { LucideProps } from 'lucide-react'

export function ArrowUpIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
      <path d="M9 14.25V3.75M9 3.75L3.75 9M9 3.75L14.25 9" />
    </svg>
  )
}
