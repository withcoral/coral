import type { LucideProps } from 'lucide-react'

export function ArrowDownIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
      <path d="M9 3.75V14.25M9 14.25L3.75 9M9 14.25L14.25 9" />
    </svg>
  )
}
