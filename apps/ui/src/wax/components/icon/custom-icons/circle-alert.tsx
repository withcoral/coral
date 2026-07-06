import type { LucideProps } from 'lucide-react'

export function CircleAlertIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
      <path d="M9 6V9M9 12H9.008M16.5 9C16.5 13.142 13.142 16.5 9 16.5C4.858 16.5 1.5 13.142 1.5 9C1.5 4.858 4.858 1.5 9 1.5C13.142 1.5 16.5 4.858 16.5 9Z" />
    </svg>
  )
}
