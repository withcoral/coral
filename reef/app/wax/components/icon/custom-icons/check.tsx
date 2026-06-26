import type { LucideProps } from 'lucide-react'

export function CheckIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M15 4.875L6.75 13.125L3 9.375"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
