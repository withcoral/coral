import type { LucideProps } from 'lucide-react'

export function HandIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M13.5 8.25V4.5a1.5 1.5 0 0 0 -1.5 -1.5a1.5 1.5 0 0 0 -1.5 1.5M10.5 7.5V3a1.5 1.5 0 0 0 -1.5 -1.5a1.5 1.5 0 0 0 -1.5 1.5v1.5M7.5 7.875V4.5a1.5 1.5 0 0 0 -1.5 -1.5a1.5 1.5 0 0 0 -1.5 1.5v6M13.5 6a1.5 1.5 0 1 1 3 0v4.5a6 6 0 0 1 -6 6h-1.5c-2.1 0 -3.375 -0.645 -4.492 -1.755l-2.7 -2.7a1.5 1.5 0 0 1 2.123 -2.115L5.25 11.25"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
