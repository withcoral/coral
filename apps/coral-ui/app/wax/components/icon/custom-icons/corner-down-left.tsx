import type { LucideProps } from 'lucide-react'

export function CornerDownLeftIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M15 3V8.25C15 9.04565 14.6839 9.80871 14.1213 10.3713C13.5587 10.9339 12.7956 11.25 12 11.25H3M3 11.25L6.75 7.5M3 11.25L6.75 15"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
