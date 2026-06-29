import type { LucideProps } from 'lucide-react'

export function TimerIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M7.5 1.5H10.5M9 10.5L11.25 8.25M15 10.5C15 13.814 12.314 16.5 9 16.5C5.686 16.5 3 13.814 3 10.5C3 7.186 5.686 4.5 9 4.5C12.314 4.5 15 7.186 15 10.5Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
