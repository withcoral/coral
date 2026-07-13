import type { LucideProps } from 'lucide-react'

export function CalendarIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M6 1.5V4.5M12 1.5V4.5M2.25 7.5H15.75M3.75 3H14.25C15.078 3 15.75 3.672 15.75 4.5V15C15.75 15.828 15.078 16.5 14.25 16.5H3.75C2.922 16.5 2.25 15.828 2.25 15V4.5C2.25 3.672 2.922 3 3.75 3Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
