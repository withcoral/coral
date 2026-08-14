import type { LucideProps } from 'lucide-react'

export function BotIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 6V3H6M4.5 6H13.5C14.328 6 15 6.672 15 7.5V13.5C15 14.328 14.328 15 13.5 15H4.5C3.672 15 3 14.328 3 13.5V7.5C3 6.672 3.672 6 4.5 6ZM1.5 10.5H3M15 10.5H16.5M11.25 9.75V11.25M6.75 9.75V11.25"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
