import type { LucideProps } from 'lucide-react'

export function CopyIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M3 12C2.175 12 1.5 11.325 1.5 10.5V3C1.5 2.175 2.175 1.5 3 1.5H10.5C11.325 1.5 12 2.175 12 3M7.5 6H15C15.828 6 16.5 6.672 16.5 7.5V15C16.5 15.828 15.828 16.5 15 16.5H7.5C6.672 16.5 6 15.828 6 15V7.5C6 6.672 6.672 6 7.5 6Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
