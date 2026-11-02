import type { LucideProps } from 'lucide-react'

export function Maximize2Icon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M11.25 2.25H15.75M15.75 2.25V6.75M15.75 2.25L10.5 7.5M2.25 15.75L7.5 10.5M2.25 15.75H6.75M2.25 15.75V11.25"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
