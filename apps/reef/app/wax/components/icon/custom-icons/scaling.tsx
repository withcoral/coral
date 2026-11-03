import type { LucideProps } from 'lucide-react'

export function ScalingIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 2.25H3.75C2.92 2.25 2.25 2.92 2.25 3.75V14.25C2.25 15.08 2.92 15.75 3.75 15.75H14.25C15.08 15.75 15.75 15.08 15.75 14.25V9M12 2.25H15.75M15.75 2.25V6M15.75 2.25L6.75 11.25M6.75 11.25H10.5M6.75 11.25V7.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
