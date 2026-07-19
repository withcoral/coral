import type { LucideProps } from 'lucide-react'

export function CircleDashedIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M7.575 1.636C8.516 1.454 9.484 1.454 10.425 1.636M10.425 16.363C9.484 16.546 8.516 16.546 7.575 16.363M13.207 2.791C14.003 3.33 14.688 4.018 15.224 4.816M1.636 10.425C1.454 9.484 1.454 8.516 1.636 7.575M15.209 13.207C14.67 14.003 13.982 14.688 13.184 15.224M16.363 7.575C16.546 8.516 16.546 9.484 16.363 10.425M2.791 4.793C3.33 3.997 4.018 3.312 4.816 2.776M4.793 15.209C3.997 14.67 3.312 13.982 2.776 13.184"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
