import type { LucideProps } from 'lucide-react'

export function ClockCheckIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9.061 4.586V9.086L12.061 10.586M16.561 9.086C16.561 7.635 16.14 6.215 15.348 4.998C14.557 3.781 13.43 2.82 12.103 2.231C10.777 1.642 9.308 1.451 7.875 1.681C6.442 1.91 5.106 2.551 4.03 3.524C2.953 4.498 2.182 5.763 1.811 7.166C1.439 8.569 1.483 10.05 1.936 11.428C2.389 12.807 3.233 14.025 4.364 14.934C5.496 15.843 6.867 16.404 8.311 16.549M16.561 12.086L12.436 16.211L10.561 14.336"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
