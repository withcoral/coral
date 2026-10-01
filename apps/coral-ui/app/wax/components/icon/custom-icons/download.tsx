import type { LucideProps } from 'lucide-react'

export function DownloadIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 11.25V2.25M9 11.25L5.25 7.5M9 11.25L12.75 7.5M15.75 11.25V14.25C15.75 14.648 15.592 15.029 15.311 15.311C15.029 15.592 14.648 15.75 14.25 15.75H3.75C3.352 15.75 2.971 15.592 2.689 15.311C2.408 15.029 2.25 14.648 2.25 14.25V11.25"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
