import type { LucideProps } from 'lucide-react'

export function PanelLeftIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M6.75 2.25V15.75M3.75 2.25H14.25C15.078 2.25 15.75 2.922 15.75 3.75V14.25C15.75 15.078 15.078 15.75 14.25 15.75H3.75C2.922 15.75 2.25 15.078 2.25 14.25V3.75C2.25 2.922 2.922 2.25 3.75 2.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
