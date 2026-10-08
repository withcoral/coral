import type { LucideProps } from 'lucide-react'

export function TriangleAlertFullIcon({
  color = 'currentColor',
  size = 24,
  ...props
}: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      viewBox="0 0 18 18"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path d="M9 2.25L17 15.75H1L9 2.25Z" fill={color} fillOpacity="0.1" />
      <path
        d="M9 6.75V9.75M9 12.75H9.0075M17 15H1L9 1.5L17 15Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
