import type { LucideProps } from 'lucide-react'

export function CircleQuestionMarkIcon({
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
      <path
        d="M6.817 6.75C6.994 6.249 7.342 5.826 7.8 5.557C8.258 5.288 8.797 5.189 9.32 5.279C9.844 5.369 10.319 5.641 10.661 6.048C11.004 6.454 11.191 6.969 11.19 7.5C11.19 9 8.94 9.75 8.94 9.75M9 12.75H9.008M16.5 9C16.5 13.142 13.142 16.5 9 16.5C4.858 16.5 1.5 13.142 1.5 9C1.5 4.858 4.858 1.5 9 1.5C13.142 1.5 16.5 4.858 16.5 9Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
