import type { LucideProps } from 'lucide-react'

export function Settings2Icon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M10.5 12.75H3.75M10.5 12.75C10.5 13.9926 11.5074 15 12.75 15C13.9926 15 15 13.9926 15 12.75C15 11.5074 13.9926 10.5 12.75 10.5C11.5074 10.5 10.5 11.5074 10.5 12.75ZM14.25 5.25H7.5M7.5 5.25C7.5 6.49264 6.49264 7.5 5.25 7.5C4.00736 7.5 3 6.49264 3 5.25C3 4.00736 4.00736 3 5.25 3C6.49264 3 7.5 4.00736 7.5 5.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
