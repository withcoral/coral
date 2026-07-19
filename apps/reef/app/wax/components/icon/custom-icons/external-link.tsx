import type { LucideProps } from 'lucide-react'

export function ExternalLinkIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M11.575 1.285H16.715M16.715 1.285V6.435M16.715 1.285L7.285 10.715M14.145 9.865V15.005C14.145 15.465 13.965 15.895 13.645 16.215C13.325 16.535 12.885 16.715 12.435 16.715H3.005C2.545 16.715 2.115 16.535 1.785 16.215C1.465 15.895 1.285 15.465 1.285 15.005V5.575C1.285 5.115 1.465 4.685 1.785 4.365C2.115 4.035 2.545 3.865 3.005 3.865H8.145"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
