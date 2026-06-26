import type { LucideProps } from 'lucide-react'

export function MessageCircleIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M2.281 12.214C2.391 12.492 2.415 12.797 2.351 13.089L1.552 15.556C1.527 15.682 1.533 15.811 1.572 15.933C1.61 16.055 1.679 16.165 1.772 16.253C1.865 16.34 1.978 16.403 2.102 16.434C2.226 16.466 2.356 16.465 2.479 16.432L5.039 15.684C5.315 15.629 5.6 15.653 5.863 15.753C7.465 16.501 9.279 16.659 10.986 16.2C12.693 15.74 14.183 14.693 15.192 13.242C16.202 11.792 16.667 10.031 16.505 8.271C16.343 6.511 15.565 4.864 14.308 3.622C13.05 2.38 11.394 1.622 9.632 1.481C7.87 1.341 6.115 1.827 4.677 2.855C3.239 3.882 2.21 5.385 1.771 7.097C1.333 8.809 1.513 10.621 2.281 12.214Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
