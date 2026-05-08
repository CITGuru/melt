import type { FeatureIconName } from "@/lib/features";

export function FeatureIcon({
  name,
  size = 18,
}: {
  name: FeatureIconName;
  size?: number;
}) {
  const props = {
    width: size,
    height: size,
    viewBox: "0 0 24 24",
    fill: "none" as const,
    "aria-hidden": true,
  };
  switch (name) {
    case "routing":
      return (
        <svg {...props}>
          <path
            d="M4 6h6l4 12h6M4 18h6"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <circle cx="4" cy="6" r="1.6" fill="currentColor" />
          <circle cx="4" cy="18" r="1.6" fill="currentColor" />
          <circle cx="20" cy="18" r="1.6" fill="currentColor" />
        </svg>
      );
    case "split":
      return (
        <svg {...props}>
          <path
            d="M6 4v6a4 4 0 0 0 4 4h4a4 4 0 0 1 4 4v2M18 4v6a4 4 0 0 1-4 4h-4a4 4 0 0 0-4 4v2"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "loadBalance":
      // single input on the left, 3 outputs fanning right
      return (
        <svg {...props}>
          <circle cx="4" cy="12" r="1.8" fill="currentColor" />
          <circle cx="20" cy="5" r="1.6" fill="currentColor" />
          <circle cx="20" cy="12" r="1.6" fill="currentColor" />
          <circle cx="20" cy="19" r="1.6" fill="currentColor" />
          <path
            d="M6 12h6m0 0 6-7m-6 7h6m-6 0 6 7"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "warehouse":
      // a small warehouse / building with a door
      return (
        <svg {...props}>
          <path
            d="M3 10 12 4l9 6v9a1 1 0 0 1-1 1h-4v-7H8v7H4a1 1 0 0 1-1-1v-9Z"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinejoin="round"
          />
          <path
            d="M10 13h4"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
          />
        </svg>
      );
    case "stack":
      // three stacked layers — materialized views
      return (
        <svg {...props}>
          <path
            d="M12 3 3 7l9 4 9-4-9-4Z"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinejoin="round"
          />
          <path
            d="m3 12 9 4 9-4M3 17l9 4 9-4"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "delta":
      // refresh / delta arrow circling
      return (
        <svg {...props}>
          <path
            d="M4 12a8 8 0 0 1 14-5.3M20 12a8 8 0 0 1-14 5.3"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
          />
          <path
            d="M18 3v4h-4M6 21v-4h4"
            stroke="currentColor"
            strokeWidth="1.7"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
  }
}
