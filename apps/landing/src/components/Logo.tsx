export function Logo({ className = "" }: { className?: string }) {
  return (
    <span className={`inline-flex items-center gap-2 ${className}`}>
      <svg
        width="22"
        height="22"
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden
      >
        <defs>
          <linearGradient id="meltMark" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#FFB68A" />
            <stop offset="100%" stopColor="#F0541E" />
          </linearGradient>
        </defs>
        <path
          d="M12 2.4c3.5 4.2 6.8 7.5 6.8 12.1a6.8 6.8 0 1 1-13.6 0c0-4.6 3.3-7.9 6.8-12.1Z"
          fill="url(#meltMark)"
        />
        <path
          d="M9.6 13.6c.1 1.7 1.2 3 2.7 3"
          stroke="rgba(255,255,255,0.85)"
          strokeWidth="1.4"
          strokeLinecap="round"
          fill="none"
        />
      </svg>
      <span className="text-[17px] font-semibold tracking-tight text-ink">
        melt
      </span>
    </span>
  );
}
