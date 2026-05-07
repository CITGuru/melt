export function Cloud({ className = "" }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 240 120"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
      className={className}
    >
      <defs>
        <radialGradient id="cloudGrad" cx="50%" cy="40%" r="60%">
          <stop offset="0%" stopColor="#ffffff" />
          <stop offset="55%" stopColor="#ffffff" stopOpacity="0.92" />
          <stop offset="100%" stopColor="#ffffff" stopOpacity="0" />
        </radialGradient>
      </defs>
      <ellipse cx="80"  cy="76" rx="60" ry="22" fill="url(#cloudGrad)" />
      <ellipse cx="120" cy="60" rx="48" ry="26" fill="url(#cloudGrad)" />
      <ellipse cx="160" cy="74" rx="56" ry="22" fill="url(#cloudGrad)" />
      <ellipse cx="60"  cy="68" rx="34" ry="14" fill="url(#cloudGrad)" />
      <ellipse cx="190" cy="62" rx="30" ry="14" fill="url(#cloudGrad)" />
    </svg>
  );
}
