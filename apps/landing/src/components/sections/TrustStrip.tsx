const tools = [
  "Snowflake driver",
  "JDBC",
  "ODBC",
  "dbt",
  "Looker",
  "Sigma",
  "Hex",
  "Tableau",
  "Python connector",
  "Go driver",
  "Rust connector",
];

export function TrustStrip({ label, items }: { label?: string, items?: string[] }) {
  const itemList = items ? [...items, ...items] : [...tools, ...tools];
  return (
    <section className="relative py-10 bg-bg-2 border-y border-line">
      {label ? (
        <p className="text-center text-xs uppercase tracking-[0.18em] text-muted mb-5">
          {label}
        </p>
      ) : null}
      <div
        className="relative overflow-hidden"
        style={{
          maskImage:
            "linear-gradient(90deg, transparent, black 12%, black 88%, transparent)",
          WebkitMaskImage:
            "linear-gradient(90deg, transparent, black 12%, black 88%, transparent)",
        }}
      >
        <div className="marquee-track flex w-max gap-12 px-6">
          {itemList.map((t, i) => (
            <span
              key={i}
              className="text-xl md:text-2xl font-semibold tracking-tight text-ink/40 whitespace-nowrap"
            >
              {t}
            </span>
          ))}
        </div>
      </div>
    </section>
  );
}
