import { ImageResponse } from "next/og";

export const runtime = "edge";

export const alt = "Melt — cut your Snowflake bill, change one connection string";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default async function OpengraphImage() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          padding: "72px 80px",
          backgroundColor: "#cad7eb",
          backgroundImage:
            "linear-gradient(180deg, #c7d6ee 0%, #dbe4f2 45%, #eef3fb 100%)",
          fontFamily: "system-ui, -apple-system, Helvetica, sans-serif",
        }}
      >
        {/* Top-left: logo + wordmark */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 16,
          }}
        >
          <svg
            width="48"
            height="48"
            viewBox="0 0 24 24"
            xmlns="http://www.w3.org/2000/svg"
          >
            <defs>
              <linearGradient id="og-mark" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#FFB68A" />
                <stop offset="100%" stopColor="#F0541E" />
              </linearGradient>
            </defs>
            <path
              d="M12 2.4c3.5 4.2 6.8 7.5 6.8 12.1a6.8 6.8 0 1 1-13.6 0c0-4.6 3.3-7.9 6.8-12.1Z"
              fill="url(#og-mark)"
            />
            <path
              d="M9.6 13.6c.1 1.7 1.2 3 2.7 3"
              stroke="#ffffff"
              strokeOpacity="0.9"
              strokeWidth="1.4"
              strokeLinecap="round"
              fill="none"
            />
          </svg>
          <div
            style={{
              fontSize: 36,
              fontWeight: 600,
              color: "#0e1320",
              letterSpacing: "-0.02em",
            }}
          >
            melt
          </div>
        </div>

        {/* Middle: big headline */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            flexGrow: 1,
            justifyContent: "center",
            paddingTop: 24,
          }}
        >
          <div
            style={{
              fontSize: 78,
              fontWeight: 600,
              color: "#0e1320",
              letterSpacing: "-0.025em",
              lineHeight: 1.04,
              maxWidth: 980,
            }}
          >
            Cut your Snowflake bill, change one connection string.
          </div>
        </div>

        {/* Bottom: subtitle */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 16,
            fontSize: 24,
            color: "#5b6478",
          }}
        >
          <span>meltcomputing.com</span>
        </div>
      </div>
    ),
    { ...size },
  );
}
