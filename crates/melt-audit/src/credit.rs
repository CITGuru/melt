//! Snowflake credit math for the audit's `$/query` baseline.
//!
//! The credit-per-hour table is the standard list-price scale; the
//! audit takes `--credit-price` as a CLI flag (default $3.00 in
//! [`crate::DEFAULT_CREDIT_PRICE_USD`]).

/// Standard Snowflake credits-per-hour by warehouse size (spec §4).
/// Returns 0 when the size string is unrecognized — defensive default
/// so unknown sizes fall out as zero-cost rather than panicking.
pub fn credits_per_hour(warehouse_size: &str) -> f64 {
    match normalize_size(warehouse_size).as_str() {
        "XS" | "X-SMALL" | "XSMALL" => 1.0,
        "S" | "SMALL" => 2.0,
        "M" | "MEDIUM" => 4.0,
        "L" | "LARGE" => 8.0,
        "XL" | "X-LARGE" | "XLARGE" => 16.0,
        "XXL" | "2XL" | "2X-LARGE" | "2XLARGE" => 32.0,
        "XXXL" | "3XL" | "3X-LARGE" | "3XLARGE" => 64.0,
        "XXXXL" | "4XL" | "4X-LARGE" | "4XLARGE" => 128.0,
        "5XL" | "5X-LARGE" | "5XLARGE" => 256.0,
        "6XL" | "6X-LARGE" | "6XLARGE" => 512.0,
        _ => 0.0,
    }
}

/// Credits used by a single query: `(execution_time_ms / 3_600_000) ×
/// credits_per_hour(size)`. Cloud-services credits are ignored per
/// spec §4 disclaimers.
pub fn credits_used(execution_time_ms: u64, warehouse_size: Option<&str>) -> f64 {
    let Some(size) = warehouse_size else {
        return 0.0;
    };
    let hourly = credits_per_hour(size);
    if hourly == 0.0 {
        return 0.0;
    }
    (execution_time_ms as f64 / 3_600_000.0) * hourly
}

/// Convert credits to USD at the operator-supplied `--credit-price`.
pub fn dollars(credits: f64, credit_price_usd: f64) -> f64 {
    credits * credit_price_usd
}

fn normalize_size(s: &str) -> String {
    s.trim().to_ascii_uppercase().replace([' ', '_'], "-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_sizes() {
        assert_eq!(credits_per_hour("X-Small"), 1.0);
        assert_eq!(credits_per_hour("XSMALL"), 1.0);
        assert_eq!(credits_per_hour("SMALL"), 2.0);
        assert_eq!(credits_per_hour("Medium"), 4.0);
        assert_eq!(credits_per_hour("LARGE"), 8.0);
        assert_eq!(credits_per_hour("X-LARGE"), 16.0);
        assert_eq!(credits_per_hour("2X-LARGE"), 32.0);
        assert_eq!(credits_per_hour("3X-LARGE"), 64.0);
        assert_eq!(credits_per_hour("4X-LARGE"), 128.0);
    }

    #[test]
    fn unknown_size_zero() {
        assert_eq!(credits_per_hour("NOT-A-SIZE"), 0.0);
        assert_eq!(credits_used(60_000, Some("NOT-A-SIZE")), 0.0);
        assert_eq!(credits_used(60_000, None), 0.0);
    }

    #[test]
    fn one_hour_xsmall_one_credit() {
        let credits = credits_used(3_600_000, Some("X-Small"));
        assert!((credits - 1.0).abs() < 1e-9, "credits = {credits}");
        assert!((dollars(credits, 3.0) - 3.0).abs() < 1e-9);
    }
}
