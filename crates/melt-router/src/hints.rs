//! Comment-hint parser for the dual-execution router.
//!
//! Lets operators stamp routing intent onto a query without changing
//! config, without touching `[sync].remote` patterns, and without
//! redeploying. Hints are recognised in **leading** SQL comments
//! (block or line) prefixed with `+`, mirroring the conventions in
//! Oracle, MySQL, Snowflake, and most query-rewriting tools.
//!
//! ## Recognised hints
//!
//! | Hint | Effect |
//! | --- | --- |
//! | `/*+ melt_route(snowflake) */` | Force passthrough; skip lake + hybrid evaluation entirely. |
//! | `/*+ melt_route(lake) */` | Force lake routing; bail-out reasons (translation failure, missing tables) propagate as errors instead of falling through to passthrough. |
//! | `/*+ melt_route(hybrid) */` | Force hybrid execution if any Remote tables are present. Bypasses size caps; useful for one-off operator-driven big scans. |
//! | `/*+ melt_strategy(attach) */` | Within hybrid: prefer Attach for every Remote node (still subject to runtime availability). |
//! | `/*+ melt_strategy(materialize) */` | Within hybrid: force Materialize for every Remote node (never use `sf_link.*` rewrites). |
//!
//! ## Parser rules
//!
//! - Only **leading** comments are scanned — comments interleaved
//!   with SQL keywords are ignored. This avoids false matches in
//!   query bodies (e.g. a string literal containing the hint
//!   syntax).
//! - Both `--` line comments and `/* ... */` block comments are
//!   accepted. The hint marker is `+` immediately after the comment
//!   opener (Oracle convention).
//! - Multiple hints in the same comment are honored
//!   (e.g. `/*+ melt_route(hybrid) melt_strategy(attach) */`).
//! - Unknown hints are silently ignored — never a parse error. This
//!   keeps SQL portable across systems that recognise different
//!   hint dialects.
//! - The parser DOES NOT execute the SQL; it's a pure pre-decision
//!   pass. Implementer:
//!   - `decide_inner` calls `parse_hints(sql)` once before
//!     classification.
//!   - The returned [`Hints`] override the relevant decisions.

/// Routing-override knobs derived from `/*+ ... */` comment hints.
/// `None` fields mean "no opinion — defer to the normal decision
/// path."
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hints {
    pub route: Option<RouteHint>,
    pub strategy: Option<StrategyHint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteHint {
    Snowflake,
    Lake,
    Hybrid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrategyHint {
    Attach,
    Materialize,
}

impl Hints {
    pub fn is_empty(&self) -> bool {
        self.route.is_none() && self.strategy.is_none()
    }
}

/// Extract leading hint comments from `sql` and parse known hints.
/// Always returns a value; unknown / malformed hints become
/// [`Hints::default`].
pub fn parse_hints(sql: &str) -> Hints {
    let mut hints = Hints::default();
    let mut s = sql;
    loop {
        // Skip whitespace.
        s = s.trim_start();
        if s.is_empty() {
            break;
        }
        // Block comment.
        if let Some(rest) = s.strip_prefix("/*") {
            // Find closing `*/`. If the file is malformed (no close),
            // we just bail — the SQL parser later will produce a
            // proper error.
            let Some(end) = rest.find("*/") else {
                break;
            };
            let body = &rest[..end];
            apply_comment_body(body, &mut hints);
            s = &rest[end + 2..];
            continue;
        }
        // Line comment.
        if let Some(rest) = s.strip_prefix("--") {
            let line_end = rest.find('\n').unwrap_or(rest.len());
            let body = &rest[..line_end];
            apply_comment_body(body, &mut hints);
            s = &rest[(line_end + 1).min(rest.len())..];
            continue;
        }
        // First non-whitespace, non-comment token — leading-comment
        // window is over.
        break;
    }
    hints
}

/// Inspect a single comment body for the `+ melt_*` hint marker. The
/// `+` must be the FIRST non-whitespace char inside the comment
/// (Oracle/Snowflake convention).
fn apply_comment_body(body: &str, hints: &mut Hints) {
    let body = body.trim();
    let body = match body.strip_prefix('+') {
        Some(b) => b.trim(),
        None => return,
    };
    // Recognise `melt_route(<x>)` and `melt_strategy(<x>)`.
    // Tokenize on whitespace AND commas so common separators work.
    for token in body.split(|c: char| c.is_whitespace() || c == ',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if let Some(arg) = strip_call(token, "melt_route") {
            match arg.to_ascii_lowercase().as_str() {
                "snowflake" | "passthrough" => hints.route = Some(RouteHint::Snowflake),
                "lake" | "local" => hints.route = Some(RouteHint::Lake),
                "hybrid" | "remote" => hints.route = Some(RouteHint::Hybrid),
                _ => {}
            }
        } else if let Some(arg) = strip_call(token, "melt_strategy") {
            match arg.to_ascii_lowercase().as_str() {
                "attach" => hints.strategy = Some(StrategyHint::Attach),
                "materialize" | "materialise" => hints.strategy = Some(StrategyHint::Materialize),
                _ => {}
            }
        }
    }
}

/// Match `<name>(<arg>)` and return the inner arg, or `None`.
fn strip_call<'a>(token: &'a str, name: &str) -> Option<&'a str> {
    let rest = token.strip_prefix(name)?;
    let rest = rest.strip_prefix('(')?;
    rest.strip_suffix(')')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sql_no_hints() {
        assert!(parse_hints("").is_empty());
        assert!(parse_hints("SELECT 1").is_empty());
    }

    #[test]
    fn block_hint_route_lake() {
        let h = parse_hints("/*+ melt_route(lake) */ SELECT 1");
        assert_eq!(h.route, Some(RouteHint::Lake));
        assert_eq!(h.strategy, None);
    }

    #[test]
    fn line_hint_route_snowflake() {
        let h = parse_hints("--+ melt_route(snowflake)\nSELECT 1");
        assert_eq!(h.route, Some(RouteHint::Snowflake));
    }

    #[test]
    fn combined_hints_in_one_comment() {
        let h = parse_hints("/*+ melt_route(hybrid) melt_strategy(materialize) */ SELECT 1");
        assert_eq!(h.route, Some(RouteHint::Hybrid));
        assert_eq!(h.strategy, Some(StrategyHint::Materialize));
    }

    #[test]
    fn comma_separated_hints_in_one_comment() {
        let h = parse_hints("/*+ melt_route(hybrid), melt_strategy(attach) */ SELECT 1");
        assert_eq!(h.route, Some(RouteHint::Hybrid));
        assert_eq!(h.strategy, Some(StrategyHint::Attach));
    }

    #[test]
    fn multiple_leading_comments_stack() {
        let h = parse_hints(
            "/*+ melt_route(hybrid) */\n\
             --+ melt_strategy(attach)\n\
             SELECT 1",
        );
        assert_eq!(h.route, Some(RouteHint::Hybrid));
        assert_eq!(h.strategy, Some(StrategyHint::Attach));
    }

    #[test]
    fn unknown_hints_silently_ignored() {
        let h = parse_hints("/*+ melt_route(banana) other_hint(yes) */ SELECT 1");
        assert!(h.is_empty());
    }

    #[test]
    fn hint_without_plus_marker_is_ignored() {
        // Plain comment, no `+` prefix — not a hint.
        let h = parse_hints("/* melt_route(lake) */ SELECT 1");
        assert!(h.is_empty());
    }

    #[test]
    fn aliases_supported() {
        assert_eq!(
            parse_hints("/*+ melt_route(passthrough) */ x").route,
            Some(RouteHint::Snowflake)
        );
        assert_eq!(
            parse_hints("/*+ melt_route(local) */ x").route,
            Some(RouteHint::Lake)
        );
        assert_eq!(
            parse_hints("/*+ melt_route(remote) */ x").route,
            Some(RouteHint::Hybrid)
        );
        assert_eq!(
            parse_hints("/*+ melt_strategy(materialise) */ x").strategy,
            Some(StrategyHint::Materialize)
        );
    }

    #[test]
    fn hint_inside_query_body_is_ignored() {
        // Comment after the first SQL token isn't a leading
        // comment, so we don't scan it.
        let h = parse_hints("SELECT /*+ melt_route(lake) */ 1");
        assert!(h.is_empty());
    }

    #[test]
    fn case_insensitive_arg() {
        assert_eq!(
            parse_hints("/*+ melt_route(SNOWFLAKE) */ x").route,
            Some(RouteHint::Snowflake)
        );
        assert_eq!(
            parse_hints("/*+ melt_strategy(Attach) */ x").strategy,
            Some(StrategyHint::Attach)
        );
    }

    #[test]
    fn unterminated_block_comment_bails() {
        let h = parse_hints("/*+ melt_route(lake) SELECT 1");
        assert!(h.is_empty());
    }

    #[test]
    fn whitespace_around_comment_ok() {
        let h = parse_hints("\n\n  /*+ melt_route(lake) */\n SELECT 1");
        assert_eq!(h.route, Some(RouteHint::Lake));
    }
}
