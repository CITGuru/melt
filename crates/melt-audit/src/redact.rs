//! Literal redaction. Mandatory on every output (spec §5) — even when
//! the operator never runs `melt audit share`. `QUERY_TEXT` from
//! `ACCOUNT_USAGE.QUERY_HISTORY` can contain pasted PII / secrets;
//! we replace string and numeric literals with `?` before any of it
//! lands in `top_patterns` / JSON / talking-points.
//!
//! The implementation is a small lexer over the SQL text rather than
//! an AST round-trip: it preserves the original SQL shape (identifier
//! casing, whitespace) so patterns are still recognizable, and it
//! works for queries that don't parse cleanly (CTAS with vendor
//! syntax, etc.) — those still need their literals scrubbed.

/// Replace all string and numeric literals in `sql` with `?`.
///
/// Handled:
///
/// * `'single quoted'` → `?` (incl. `''`-escaped quotes)
/// * `"double quoted"` is left alone — those are identifiers in
///   ANSI/Snowflake SQL, not literals.
/// * `$$dollar quoted$$` → `?`
/// * `--line comment` and `/* block comment */` → preserved (they
///   already won't match a literal grouping key, but stripping them
///   would over-merge dialect markers).
/// * unquoted numerics (`12345`, `1.5e3`, `.25`, `-7`) → `?`. The
///   leading `-` is left where it is; the digits collapse to `?`.
/// * keywords like `NULL`, `TRUE`, `FALSE` → unchanged. They're
///   load-bearing for routability classification.
pub fn redact_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];

        // Line comment: copy through to newline.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }

        // Block comment: copy through to */ (or EOF).
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push_str("/*");
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push_str("*/");
                    i += 2;
                    break;
                }
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }

        // Single-quoted string. SQL doubles the quote to escape it,
        // so we close only on a single `'` not followed by another.
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push('?');
            continue;
        }

        // Dollar-quoted string. Snowflake supports `$$body$$` (no tag)
        // and `$tag$body$tag$`. We handle both.
        if b == b'$' {
            if let Some(end) = scan_dollar_quote(bytes, i) {
                out.push('?');
                i = end;
                continue;
            }
        }

        // Double-quoted identifiers: preserve verbatim so the output
        // still names the table.
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'"' {
                    if i < bytes.len() && bytes[i] == b'"' {
                        out.push('"');
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        // Numeric literal — must not start mid-identifier (SELECT a1
        // is one ident, not `a` + `1`). We detect digit boundaries by
        // checking the previous output character.
        if b.is_ascii_digit() || (b == b'.' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let prev = out.chars().last();
            let starts_token = match prev {
                None => true,
                Some(c) => !c.is_ascii_alphanumeric() && c != '_',
            };
            if starts_token {
                while i < bytes.len()
                    && (bytes[i].is_ascii_digit()
                        || bytes[i] == b'.'
                        || bytes[i] == b'e'
                        || bytes[i] == b'E'
                        || ((bytes[i] == b'+' || bytes[i] == b'-')
                            && i > 0
                            && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E')))
                {
                    i += 1;
                }
                out.push('?');
                continue;
            }
        }

        out.push(b as char);
        i += 1;
    }

    out
}

fn scan_dollar_quote(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'$');
    // tag = bytes between $ ... $
    let tag_start = start + 1;
    let mut tag_end = tag_start;
    while tag_end < bytes.len() {
        let c = bytes[tag_end];
        if c == b'$' {
            break;
        }
        // Tag chars are letters, digits, underscores. Anything else
        // (whitespace, parens) means this `$` was not a quote opener.
        if !c.is_ascii_alphanumeric() && c != b'_' {
            return None;
        }
        tag_end += 1;
    }
    if tag_end >= bytes.len() {
        return None;
    }
    let tag = &bytes[tag_start..tag_end];
    let body_start = tag_end + 1;
    // Find $tag$ closer.
    let mut i = body_start;
    while i + tag.len() + 1 < bytes.len() {
        if bytes[i] == b'$' && bytes[i + 1..].starts_with(tag) && bytes[i + 1 + tag.len()] == b'$' {
            return Some(i + 2 + tag.len());
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(sql: &str) -> String {
        redact_literals(sql)
    }

    #[test]
    fn string_literals() {
        assert_eq!(r("SELECT 'hi'"), "SELECT ?");
        assert_eq!(r("WHERE a='it''s'"), "WHERE a=?");
    }

    #[test]
    fn numeric_literals() {
        assert_eq!(r("WHERE id = 42"), "WHERE id = ?");
        assert_eq!(r("WHERE x > 1.5e3"), "WHERE x > ?");
        assert_eq!(r("WHERE x BETWEEN 1 AND 100"), "WHERE x BETWEEN ? AND ?");
    }

    #[test]
    fn does_not_break_identifiers() {
        assert_eq!(r("SELECT a1 FROM t"), "SELECT a1 FROM t");
        assert_eq!(r("SELECT col_2 FROM t"), "SELECT col_2 FROM t");
    }

    #[test]
    fn preserves_double_quoted_identifiers() {
        assert_eq!(r("SELECT \"My Col\" FROM t"), "SELECT \"My Col\" FROM t");
    }

    #[test]
    fn dollar_quoted() {
        assert_eq!(r("SELECT $$secret$$ FROM t"), "SELECT ? FROM t");
        assert_eq!(r("SELECT $tag$x$tag$ FROM t"), "SELECT ? FROM t");
    }

    #[test]
    fn line_and_block_comments_pass_through() {
        assert_eq!(r("-- pwd 'abc'\nSELECT 1"), "-- pwd 'abc'\nSELECT ?");
        assert_eq!(r("/* x=1 */ SELECT 2"), "/* x=1 */ SELECT ?");
    }
}
