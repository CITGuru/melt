//! Golden tests for the Snowflake → DuckDB translator. Each pair is
//! `(input, expected)`. The expected SQL is what `parse + translate +
//! unparse` should produce — note that the sqlparser AST round-trip
//! normalizes some whitespace and casing, which the assertions
//! account for.

use melt_router::parse::{parse, unparse};
use melt_router::translate::translate_ast;

fn rewrite(sql: &str) -> String {
    let mut ast = parse(sql).expect("parse failed");
    translate_ast(&mut ast).expect("translate failed");
    unparse(&ast)
}

fn assert_translates(input: &str, expected_substring: &str) {
    let out = rewrite(input);
    assert!(
        out.contains(expected_substring),
        "translate({input:?})\n  got:      {out}\n  expected substring: {expected_substring}",
    );
}

#[test]
fn iff_to_case_when() {
    assert_translates(
        "SELECT IFF(x > 0, 'pos', 'neg') FROM t",
        "CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END",
    );
}

#[test]
fn nvl_to_coalesce() {
    assert_translates("SELECT NVL(a, 0) FROM t", "COALESCE(a, 0)");
}

#[test]
fn nvl2_to_case() {
    assert_translates(
        "SELECT NVL2(a, 'has', 'none') FROM t",
        "CASE WHEN a IS NOT NULL THEN 'has' ELSE 'none' END",
    );
}

#[test]
fn zeroifnull_to_coalesce_zero() {
    assert_translates("SELECT ZEROIFNULL(x) FROM t", "COALESCE(x, 0)");
}

#[test]
fn equal_null_to_distinct() {
    assert_translates("SELECT EQUAL_NULL(a, b) FROM t", "IS NOT DISTINCT FROM");
}

#[test]
fn decode_to_case() {
    assert_translates(
        "SELECT DECODE(c, 1, 'one', 2, 'two', 'other') FROM t",
        "CASE WHEN c IS NOT DISTINCT FROM 1 THEN 'one'",
    );
}

#[test]
fn dateadd_unit_quoted() {
    let out = rewrite("SELECT DATEADD(day, 7, ts) FROM t");
    assert!(out.contains("'day'"), "expected unit quoted in {out}");
}

#[test]
fn datediff_unit_quoted() {
    let out = rewrite("SELECT DATEDIFF(month, a, b) FROM t");
    assert!(out.contains("'month'"), "expected unit quoted in {out}");
}

#[test]
fn date_trunc_unit_quoted() {
    let out = rewrite("SELECT DATE_TRUNC(quarter, ts) FROM t");
    assert!(out.contains("'quarter'"), "expected unit quoted in {out}");
}

#[test]
fn dateadd_already_quoted_left_alone() {
    let out = rewrite("SELECT DATEADD('day', 7, ts) FROM t");
    assert!(out.contains("'day'") && !out.contains("''day''"));
}

#[test]
fn convert_timezone_to_at_time_zone() {
    let out = rewrite("SELECT CONVERT_TIMEZONE('UTC', ts) FROM t");
    assert!(
        out.contains("AT TIME ZONE"),
        "expected AT TIME ZONE in {out}"
    );
}

#[test]
fn parse_json_to_cast() {
    let out = rewrite("SELECT PARSE_JSON(s) FROM t");
    assert!(
        out.contains("::JSON") || out.contains("CAST"),
        "expected JSON cast in {out}"
    );
}

#[test]
fn object_construct_to_json_object() {
    assert_translates(
        "SELECT OBJECT_CONSTRUCT('k', 1, 'v', 2) FROM t",
        "json_object",
    );
}

#[test]
fn array_construct_to_list_literal() {
    let out = rewrite("SELECT ARRAY_CONSTRUCT(1, 2, 3) FROM t");
    // Either sqlparser displays as `[1, 2, 3]` or `ARRAY[1, 2, 3]`
    assert!(
        out.contains("[1, 2, 3]") || out.contains("ARRAY[1, 2, 3]"),
        "expected list literal in {out}"
    );
}

#[test]
fn get_path_to_json_extract() {
    assert_translates(
        "SELECT GET_PATH(j, 'a.b.c') FROM t",
        "json_extract(j, '$.a.b.c')",
    );
}

#[test]
fn boolor_agg_renamed() {
    let out = rewrite("SELECT BOOLOR_AGG(flag) FROM t");
    assert!(out.contains("bool_or"), "expected bool_or rename in {out}");
}

#[test]
fn nested_iff_inside_case_translates() {
    let out = rewrite("SELECT IFF(IFF(a, b, c), 1, 0) FROM t");
    assert!(out.matches("CASE").count() >= 2, "expected 2 CASE in {out}");
}
