#' Cross-driver Safe Parameterized Query
#'
#' Drop-in replacement for `DBI::dbGetQuery(con, sql, params = list(...))` that
#' works with both DuckDB and PostgreSQL (and any driver supporting
#' `DBI::sqlInterpolate`). PostgreSQL does not accept `?` as a positional
#' placeholder — this wrapper converts `?` to named params (`?p1`, `?p2`, ...),
#' interpolates them via `DBI::sqlInterpolate`, then executes the final SQL.
#'
#' @param con A DBI connection (DuckDB, PostgreSQL, SQLite, etc.)
#' @param query SQL string with positional `?` placeholders
#' @param params List of values to bind, in order
#' @return data.frame from `DBI::dbGetQuery`
#'
#' @examples
#' \dontrun{
#' df <- dbGetQuerySafe(
#'   con,
#'   "SELECT * FROM customers WHERE platform_id = ? AND product_line_id = ?",
#'   params = list("amz", "blb")
#' )
#' }
#' @section Deprecation:
#' This function is **DEPRECATED** as of 2026-04-13. It was introduced as a
#' hot-fix for issue #365 (MAMBA Posit Connect PostgreSQL failure) but has
#' been superseded by the `tbl2()` + dplyr pattern enforced by `DM_R023 v1.2`.
#' Migrate callers to `tbl2(con, "table") %>% filter(...) %>% collect()`.
#' Target removal date: 2026-07-13 (three-month deprecation window).
#'
#' See `00_principles/docs/en/part1_principles/CH02_data_management/rules/DM_R023_universal_dbi_approach.qmd`
#' Section 6 "Exceptions & Migration" for the full rationale.
#'
#' @export
dbGetQuerySafe <- function(con, query, params = list()) {
  .Deprecated(
    new = "tbl2",
    package = "global_scripts",
    msg = paste0(
      "dbGetQuerySafe() is deprecated as of 2026-04-13. ",
      "Use `tbl2(con, \"table\") %>% dplyr::filter(...) %>% dplyr::collect()` instead. ",
      "See DM_R023 Section 6 for migration guidance. ",
      "Target removal: 2026-07-13."
    )
  )
  if (length(params) == 0) {
    return(DBI::dbGetQuery(con, query))
  }
  for (i in seq_along(params)) {
    query <- sub("\\?", paste0("?p", i), query)
  }
  args <- setNames(params, paste0("p", seq_along(params)))
  interpolated <- do.call(DBI::sqlInterpolate, c(list(con, query), args))
  DBI::dbGetQuery(con, interpolated)
}
