#' @title D05_01 Category Finalize - Cross-platform Category Aggregate + Excess Growth
#' @description Reads `df_macro_monthly_summary` (already populated per-platform
#'   by `run_D05_01`), computes category-level aggregate (SUM of total_revenue
#'   across platforms within same product_line_id_filter per year_month),
#'   computes category MoM/YoY/QoQ growth rates on the category time series,
#'   and adds excess_growth_* columns (brand_pct - category_pct, in pp).
#'
#'   This SHALL run AFTER all per-platform `run_D05_01()` calls complete
#'   (otherwise category SUM is incomplete). Orchestrated by `all_D05_01.R`.
#'
#'   Decision 1 (issue #416, locked at plan-tier approval):
#'     category = SUM total_revenue across platforms within same
#'     product_line_id_filter (includes self).
#'
#'   Decision 2 (issue #416, locked):
#'     excess_growth = brand_pct - category_pct (差值, percentage points).
#'
#' @param brand_df Optional data.frame. If provided, function operates on it
#'   in-memory and returns the augmented df (TEST MODE — no DB write). If
#'   NULL, reads/writes `df_macro_monthly_summary` from `app_data` connection.
#' @param app_data Optional DBIConnection to app_data DuckDB. Required when
#'   brand_df is NULL. Function does not close the connection.
#' @return When brand_df is NULL: list(success, rows_written). When brand_df
#'   is provided: the augmented data.frame.
#' @principle MP064 (ETL/DRV separation), MP140, DM_R044, DEV_R001 (vectorized)

# Source helper if not already loaded
if (!exists("compute_growth_rates", mode = "function")) {
  helper_path <- file.path(
    if (exists("GLOBAL_DIR", mode = "character")) GLOBAL_DIR
    else file.path(getwd(), "scripts", "global_scripts"),
    "04_utils", "fn_compute_growth_rates.R"
  )
  if (file.exists(helper_path)) source(helper_path)
}

finalize_D05_01_category <- function(brand_df = NULL, app_data = NULL) {
  test_mode <- !is.null(brand_df)
  output_table <- "df_macro_monthly_summary"

  # ---- Load brand-level data ----
  if (test_mode) {
    df_full <- brand_df
  } else {
    if (is.null(app_data) || !inherits(app_data, "DBIConnection")) {
      stop("finalize_D05_01_category: app_data DBIConnection required when brand_df is NULL")
    }
    if (!DBI::dbExistsTable(app_data, output_table)) {
      stop(sprintf("Output table %s missing — run per-platform D05_01 first", output_table))
    }
    df_full <- DBI::dbReadTable(app_data, output_table)
  }

  required_cols <- c("year_month", "platform_id", "product_line_id_filter",
                     "total_revenue")
  missing_cols <- setdiff(required_cols, names(df_full))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  # Brand growth rate columns may not yet exist — ensure they do (NA fill)
  for (col in c("mom_revenue_pct", "yoy_revenue_pct", "qoq_revenue_pct")) {
    if (!col %in% names(df_full)) df_full[[col]] <- NA_real_
  }

  # Recompute brand-level pct columns (ensures consistency + populates QoQ
  # for any pre-existing rows written before QoQ was added).
  df_full <- recompute_brand_pcts(df_full)

  # ---- Compute category aggregate (group by year_month + product_line_id_filter) ----
  cat_keys <- unique(df_full[, c("year_month", "product_line_id_filter")])
  cat_keys <- cat_keys[order(cat_keys$product_line_id_filter, cat_keys$year_month), ]

  cat_summary <- aggregate(
    total_revenue ~ year_month + product_line_id_filter,
    data = df_full,
    FUN = function(x) sum(x, na.rm = TRUE)
  )
  names(cat_summary)[names(cat_summary) == "total_revenue"] <- "category_revenue"

  # Compute MoM/YoY/QoQ on category series, per product_line_id_filter
  cat_pcts <- do.call(rbind, lapply(
    split(cat_summary, cat_summary$product_line_id_filter),
    function(sub) {
      out <- compute_growth_rates(sub, value_col = "category_revenue",
                                  time_col = "year_month")
      out$category_mom_pct <- out$mom_pct
      out$category_yoy_pct <- out$yoy_pct
      out$category_qoq_pct <- out$qoq_pct
      out$mom_pct <- NULL
      out$yoy_pct <- NULL
      out$qoq_pct <- NULL
      out
    }
  ))
  rownames(cat_pcts) <- NULL

  # ---- Join category back to brand rows ----
  df_out <- merge(
    df_full,
    cat_pcts[, c("year_month", "product_line_id_filter",
                 "category_revenue", "category_mom_pct",
                 "category_yoy_pct", "category_qoq_pct")],
    by = c("year_month", "product_line_id_filter"),
    all.x = TRUE,
    sort = FALSE
  )

  # ---- Compute excess_growth (差值 = brand_pct - category_pct, in pp) ----
  df_out$excess_growth_mom <- df_out$mom_revenue_pct - df_out$category_mom_pct
  df_out$excess_growth_yoy <- df_out$yoy_revenue_pct - df_out$category_yoy_pct
  df_out$excess_growth_qoq <- df_out$qoq_revenue_pct - df_out$category_qoq_pct

  if (test_mode) {
    return(df_out)
  }

  # ---- Write back to app_data ----
  message(sprintf(
    "[D05_01_finalize_category] Writing %d rows back to %s with category + excess columns...",
    nrow(df_out), output_table
  ))
  DBI::dbWriteTable(app_data, output_table, as.data.frame(df_out),
                    overwrite = TRUE)
  list(success = TRUE, rows_written = nrow(df_out))
}

# Helper: recompute brand-level mom/yoy/qoq for every (platform_id, product_line_id_filter)
# segment to ensure QoQ is populated even if pre-existing rows were written
# before the QoQ extension landed.
recompute_brand_pcts <- function(df) {
  segments <- unique(df[, c("platform_id", "product_line_id_filter")])
  out_list <- vector("list", nrow(segments))
  for (i in seq_len(nrow(segments))) {
    plat <- segments$platform_id[i]
    pl   <- segments$product_line_id_filter[i]
    sel <- df$platform_id == plat & df$product_line_id_filter == pl
    sub <- df[sel, , drop = FALSE]
    sub <- compute_growth_rates(sub, value_col = "total_revenue",
                                time_col = "year_month")
    sub$mom_revenue_pct <- sub$mom_pct
    sub$yoy_revenue_pct <- sub$yoy_pct
    sub$qoq_revenue_pct <- sub$qoq_pct
    sub$mom_pct <- NULL
    sub$yoy_pct <- NULL
    sub$qoq_pct <- NULL
    out_list[[i]] <- sub
  }
  out <- do.call(rbind, out_list)
  rownames(out) <- NULL
  out
}
