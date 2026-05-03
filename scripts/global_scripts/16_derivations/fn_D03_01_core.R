# =============================================================================
# fn_D03_01_core.R — Geographic Sales Aggregation (World Market Map)
# GROUP: D03 (Positioning Analysis)
# CONSUMES: transformed_data.df_amz_sales___standardized
# PRODUCES: app_data.df_geo_sales_by_country, app_data.df_customer_country_map, app_data.df_geo_sales_by_state
#           (both country + state tables include cross-platform platform_id='all' rows — #417, MP055)
# DEPENDS_ON_ETL: amz_ETL_sales_2TR
# Following: MP064, MP029, MP140, MP055, DM_R025
# EXPORTS:
#   run_D03_01(platform_id)                — per-platform aggregation (existing)
#   aggregate_D03_01_all_platforms(app_data) — platform_id='all' rollup (#417)
# =============================================================================

run_D03_01 <- function(platform_id, config = NULL) {

  # Guard
  if (missing(platform_id) || is.null(platform_id) || !nzchar(platform_id)) {
    stop("platform_id is required")
  }

  # ---- PART 1: INITIALIZE ----
  connection_created_transformed <- FALSE
  connection_created_app_data    <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed    <- FALSE
  state$rows_processed <- 0
  start_time <- Sys.time()

  message(sprintf("[%s_D03_01] START: Geographic Sales Aggregation", platform_id))

  # Source DB utility
  db_util_path <- file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R")
  if (file.exists(db_util_path)) source(db_util_path)

  # Open transformed_data (read)
  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection") || !DBI::dbIsValid(transformed_data)) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  # Open app_data (write)
  if (!exists("app_data") || !inherits(app_data, "DBIConnection") || !DBI::dbIsValid(app_data)) {
    app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
    connection_created_app_data <- TRUE
  }

  # Cross-platform-safe write: preserve rows for other platforms (issue #374,
  # same pattern as fn_D01_02_core.R::write_platform_table fix from #371 blocker 13).
  # Without this, sequential per-platform calls (cbz then eby) cause silent data loss
  # because dbWriteTable(overwrite=TRUE) drops earlier platform rows.
  write_platform_table_d03 <- function(con, table_name, data, platform_value) {
    if (!DBI::dbExistsTable(con, table_name)) {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
      return(invisible())
    }

    other_rows <- tryCatch({
      existing <- DBI::dbReadTable(con, table_name)
      if ("platform_id" %in% names(existing)) {
        as.data.frame(existing[existing$platform_id != platform_value, , drop = FALSE])
      } else {
        existing[FALSE, , drop = FALSE]
      }
    }, error = function(e) {
      message(sprintf("[%s] Could not read existing %s: %s", platform_value, table_name, e$message))
      NULL
    })

    DBI::dbRemoveTable(con, table_name)

    if (!is.null(other_rows) && nrow(other_rows) > 0) {
      merged <- tryCatch(
        dplyr::bind_rows(as.data.frame(other_rows), as.data.frame(data)),
        error = function(e) {
          message(sprintf(
            "[%s] WARNING: schema cannot be unified with existing rows for platforms (%s); dropping them. Re-run those platforms after this completes.",
            platform_value,
            paste(unique(other_rows$platform_id), collapse = ", ")
          ))
          as.data.frame(data)
        }
      )
      DBI::dbWriteTable(con, table_name, merged, overwrite = TRUE)
      message(sprintf("[%s] Merged: %d new + %d preserved from other platforms",
                      platform_value, nrow(data), nrow(other_rows)))
    } else {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
    }
  }

  # ---- PART 2: MAIN ----
  tryCatch({
    # Validate source table
    source_table <- sprintf("df_%s_sales___standardized", platform_id)
    if (!DBI::dbExistsTable(transformed_data, source_table)) {
      stop(sprintf("Source table '%s' not found in transformed_data", source_table))
    }

    message(sprintf("[%s_D03_01] Reading from: %s", platform_id, source_table))

    # Aggregate by country + product_line
    # Filter: Shipped orders only, non-NA ship_country
    query <- sprintf("
      SELECT
        platform_id,
        COALESCE(product_line_id, 'unclassified') AS product_line_id_filter,
        ship_country,
        SUM(lineproduct_price)                          AS total_revenue,
        COUNT(DISTINCT amazon_order_id)                 AS order_count,
        COUNT(DISTINCT customer_id)                     AS customer_count,
        SUM(lineproduct_price) / NULLIF(COUNT(DISTINCT amazon_order_id), 0) AS avg_order_value,
        AVG(quantity)                                   AS avg_quantity
      FROM %s
      WHERE order_status = 'Shipped'
        AND ship_country IS NOT NULL
        AND ship_country != ''
      GROUP BY platform_id, COALESCE(product_line_id, 'unclassified'), ship_country
    ", source_table)

    df_by_line <- DBI::dbGetQuery(transformed_data, query)
    message(sprintf("[%s_D03_01] Per product_line rows: %d", platform_id, nrow(df_by_line)))

    # Also aggregate 'all' product lines combined
    query_all <- sprintf("
      SELECT
        platform_id,
        'all' AS product_line_id_filter,
        ship_country,
        SUM(lineproduct_price)                          AS total_revenue,
        COUNT(DISTINCT amazon_order_id)                 AS order_count,
        COUNT(DISTINCT customer_id)                     AS customer_count,
        SUM(lineproduct_price) / NULLIF(COUNT(DISTINCT amazon_order_id), 0) AS avg_order_value,
        AVG(quantity)                                   AS avg_quantity
      FROM %s
      WHERE order_status = 'Shipped'
        AND ship_country IS NOT NULL
        AND ship_country != ''
      GROUP BY platform_id, ship_country
    ", source_table)

    df_all <- DBI::dbGetQuery(transformed_data, query_all)
    message(sprintf("[%s_D03_01] 'all' product_line rows: %d", platform_id, nrow(df_all)))

    # Combine
    df_geo <- rbind(df_by_line, df_all)

    # Round numeric columns
    df_geo$total_revenue   <- round(df_geo$total_revenue, 2)
    df_geo$avg_order_value <- round(df_geo$avg_order_value, 2)
    df_geo$avg_quantity    <- round(df_geo$avg_quantity, 2)

    message(sprintf("[%s_D03_01] Total output rows: %d, countries: %d",
                    platform_id, nrow(df_geo), length(unique(df_geo$ship_country))))

    # Write to app_data — cross-platform safe (#374)
    write_platform_table_d03(app_data, "df_geo_sales_by_country", df_geo, platform_id)
    state$rows_processed <- nrow(df_geo)

    message(sprintf("[%s_D03_01] Written df_geo_sales_by_country to app_data (%d rows)",
                    platform_id, nrow(df_geo)))

    # ---- Customer → Country mapping (Issue #237) ----
    # Each customer's primary shipping country (mode = most frequent)
    query_ccm <- sprintf("
      SELECT customer_id, platform_id, ship_country
      FROM (
        SELECT customer_id, platform_id, ship_country,
               ROW_NUMBER() OVER (PARTITION BY customer_id, platform_id
                                  ORDER BY COUNT(*) DESC) AS rn
        FROM %s
        WHERE order_status = 'Shipped'
          AND ship_country IS NOT NULL AND ship_country != ''
        GROUP BY customer_id, platform_id, ship_country
      ) sub
      WHERE rn = 1
    ", source_table)

    df_ccm <- DBI::dbGetQuery(transformed_data, query_ccm)
    write_platform_table_d03(app_data, "df_customer_country_map", df_ccm, platform_id)
    message(sprintf("[%s_D03_01] Written df_customer_country_map (%d customers)",
                    platform_id, nrow(df_ccm)))

    # ---- State-level aggregation for US drill-down (Issue #240) ----
    query_state <- sprintf("
      SELECT
        platform_id,
        COALESCE(product_line_id, 'unclassified') AS product_line_id_filter,
        ship_state,
        SUM(lineproduct_price)                          AS total_revenue,
        COUNT(DISTINCT amazon_order_id)                 AS order_count,
        COUNT(DISTINCT customer_id)                     AS customer_count,
        SUM(lineproduct_price) / NULLIF(COUNT(DISTINCT amazon_order_id), 0) AS avg_order_value,
        AVG(quantity)                                   AS avg_quantity
      FROM %s
      WHERE order_status = 'Shipped'
        AND ship_country = 'US'
        AND ship_state IS NOT NULL AND ship_state != ''
      GROUP BY platform_id, COALESCE(product_line_id, 'unclassified'), ship_state
    ", source_table)

    df_state_by_line <- DBI::dbGetQuery(transformed_data, query_state)

    query_state_all <- sprintf("
      SELECT
        platform_id,
        'all' AS product_line_id_filter,
        ship_state,
        SUM(lineproduct_price)                          AS total_revenue,
        COUNT(DISTINCT amazon_order_id)                 AS order_count,
        COUNT(DISTINCT customer_id)                     AS customer_count,
        SUM(lineproduct_price) / NULLIF(COUNT(DISTINCT amazon_order_id), 0) AS avg_order_value,
        AVG(quantity)                                   AS avg_quantity
      FROM %s
      WHERE order_status = 'Shipped'
        AND ship_country = 'US'
        AND ship_state IS NOT NULL AND ship_state != ''
      GROUP BY platform_id, ship_state
    ", source_table)

    df_state_all <- DBI::dbGetQuery(transformed_data, query_state_all)
    df_state <- rbind(df_state_by_line, df_state_all)
    df_state$total_revenue   <- round(df_state$total_revenue, 2)
    df_state$avg_order_value <- round(df_state$avg_order_value, 2)
    df_state$avg_quantity    <- round(df_state$avg_quantity, 2)

    write_platform_table_d03(app_data, "df_geo_sales_by_state", df_state, platform_id)
    message(sprintf("[%s_D03_01] Written df_geo_sales_by_state (%d rows, %d states)",
                    platform_id, nrow(df_state), length(unique(df_state$ship_state))))

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s_D03_01] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ---- PART 3: TEST ----
  if (!state$error_occurred) {
    tryCatch({
      # Verify output exists
      if (!DBI::dbExistsTable(app_data, "df_geo_sales_by_country")) {
        stop("Output table df_geo_sales_by_country not found")
      }
      row_count <- DBI::dbGetQuery(app_data, "SELECT COUNT(*) AS n FROM df_geo_sales_by_country")$n
      if (row_count == 0) stop("Output table is empty")

      # Verify required columns
      cols <- DBI::dbListFields(app_data, "df_geo_sales_by_country")
      required <- c("platform_id", "product_line_id_filter", "ship_country",
                     "total_revenue", "order_count", "customer_count",
                     "avg_order_value", "avg_quantity")
      missing_cols <- setdiff(required, cols)
      if (length(missing_cols) > 0) {
        stop(sprintf("Missing columns: %s", paste(missing_cols, collapse = ", ")))
      }

      # Verify df_customer_country_map
      if (!DBI::dbExistsTable(app_data, "df_customer_country_map")) {
        stop("Output table df_customer_country_map not found")
      }
      ccm_count <- DBI::dbGetQuery(app_data, "SELECT COUNT(*) AS n FROM df_customer_country_map")$n

      # Verify df_geo_sales_by_state
      if (!DBI::dbExistsTable(app_data, "df_geo_sales_by_state")) {
        stop("Output table df_geo_sales_by_state not found")
      }
      state_count <- DBI::dbGetQuery(app_data, "SELECT COUNT(*) AS n FROM df_geo_sales_by_state")$n

      state$test_passed <- TRUE
      message(sprintf("[%s_D03_01] TEST: PASSED (geo=%d rows, ccm=%d customers, states=%d rows)",
                      platform_id, row_count, ccm_count, state_count))
    }, error = function(e) {
      state$test_passed <- FALSE
      message(sprintf("[%s_D03_01] TEST: FAILED - %s", platform_id, e$message))
    })
  }

  # ---- PART 4: SUMMARIZE ----
  execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  summary_report <- list(
    success           = !state$error_occurred && state$test_passed,
    platform_id       = platform_id,
    rows_processed    = state$rows_processed,
    execution_time_secs = execution_time,
    outputs           = c("df_geo_sales_by_country", "df_customer_country_map", "df_geo_sales_by_state")
  )
  message(sprintf("[%s_D03_01] SUMMARY: %s | Rows: %d | Time: %.2fs",
                  platform_id,
                  ifelse(summary_report$success, "SUCCESS", "FAILED"),
                  state$rows_processed, execution_time))

  # ---- PART 5: DEINITIALIZE ----
  if (connection_created_transformed && exists("transformed_data") && DBI::dbIsValid(transformed_data)) {
    DBI::dbDisconnect(transformed_data, shutdown = FALSE)
  }
  if (connection_created_app_data && exists("app_data") && DBI::dbIsValid(app_data)) {
    DBI::dbDisconnect(app_data, shutdown = FALSE)
  }

  summary_report
}

# =============================================================================
# aggregate_D03_01_all_platforms — compose platform_id='all' aggregate rows (#417)
# =============================================================================
# CALLED BY: all_D03_01.R orchestrator AFTER the per-platform run_D03_01() loop.
# WRITES  : platform_id='all' rows into df_geo_sales_by_country + df_geo_sales_by_state.
#
# Rationale (#417):
#   run_D03_01() is per-platform and does not produce cross-platform 'all'
#   rollup rows. UI components (e.g. worldMap.R) default to platform_id='all'
#   filter, which returned 0 rows -> blank world map tab. This helper fills
#   the gap per MP055 (Special Treatment of 'ALL' Category).
#
# Semantics of 'all' platform row:
#   total_revenue, order_count : SUM across platforms (exact — orders do not
#                                overlap across platforms).
#   customer_count             : SUM across platforms (UPPER BOUND for multi-
#                                platform companies — same customer_id may
#                                exist on amz + cbz. Precise de-duplication
#                                requires raw transformed_data access and is
#                                tracked as a follow-up issue).
#   avg_order_value            : SUM(total_revenue) / SUM(order_count).
#   avg_quantity               : order-count-weighted average.
#
# Idempotent: safe to re-run — existing platform_id='all' rows are dropped
# before the aggregation is re-inserted.
# =============================================================================
aggregate_D03_01_all_platforms <- function(app_data) {
  if (!inherits(app_data, "DBIConnection") || !DBI::dbIsValid(app_data)) {
    stop("aggregate_D03_01_all_platforms: app_data must be a valid DBI connection")
  }

  # Helper: aggregate one geo table (country or state) into platform='all' rows.
  aggregate_one <- function(table_name, dim_col) {
    if (!DBI::dbExistsTable(app_data, table_name)) {
      message(sprintf("[all_D03_01_agg] table %s not found; skipping", table_name))
      return(invisible(FALSE))
    }
    # Idempotent: drop existing platform_id='all' rows first.
    DBI::dbExecute(app_data, sprintf(
      "DELETE FROM %s WHERE platform_id = 'all'", table_name))
    # Re-aggregate from per-platform rows and INSERT SELECT.
    DBI::dbExecute(app_data, sprintf("
      INSERT INTO %1$s (platform_id, product_line_id_filter, %2$s,
                        total_revenue, order_count, customer_count,
                        avg_order_value, avg_quantity)
      SELECT
        'all' AS platform_id,
        product_line_id_filter,
        %2$s,
        ROUND(SUM(total_revenue), 2)                                           AS total_revenue,
        SUM(order_count)                                                       AS order_count,
        SUM(customer_count)                                                    AS customer_count,
        ROUND(SUM(total_revenue) / NULLIF(SUM(order_count), 0), 2)             AS avg_order_value,
        ROUND(SUM(avg_quantity * order_count) / NULLIF(SUM(order_count), 0), 2) AS avg_quantity
      FROM %1$s
      WHERE platform_id != 'all'
      GROUP BY product_line_id_filter, %2$s
    ", table_name, dim_col))
    new_rows <- DBI::dbGetQuery(app_data, sprintf(
      "SELECT COUNT(*) AS n FROM %s WHERE platform_id = 'all'", table_name))$n
    message(sprintf("[all_D03_01_agg] %s: wrote %d 'all'-platform rows",
                    table_name, new_rows))
    invisible(TRUE)
  }

  tryCatch({
    aggregate_one("df_geo_sales_by_country", "ship_country")
    aggregate_one("df_geo_sales_by_state",   "ship_state")
    TRUE
  }, error = function(e) {
    message(sprintf("[all_D03_01_agg] ERROR: %s", e$message))
    FALSE
  })
}
