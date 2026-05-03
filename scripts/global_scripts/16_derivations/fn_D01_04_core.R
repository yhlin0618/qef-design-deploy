#' @title D01_04 Core Function - Application Views Generation (Cross-Company)
#' @description Core logic for normalizing cleansed DNA/profile outputs to app_data.
#'              Follows DEV_R038: Core Function + Platform Wrapper Pattern.
#'              This function is shared across all companies via git subrepo.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby", "shopify")
#' @param config Optional list. Platform config. If NULL, uses default table patterns.
#' @return List with success status and summary
#' @principle DEV_R038, DEV_R037, DM_R044, MP064, MP144

run_D01_04 <- function(platform_id, config = NULL) {
  # Validate platform_id is provided
  if (missing(platform_id) || is.null(platform_id) || !nzchar(platform_id)) {
    stop("platform_id is required")
  }

  if (is.null(config)) {
    if (!exists("get_platform_config", mode = "function", inherits = TRUE)) {
      stop("get_platform_config() is required when config is NULL")
    }
    config <- get_platform_config(platform_id, warn = FALSE)
  }

  # ===========================================================================
  # PART 1: INITIALIZE
  # ===========================================================================

  connection_created_cleansed <- FALSE
  connection_created_app <- FALSE

  error_occurred <- FALSE
  test_passed <- FALSE
  rows_processed <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D01_04", platform_id)

  # Default table names (can be overridden by config)
  input_dna_table <- if (!is.null(config$input_dna_table)) {
    config$input_dna_table
  } else {
    "df_dna_by_customer___cleansed"
  }

  input_profile_table <- if (!is.null(config$input_profile_table)) {
    config$input_profile_table
  } else {
    "df_profile_by_customer___cleansed"
  }

  # Source metadata function
  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)
  if (!exists("add_drv_metadata", mode = "function")) {
    stop("Missing required function add_drv_metadata")
  }

  # Source schema migration helper (#376, MP154 Side Effect Defense)
  ensure_schema_fn_path <- file.path(GLOBAL_DIR, "28_migration_scripts", "fn_ensure_dna_schema.R")
  if (file.exists(ensure_schema_fn_path)) source(ensure_schema_fn_path)
  if (!exists("ensure_dna_schema", mode = "function")) {
    stop("Missing required function ensure_dna_schema (28_migration_scripts/fn_ensure_dna_schema.R)")
  }

  # Establish database connections
  if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
    cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = TRUE)
    connection_created_cleansed <- TRUE
  }

  if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
    app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
    connection_created_app <- TRUE
  }

  # Helper: Get insertable columns (excluding generated columns)
  get_insertable_columns <- function(con, table_name) {
    info <- tryCatch(
      DBI::dbGetQuery(
        con,
        sprintf(
          "SELECT column_name, is_generated, generation_expression
           FROM information_schema.columns
           WHERE table_name = '%s' AND table_schema = 'main'",
          table_name
        )
      ),
      error = function(e) NULL
    )
    if (!is.null(info) && nrow(info) > 0) {
      if ("is_generated" %in% names(info)) {
        info <- info[is.na(info$is_generated) | info$is_generated == "NO", , drop = FALSE]
      } else if ("generation_expression" %in% names(info)) {
        info <- info[is.na(info$generation_expression) | info$generation_expression == "", , drop = FALSE]
      }
      return(unique(info$column_name))
    }

    cols <- unique(DBI::dbListFields(con, table_name))
    if (table_name == "df_profile_by_customer") {
      cols <- setdiff(cols, "display_name")
    }
    cols
  }

  # Helper: Select only columns that exist in target table
  # MP154 Side Effect Defense (#376): emit warning when columns are silently
  # dropped so schema drift cannot mask broken derivations (e.g., BTYD #211
  # p_alive being dropped because app_data table predates the column).
  select_table_columns <- function(con, table_name, df) {
    target_cols <- get_insertable_columns(con, table_name)
    dropped <- setdiff(names(df), target_cols)
    if (length(dropped) > 0) {
      warning(sprintf(
        "[D01_04] select_table_columns dropped %d column(s) from %s: %s. Run ensure_<table>_schema(con) to migrate the target schema.",
        length(dropped), table_name, paste(dropped, collapse = ", ")
      ), call. = FALSE)
    }
    df <- df[, intersect(names(df), target_cols), drop = FALSE]
    df
  }

  # Helper: Create or replace analytics views
  create_or_replace_views <- function(con) {
    DBI::dbExecute(
      con,
      "CREATE OR REPLACE VIEW v_customer_dna_analytics AS
       SELECT * FROM df_dna_by_customer"
    )

    DBI::dbExecute(
      con,
      "CREATE OR REPLACE VIEW v_customer_segments AS
       SELECT customer_id,
              platform_id,
              product_line_id_filter,
              nes_status,
              dna_segment,
              cai,
              CASE
                WHEN dna_m_score >= 0.8 THEN 'Premium'
                WHEN dna_m_score >= 0.6 THEN 'High'
                WHEN dna_m_score >= 0.4 THEN 'Medium'
                ELSE 'Low'
              END AS value_tier
       FROM df_dna_by_customer"
    )

    DBI::dbExecute(
      con,
      "CREATE OR REPLACE VIEW v_segment_statistics AS
       SELECT platform_id,
              product_line_id_filter,
              nes_status,
              COUNT(*) AS customer_count,
              AVG(m_value) AS avg_m_value,
              AVG(cai) AS avg_cai,
              SUM(total_spent) AS total_value
       FROM df_dna_by_customer
       GROUP BY platform_id, product_line_id_filter, nes_status"
    )
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    # Check required cleansed tables
    required_cleansed_tables <- c(input_dna_table, input_profile_table)
    for (table_name in required_cleansed_tables) {
      if (!DBI::dbExistsTable(cleansed_data, table_name)) {
        stop(sprintf("Missing cleansed_data table: %s", table_name))
      }
    }

    # Check required app_data tables
    required_app_tables <- c("df_profile_by_customer", "df_dna_by_customer", "df_segments_by_customer")
    for (table_name in required_app_tables) {
      if (!DBI::dbExistsTable(app_data, table_name)) {
        stop(sprintf("Missing app_data table: %s. Run D00_app_data_init first.", table_name))
      }
    }

    # Ensure df_dna_by_customer schema matches canonical source-of-truth (#376)
    # before projection. Idempotent — adds any missing columns (e.g., BTYD #211
    # p_alive) via ALTER TABLE so select_table_columns() will not silently
    # drop them.
    message(sprintf("[%s] MAIN: Ensuring df_dna_by_customer schema is current...", platform_id))
    ensure_dna_schema(app_data, verbose = TRUE)

    message(sprintf("[%s] MAIN: Loading cleansed DNA data...", platform_id))
    customer_dna <- dplyr::tbl(cleansed_data, input_dna_table) |>
      dplyr::filter(platform_id == !!platform_id) |>
      dplyr::collect()

    if (nrow(customer_dna) == 0) {
      stop("No customer DNA rows found for platform")
    }

    message(sprintf("[%s] MAIN: Loading cleansed profile data...", platform_id))
    customer_profile <- dplyr::tbl(cleansed_data, input_profile_table) |>
      dplyr::filter(platform_id == !!platform_id) |>
      dplyr::collect()

    if (nrow(customer_profile) == 0) {
      stop("No customer profile rows found for platform")
    }

    if (!"customer_id" %in% names(customer_profile)) {
      stop("Missing customer_id in profile data")
    }

    if (!"customer_id" %in% names(customer_dna)) {
      stop("Missing customer_id in DNA data")
    }

    customer_dna$customer_id <- suppressWarnings(as.integer(customer_dna$customer_id))
    customer_dna$platform_id <- platform_id

    required_dna_cols <- c(
      "product_line_id_filter",
      "nes_status",
      "dna_segment",
      "dna_m_score",
      "dna_f_score",
      "dna_r_score",
      "cai",
      "cai_value"
    )
    missing_dna_cols <- setdiff(required_dna_cols, names(customer_dna))
    if (length(missing_dna_cols) > 0) {
      stop(sprintf(
        "Missing required DNA columns from D01_02 output: %s",
        paste(missing_dna_cols, collapse = ", ")
      ))
    }

    customer_profile$customer_id <- suppressWarnings(as.integer(customer_profile$customer_id))
    customer_profile$platform_id <- platform_id

    customer_profile <- customer_profile |>
      dplyr::distinct(customer_id, platform_id, .keep_all = TRUE)
    customer_dna <- customer_dna |>
      dplyr::distinct(customer_id, platform_id, product_line_id_filter, .keep_all = TRUE)

    customer_segments <- customer_dna |>
      dplyr::mutate(
        value_tier = dplyr::case_when(
          !is.na(dna_m_score) & dna_m_score >= 0.8 ~ "Premium",
          !is.na(dna_m_score) & dna_m_score >= 0.6 ~ "High",
          !is.na(dna_m_score) & dna_m_score >= 0.4 ~ "Medium",
          TRUE ~ "Low"
        )
      ) |>
      dplyr::select(
        customer_id,
        platform_id,
        product_line_id_filter,
        nes_status,
        dna_segment,
        cai,
        value_tier
      ) |>
      dplyr::distinct(customer_id, platform_id, product_line_id_filter, .keep_all = TRUE)

    customer_profile <- add_drv_metadata(customer_profile, drv_script_name, drv_batch_id)
    customer_dna <- add_drv_metadata(customer_dna, drv_script_name, drv_batch_id)
    customer_segments <- add_drv_metadata(customer_segments, drv_script_name, drv_batch_id)

    customer_profile <- select_table_columns(app_data, "df_profile_by_customer", customer_profile)
    customer_dna <- select_table_columns(app_data, "df_dna_by_customer", customer_dna)
    customer_segments <- select_table_columns(app_data, "df_segments_by_customer", customer_segments)

    message(sprintf("[%s] MAIN: Writing outputs to app_data...", platform_id))
    DBI::dbWithTransaction(app_data, {
      DBI::dbExecute(app_data, "DELETE FROM df_profile_by_customer WHERE platform_id = ?", params = list(platform_id))
      DBI::dbExecute(app_data, "DELETE FROM df_dna_by_customer WHERE platform_id = ?", params = list(platform_id))
      DBI::dbExecute(app_data, "DELETE FROM df_segments_by_customer WHERE platform_id = ?", params = list(platform_id))

      DBI::dbAppendTable(app_data, "df_profile_by_customer", customer_profile)
      DBI::dbAppendTable(app_data, "df_dna_by_customer", customer_dna)
      DBI::dbAppendTable(app_data, "df_segments_by_customer", customer_segments)
    })

    create_or_replace_views(app_data)
    rows_processed <- nrow(customer_dna)

  }, error = function(e) {
    error_occurred <<- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!error_occurred) {
    tryCatch({
      required_tables <- c("df_profile_by_customer", "df_dna_by_customer", "df_segments_by_customer")
      for (table_name in required_tables) {
        if (!DBI::dbExistsTable(app_data, table_name)) {
          stop(sprintf("Missing app_data table: %s", table_name))
        }
      }

      dna_count <- dplyr::tbl(app_data, "df_dna_by_customer") |>
        dplyr::filter(platform_id == !!platform_id) |>
        dplyr::count() |>
        dplyr::pull()

      if (dna_count == 0) {
        stop("No DNA rows written for platform")
      }

      test_passed <- TRUE
      message(sprintf("[%s] TEST: Output tables verified", platform_id))

    }, error = function(e) {
      test_passed <<- FALSE
      message(sprintf("[%s] TEST: ERROR - %s", platform_id, e$message))
    })
  }

  # ===========================================================================
  # PART 4: SUMMARIZE
  # ===========================================================================

  execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  summary_report <- list(
    success = !error_occurred && test_passed,
    platform_id = platform_id,
    rows_processed = rows_processed,
    execution_time_secs = execution_time,
    outputs = c("df_profile_by_customer", "df_dna_by_customer", "df_segments_by_customer",
                "v_customer_dna_analytics", "v_customer_segments", "v_segment_statistics")
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows processed: %d", platform_id, rows_processed))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_cleansed && DBI::dbIsValid(cleansed_data)) {
    DBI::dbDisconnect(cleansed_data, shutdown = FALSE)
  }
  if (connection_created_app && DBI::dbIsValid(app_data)) {
    DBI::dbDisconnect(app_data, shutdown = FALSE)
  }

  summary_report
}
