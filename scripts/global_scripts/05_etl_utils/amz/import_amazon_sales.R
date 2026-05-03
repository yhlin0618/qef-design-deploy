#' Import Amazon Sales Data from Excel Files
#'
#' This function imports Amazon sales data from Excel files into the DuckDB database.
#' It processes all Excel files in a specified folder and its subfolders, 
#' performs basic data cleaning and validation, and appends the data to 
#' the df_amazon_sales table.
#'
#' @param folder_path Character string. Path to the folder containing Amazon sales Excel files.
#' @param connection DBI connection object. An active connection to a DuckDB database.
#' @param clean_columns Logical. Whether to apply standardized column name cleaning. Default is TRUE.
#' @param overwrite Logical. Whether to overwrite the existing table or append to it. Default is FALSE (append).
#' @param verbose Logical. Whether to print detailed processing information. Default is TRUE.
#' @param fail_on_any_error Logical. If TRUE, mark import as failed when any source file fails. Default is TRUE.
#' @param return_report Logical. If TRUE, return a structured import report list; otherwise return connection. Default is FALSE.
#' @param write_audit_csv Optional character path to write per-file audit CSV.
#' @param col_types Character scalar passed to readxl::read_excel col_types. Default is "text" for cross-file schema stability.
#'
#' @return By default, DBI connection object for chaining operations. If return_report = TRUE,
#' returns a list with `connection`, `summary`, `file_report`, `source_file_reconciliation`,
#' and `key_reconciliation`.
#'
#' @details 
#' The function finds all Excel files (.xlsx or .xls) in the provided folder and its subfolders,
#' then attempts to read and process each file. It performs the following operations:
#' 1. Standardizes column names to snake_case
#' 2. Validates that required columns (sku, purchase_date) exist
#' 3. Adds source metadata (etl_source_file, etl_source_path, etl_import_timestamp)
#' 4. Performs per-file + key reconciliation checks after write
#' 5. Writes or appends to df_amazon_sales
#'
#' If fail_on_any_error = TRUE, any read/process/write failure is treated as import failure.
#'
#' @examples
#' \dontrun{
#' # Connect to a DuckDB database
#' con <- dbConnect(duckdb::duckdb(), dbdir = "path/to/database.duckdb")
#'
#' # Import Amazon sales data
#' import_df_amazon_sales("path/to/amazon_sales_files", con)
#'
#' # Import and overwrite existing data
#' import_df_amazon_sales("path/to/amazon_sales_files", con, overwrite = TRUE)
#' }
#'
#' @export
import_df_amazon_sales <- function(folder_path, connection, clean_columns = TRUE,
                                   overwrite = FALSE, verbose = TRUE,
                                   fail_on_any_error = TRUE,
                                   return_report = FALSE,
                                   write_audit_csv = NULL,
                                   col_types = "text") {
  import_run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")

  # Input validation
  if (!dir.exists(folder_path)) {
    stop("Folder path does not exist: ", folder_path)
  }

  if (!inherits(connection, "DBIConnection")) {
    stop("Connection must be a DBI database connection")
  }

  if (!is.null(write_audit_csv) && (!is.character(write_audit_csv) || length(write_audit_csv) != 1)) {
    stop("write_audit_csv must be a single character path or NULL")
  }

  # Find all Excel files in the folder and subfolder
  all_files <- sort(list.files(
    folder_path,
    pattern = "\\.(xlsx|xls)$",
    recursive = TRUE,
    full.names = TRUE
  ))

  if (length(all_files) == 0) {
    stop("No Excel files found in ", folder_path)
  }

  if (verbose) message("Found ", length(all_files), " Excel files to process")

  file_report_list <- vector("list", length(all_files))
  imported_data <- list()
  imported_index <- 0

  # #475: Coverage audit accumulator for column-shifted rows.
  # Per SO_R038 v1.1 rule 5 + MP163: structurally-corrupt source rows are
  # dropped from main df and surfaced via df_amazon_sales_coverage_audit.
  # Validator helper is sourced lazily (backward compat); when absent,
  # validator is a no-op and a LOUD warning surfaces the silent-bypass.
  coverage_audit_accumulator <- list()

  for (file_path in all_files) {
    report_row <- data.frame(
      import_run_id = import_run_id,
      file_path = file_path,
      file_name = basename(file_path),
      status = "PENDING",
      rows_source = NA_integer_,
      rows_prepared = NA_integer_,
      required_cols_ok = FALSE,
      has_asin_col = FALSE,
      distinct_asin_source = NA_integer_,
      error_message = "",
      stringsAsFactors = FALSE
    )

    if (verbose) message("Importing: ", basename(file_path))

    tryCatch({
      data <- readxl::read_excel(
        file_path,
        col_types = col_types,
        .name_repair = "minimal"
      )
      report_row$rows_source <- nrow(data)

      if (nrow(data) == 0) {
        report_row$status <- "EMPTY"
        file_report_list[[which(all_files == file_path)]] <- report_row
        if (verbose) message("  Empty file")
        next
      }

      # Ensure stable column names across files.
      if (clean_columns) {
        names(data) <- tolower(names(data))
        names(data) <- gsub("-", "_", names(data))
        names(data) <- gsub(" ", "_", names(data))
        names(data) <- gsub("[^a-z0-9_]", "", names(data))
      }
      names(data) <- make.unique(names(data), sep = "_dup_")

      # Check required columns
      required_cols <- c("sku", "purchase_date")
      missing_cols <- setdiff(required_cols, names(data))
      if (length(missing_cols) > 0) {
        report_row$status <- "MISSING_REQUIRED_COLS"
        report_row$error_message <- paste(missing_cols, collapse = ", ")
        file_report_list[[which(all_files == file_path)]] <- report_row
        if (verbose) {
          message("  Missing required columns: ", report_row$error_message)
        }
        next
      }
      report_row$required_cols_ok <- TRUE

      # #445: Normalize Excel serial date strings in purchase_date to ISO.
      # readxl with col_types="text" returns serial numbers like
      # "45323.435208333336" as strings; downstream CAST AS TIMESTAMP fails.
      # fix_excel_serial_dates() detects serials via sanity range and converts
      # to ISO; non-serial values and NAs are preserved.
      #
      # The exists() guard keeps this function usable for callers that haven't
      # sourced the helper (backward compat). But in that case we emit a LOUD
      # warning so the silent-bypass regression vector (verify-445 B1) is
      # surfaced — #445 silent bypass → Excel serial would resurrect.
      if (exists("fix_excel_serial_dates", mode = "function")) {
        data <- fix_excel_serial_dates(data, col_name = "purchase_date")
      } else {
        warning(
          "fix_excel_serial_dates() helper not sourced; purchase_date may ",
          "still contain Excel serial numbers. Source ",
          "global_scripts/05_etl_utils/common/fn_fix_excel_serial_dates.R ",
          "before calling import_df_amazon_sales(). (#445)",
          call. = FALSE
        )
      }

      # Flatten list columns (DuckDB cannot write list vectors directly).
      list_cols <- vapply(data, is.list, logical(1))
      if (any(list_cols)) {
        data[list_cols] <- lapply(data[list_cols], function(col_data) {
          vapply(col_data, function(x) {
            if (is.null(x) || length(x) == 0) return(NA_character_)
            paste(as.character(x), collapse = "; ")
          }, character(1))
        })
      }

      # Add trace metadata for per-file reconciliation.
      data$etl_source_file <- basename(file_path)
      data$etl_source_path <- file_path
      data$etl_import_timestamp <- Sys.time()

      # #475: Validate row integrity (drop column-shifted rows, accumulate audit).
      # Per SO_R038 v1.1 rule 5 — multi-field row corruption escalates to
      # df_amazon_sales_coverage_audit, NOT per-field UNKNOWN_<ENTITY>.
      if (exists("validate_amz_sales_row_integrity", mode = "function")) {
        integrity_result <- validate_amz_sales_row_integrity(
          data,
          source_file = basename(file_path),
          verbose = verbose
        )
        data <- integrity_result$clean_data
        if (nrow(integrity_result$audit_rows) > 0) {
          coverage_audit_accumulator[[length(coverage_audit_accumulator) + 1]] <-
            integrity_result$audit_rows
        }
      } else {
        warning(
          "validate_amz_sales_row_integrity() helper not sourced; ",
          "column-shifted rows (e.g., asin='Shipped' from MX marketplace) ",
          "will pass through to df_amazon_sales raw layer. Source ",
          "global_scripts/05_etl_utils/amz/fn_validate_amz_sales_row_integrity.R ",
          "before calling import_df_amazon_sales(). (#475)",
          call. = FALSE
        )
      }

      report_row$rows_prepared <- nrow(data)
      report_row$has_asin_col <- "asin" %in% names(data)
      if (report_row$has_asin_col) {
        asin_vals <- trimws(toupper(as.character(data$asin)))
        asin_vals <- asin_vals[!is.na(asin_vals) & nzchar(asin_vals)]
        report_row$distinct_asin_source <- length(unique(asin_vals))
      }

      imported_index <- imported_index + 1
      imported_data[[imported_index]] <- data
      report_row$status <- "IMPORTED"
      if (verbose) message("  Prepared ", nrow(data), " rows")

    }, error = function(e) {
      report_row$status <- "READ_OR_PROCESS_FAIL"
      report_row$error_message <- gsub("\\s+", " ", e$message)
      if (verbose) message("  ERROR: ", report_row$error_message)
    })

    file_report_list[[which(all_files == file_path)]] <- report_row
  }

  file_report <- do.call(rbind, file_report_list)
  imported_files <- subset(file_report, status == "IMPORTED")
  failed_files <- subset(file_report, status %in% c("MISSING_REQUIRED_COLS", "READ_OR_PROCESS_FAIL"))

  summary <- list(
    import_run_id = import_run_id,
    files_total = nrow(file_report),
    files_imported = nrow(imported_files),
    files_empty = sum(file_report$status == "EMPTY"),
    files_failed = nrow(failed_files),
    rows_expected_from_imported_files = sum(imported_files$rows_prepared, na.rm = TRUE),
    rows_written = NA_integer_,
    rows_committed = NA_integer_,
    coverage_audit_rows = 0L,  # #475: column-shifted rows surfaced
    per_file_reconciliation_ok = FALSE,
    key_reconciliation_ok = NA,
    transaction_result = "NOT_STARTED",
    success = FALSE
  )

  source_file_reconciliation <- data.frame(
    file_name = character(0),
    rows_expected = integer(0),
    rows_written = integer(0),
    match = logical(0),
    stringsAsFactors = FALSE
  )
  key_reconciliation <- list(
    key_name = "ASIN",
    source_n = NA_integer_,
    local_n = NA_integer_,
    missing_in_local = character(0),
    extra_in_local = character(0),
    ok = NA
  )

  write_audit <- function() {
    if (is.null(write_audit_csv)) return(invisible(NULL))

    audit_df <- file_report
    if (nrow(source_file_reconciliation) > 0) {
      audit_df <- merge(
        audit_df,
        source_file_reconciliation,
        by = "file_name",
        all.x = TRUE,
        sort = FALSE
      )
    } else {
      audit_df$rows_expected <- NA_integer_
      audit_df$rows_written <- NA_integer_
      audit_df$match <- NA
    }

    key_missing_n <- if (is.null(key_reconciliation$missing_in_local)) NA_integer_ else length(key_reconciliation$missing_in_local)
    key_extra_n <- if (is.null(key_reconciliation$extra_in_local)) NA_integer_ else length(key_reconciliation$extra_in_local)

    audit_df$transaction_result <- summary$transaction_result
    audit_df$run_success <- summary$success
    audit_df$files_failed_total <- summary$files_failed
    audit_df$rows_expected_total <- summary$rows_expected_from_imported_files
    audit_df$rows_written_total <- summary$rows_written
    audit_df$rows_committed_total <- summary$rows_committed
    audit_df$key_name <- key_reconciliation$key_name
    audit_df$key_reconciliation_ok <- summary$key_reconciliation_ok
    audit_df$key_missing_in_local_n <- key_missing_n
    audit_df$key_extra_in_local_n <- key_extra_n
    audit_df$audit_generated_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    dir.create(dirname(write_audit_csv), recursive = TRUE, showWarnings = FALSE)
    utils::write.csv(audit_df, write_audit_csv, row.names = FALSE, na = "")
    invisible(NULL)
  }

  # Enforce 0IM completeness gate before write.
  if (fail_on_any_error && summary$files_failed > 0) {
    summary$transaction_result <- "ROLLBACK_NO_WRITE"
    summary$success <- FALSE
    write_audit()
    result <- list(
      connection = connection,
      summary = summary,
      file_report = file_report,
      source_file_reconciliation = source_file_reconciliation,
      key_reconciliation = key_reconciliation
    )
    if (return_report) return(result)
    attr(connection, "import_report") <- result
    return(connection)
  }

  if (length(imported_data) == 0) {
    stop("No importable files found (all files empty or failed)")
  }

  # Combine all successfully parsed files first (prevents append-time schema drift).
  combined_data <- data.table::rbindlist(imported_data, use.names = TRUE, fill = TRUE)
  combined_data <- as.data.frame(combined_data, stringsAsFactors = FALSE)

  transaction_started <- FALSE
  committed <- FALSE
  DBI::dbBegin(connection)
  transaction_started <- TRUE
  summary$transaction_result <- "STARTED"
  on.exit({
    if (transaction_started && !committed) {
      try(DBI::dbRollback(connection), silent = TRUE)
    }
  }, add = TRUE)

  if (overwrite) {
    DBI::dbWriteTable(connection, "df_amazon_sales", combined_data, overwrite = TRUE, row.names = FALSE)
  } else {
    if (DBI::dbExistsTable(connection, "df_amazon_sales")) {
      DBI::dbWriteTable(connection, "df_amazon_sales", combined_data, append = TRUE, row.names = FALSE)
    } else {
      DBI::dbWriteTable(connection, "df_amazon_sales", combined_data, overwrite = TRUE, row.names = FALSE)
    }
  }

  # Reconciliation checks after write.
  summary$rows_written <- DBI::dbGetQuery(
    connection,
    "SELECT COUNT(*) AS n FROM df_amazon_sales"
  )$n

  if (overwrite) {
    source_written <- DBI::dbGetQuery(
      connection,
      "SELECT etl_source_file AS file_name, COUNT(*) AS rows_written
       FROM df_amazon_sales
       GROUP BY etl_source_file"
    )
    source_expected <- imported_files[, c("file_name", "rows_prepared")]
    names(source_expected) <- c("file_name", "rows_expected")
    source_file_reconciliation <- merge(
      source_expected,
      source_written,
      by = "file_name",
      all = TRUE
    )
    source_file_reconciliation$rows_expected[is.na(source_file_reconciliation$rows_expected)] <- 0L
    source_file_reconciliation$rows_written[is.na(source_file_reconciliation$rows_written)] <- 0L
    source_file_reconciliation$match <- source_file_reconciliation$rows_expected == source_file_reconciliation$rows_written

    summary$per_file_reconciliation_ok <-
      all(source_file_reconciliation$match) &&
      summary$rows_written == summary$rows_expected_from_imported_files
  } else {
    # Append mode cannot guarantee full-table reconciliation by source_file.
    summary$per_file_reconciliation_ok <- TRUE
  }

  if (any(imported_files$has_asin_col)) {
    source_asin <- unique(unlist(lapply(imported_data, function(df) {
      if (!"asin" %in% names(df)) return(character(0))
      vals <- trimws(toupper(as.character(df$asin)))
      vals <- vals[!is.na(vals) & nzchar(vals)]
      unique(vals)
    })))
    local_asin <- DBI::dbGetQuery(
      connection,
      "SELECT DISTINCT upper(trim(CAST(asin AS VARCHAR))) AS asin
       FROM df_amazon_sales
       WHERE asin IS NOT NULL
         AND length(trim(CAST(asin AS VARCHAR))) > 0"
    )$asin

    missing_in_local <- setdiff(source_asin, local_asin)
    extra_in_local <- if (overwrite) setdiff(local_asin, source_asin) else character(0)
    key_ok <- length(missing_in_local) == 0 && length(extra_in_local) == 0

    key_reconciliation <- list(
      key_name = "ASIN",
      source_n = length(source_asin),
      local_n = length(local_asin),
      missing_in_local = missing_in_local,
      extra_in_local = extra_in_local,
      ok = key_ok
    )
    summary$key_reconciliation_ok <- key_ok
  } else {
    summary$key_reconciliation_ok <- NA
  }

  summary$success <-
    (summary$files_failed == 0 || !fail_on_any_error) &&
    isTRUE(summary$per_file_reconciliation_ok) &&
    !identical(summary$key_reconciliation_ok, FALSE)

  if (isTRUE(summary$success)) {
    DBI::dbCommit(connection)
    committed <- TRUE
    transaction_started <- FALSE
    summary$transaction_result <- "COMMIT"
  } else {
    DBI::dbRollback(connection)
    transaction_started <- FALSE
    summary$transaction_result <- "ROLLBACK"
  }

  if (DBI::dbExistsTable(connection, "df_amazon_sales")) {
    summary$rows_committed <- DBI::dbGetQuery(
      connection,
      "SELECT COUNT(*) AS n FROM df_amazon_sales"
    )$n
  } else {
    summary$rows_committed <- 0L
  }

  # #475: Write coverage audit for column-shifted rows (SO_R038 v1.1 rule 5,
  # MP163 surface-to-attention). Append-mode; survives df_amazon_sales rebuild.
  # Written outside the main transaction so audit is preserved even if main
  # write rolls back — the audit is independent forensic evidence.
  if (length(coverage_audit_accumulator) > 0) {
    coverage_audit_df <- do.call(rbind, coverage_audit_accumulator)
    coverage_audit_df$import_run_id <- import_run_id

    tryCatch({
      if (DBI::dbExistsTable(connection, "df_amazon_sales_coverage_audit")) {
        DBI::dbWriteTable(
          connection,
          "df_amazon_sales_coverage_audit",
          coverage_audit_df,
          append = TRUE,
          row.names = FALSE
        )
      } else {
        DBI::dbWriteTable(
          connection,
          "df_amazon_sales_coverage_audit",
          coverage_audit_df,
          overwrite = TRUE,
          row.names = FALSE
        )
      }
      summary$coverage_audit_rows <- nrow(coverage_audit_df)
      if (verbose) {
        message(sprintf(
          "  - Coverage audit: %d row(s) written to df_amazon_sales_coverage_audit (column shift detected)",
          nrow(coverage_audit_df)
        ))
      }
    }, error = function(e) {
      warning("Failed to write df_amazon_sales_coverage_audit: ", e$message, call. = FALSE)
      summary$coverage_audit_rows <<- NA_integer_
    })
  } else {
    summary$coverage_audit_rows <- 0L
  }

  write_audit()

  if (verbose) {
    message("Import summary:")
    message("  - Import run ID: ", summary$import_run_id)
    message("  - Transaction result: ", summary$transaction_result)
    message("  - Files processed: ", summary$files_total)
    message("  - Files imported: ", summary$files_imported)
    message("  - Files empty: ", summary$files_empty)
    message("  - Files failed: ", summary$files_failed)
    message("  - Rows expected: ", summary$rows_expected_from_imported_files)
    message("  - Rows written: ", summary$rows_written)
    message("  - Rows committed: ", summary$rows_committed)
    message("  - Per-file reconciliation: ", summary$per_file_reconciliation_ok)
    message("  - Key reconciliation (ASIN): ", summary$key_reconciliation_ok)
    message("  - Coverage audit rows (column shift): ", summary$coverage_audit_rows)
    message("  - Success: ", summary$success)
  }

  result <- list(
    connection = connection,
    summary = summary,
    file_report = file_report,
    source_file_reconciliation = source_file_reconciliation,
    key_reconciliation = key_reconciliation
  )

  if (return_report) return(result)
  attr(connection, "import_report") <- result
  return(connection)
}

#' Process Amazon sales data
#'
#' Processes raw Amazon sales data from the database, performs transformations,
#' and writes the processed data to a destination table.
#'
#' @param raw_data DBI connection. Connection to the database containing raw data.
#' @param Data DBI connection. Connection to the database where processed data will be stored.
#' @param verbose Logical. Whether to display progress messages. Default is TRUE.
#'
#' @return Invisibly returns the Data connection for chaining.
#'
#' @details
#' This function performs the following operations:
#' 1. Filters records with valid email addresses
#' 2. Extracts customer_id from buyer_email
#' 3. Renames columns for consistency
#' 4. Joins with product_property_dictionary for additional product information
#' 5. Filters for US-only orders and required fields
#' 6. Writes the processed data to the destination database
#'
#' @examples
#' \dontrun{
#' # Connect to raw and processed data databases
#' raw_con <- dbConnect(duckdb::duckdb(), dbdir = "raw_data.duckdb")
#' proc_con <- dbConnect(duckdb::duckdb(), dbdir = "processed_data.duckdb")
#'
#' # Process Amazon sales data
#' process_amazon_sales(raw_con, proc_con)
#' }
#'
#' @export
process_amazon_sales <- function(raw_data, Data, verbose = TRUE) {
  # Load required packages
  library(dplyr)
  library(dbplyr)
  library(stringr)
  
  # Input validation
  if (!inherits(raw_data, "DBIConnection")) {
    stop("raw_data must be a DBI database connection")
  }
  
  if (!inherits(Data, "DBIConnection")) {
    stop("Data must be a DBI database connection")
  }
  
  # Check if required tables exist
  if (!DBI::dbExistsTable(raw_data, "df_amazon_sales")) {
    stop("Table 'df_amazon_sales' does not exist in the raw_data database")
  }
  
  if (!DBI::dbExistsTable(raw_data, "product_property_dictionary")) {
    stop("Table 'product_property_dictionary' does not exist in the raw_data database")
  }
  
  if (verbose) message("Processing Amazon sales data...")
  
  # Reference tables
  amazon_sales_dta <- tbl(raw_data, "df_amazon_sales")
  product_property_dictionary <- tbl(raw_data, "product_property_dictionary")
  
  # Process data
  tryCatch({
    result <- amazon_sales_dta %>% 
      filter(str_detect(buyer_email, "@")) %>%
      mutate(
        customer_id = sql("LOWER(SUBSTR(buyer_email, 1, POSITION('@' IN buyer_email) - 1))"),
        time = purchase_date
      ) %>%
      rename(
        lineproduct_price = product_price,
        zip_code = shipping_postal_code
      ) %>%
      left_join(product_property_dictionary, by = join_by(sku)) %>%
      filter(
        !is.na(customer_id) & !is.na(time) & 
          !is.na(sku) & !is.na(asin) & !is.na(product_line_id)
      ) %>% 
      filter(shipping_country_code == "US") %>% 
      select(customer_id, time, sku, lineproduct_price, everything()) %>% 
      collect()
    
    # Write to destination database
    DBI::dbWriteTable(Data, "df_amazon_sales", result, overwrite = TRUE, temporary = FALSE)
    
    if (verbose) {
      message("Amazon sales data processing complete:")
      message("  - Processed rows: ", nrow(result))
      message("  - Data written to 'df_amazon_sales' table in destination database")
    }
    
  }, error = function(e) {
    stop("Error processing Amazon sales data: ", e$message)
  })
  
  # Return connection for chaining
  invisible(Data)
}
