#' Process Property Ratings for Multiple Product Lines
#'
#' Processes review property ratings for multiple product lines as part of D03_07 (Rate Reviews) step.
#' Creates tables, processes sampled data, and performs AI rating of reviews.
#' Timestamps are stored as TIMESTAMP WITH TIME ZONE to preserve timezone information.
#' 
#' The function includes robust connection error handling:
#' - Tests OpenAI API connectivity before processing
#' - Stops immediately on connection errors (network, timeout, SSL, etc.)
#' - Monitors database connection health during writes
#' - Provides clear error messages for debugging
#'
#' @param comment_property_rating A DBI connection to the comment property rating database
#' @param comment_property_rating_results A DBI connection to the comment property rating results database
#' @param vec_product_line_id_noall A vector of product line IDs to process (excluding "all")
#' @param chunk_size Integer. Number of records to process in each batch (default: 20)
#' @param workers Integer. Number of parallel workers to use (default: 4)
#' @param gpt_key Character. OpenAI API key.
#' @param model Character. The model to use (default: "gpt-5.2").
#' @param title Character. Column name for title/comment field (default: "title")
#' @param body Character. Column name for body/content field (default: "body")
#' @param platform Character. Platform identifier ("amz" or "eby") (default: "amz")
#' @param input_database Character. Source database option: "results" (copy to results first) or "source" (read directly from comment_property_rating)
#' @param retry_errors Logical. If TRUE, re-process previously failed records (Connection_error, HTTP_*, API_error).
#'   Uses DELETE + INSERT pattern to replace error records with corrected results. (default: FALSE)
#'   Error patterns detected: Connection_error, HTTP_*, API_error, Unknown_format
#'   See API03 stored_error_recovery principle for details.
#'
#' @return Invisible NULL. The function creates database tables as a side effect.
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' dbConnect_from_list("comment_property_rating", read_only = FALSE)
#' dbConnect_from_list("comment_property_rating_results", read_only = FALSE)
#' 
#' # Process property ratings
#' process_property_ratings(
#'   comment_property_rating = comment_property_rating,
#'   comment_property_rating_results = comment_property_rating_results,
#'   vec_product_line_id_noall = vec_product_line_id_noall,
#'   chunk_size = 20,
#'   workers = 8,
#'   gpt_key = Sys.getenv("OPENAI_API_KEY"),
#'   model = "gpt-5.2",
#'   title = "title",
#'   body = "body"
#' )
#' 
#' # For eBay data where comment is in fb_comment field
#' process_property_ratings(
#'   comment_property_rating = comment_property_rating,
#'   comment_property_rating_results = comment_property_rating_results,
#'   vec_product_line_id_noall = vec_product_line_id_noall,
#'   gpt_key = gpt_key,
#'   title = "fb_comment",
#'   body = "fb_comment",  # Use same column for both if only one text field
#'   platform = "eby"
#' )
#' }
#'
#' @export
process_property_ratings <- function(comment_property_rating,
                                    comment_property_rating_results,
                                    vec_product_line_id_noall,
                                    chunk_size = 20,
                                    workers = 4,
                                    gpt_key,
                                    model = "gpt-5.2",  # Default; callers should pass model from ai_prompts.yaml
                                    title = "title",
                                    body = "body",
                                    platform = "amz",
                                    input_database = "results",
                                    retry_errors = FALSE) {
  
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("furrr", quietly = TRUE)) library(furrr)
  if (!requireNamespace("future", quietly = TRUE)) library(future)
  if (!requireNamespace("purrr", quietly = TRUE)) library(purrr)
  if (!requireNamespace("cli", quietly = TRUE)) library(cli)
  if (!requireNamespace("lubridate", quietly = TRUE)) library(lubridate)
  if (!requireNamespace("httr", quietly = TRUE)) library(httr)
  if (!exists("tbl2")) {
    source(file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"))
  }
  
  
  # Test API connection before starting
  cli::cli_alert_info("Testing OpenAI API connection...")
  tryCatch({
    # Simple test call to verify API connectivity
    test_response <- httr::GET(
      "https://api.openai.com/v1/models",
      httr::add_headers(Authorization = paste0("Bearer ", gpt_key))
    )
    if (httr::status_code(test_response) != 200) {
      stop("OpenAI API connection failed. Status code: ", httr::status_code(test_response))
    }
    cli::cli_alert_success("OpenAI API connection verified")
  }, error = function(e) {
    cli::cli_alert_danger("Failed to connect to OpenAI API: {e$message}")
    stop("OpenAI API connection test failed: ", e$message, call. = FALSE)
  })

  # Set up parallel processing
  cores <- parallel::detectCores()
  actual_workers <- min(workers, cores - 1)
  future::plan(future::multisession, workers = actual_workers)
  cli::cli_alert_info("Using {actual_workers} workers for parallel processing")
  
  # Process each product line
  for (product_line_id in vec_product_line_id_noall) {
    
    # Log processing status
    cli::cli_h2("Processing product line: {product_line_id}")
    
    # Define table names
    sampled_table_name <- paste0("df_comment_property_rating_", product_line_id, "___sampled_long")
    append_table_name <- paste0("df_comment_property_rating_", product_line_id, "___append_long")
    
    # Determine input database
    if (input_database == "source") {
      cli::cli_alert_info("Reading data directly from source database")

      # Check if table exists before reading (fix for "Can't query fields" error)
      if (!DBI::dbExistsTable(comment_property_rating, sampled_table_name)) {
        cli::cli_alert_warning("Table {sampled_table_name} not found in source database, skipping product line: {product_line_id}")
        next  # Skip to next product line
      }

      # Read directly from comment_property_rating database
      sampled_tbl <- tbl2(comment_property_rating, sampled_table_name)
    } else {
      # Original behavior: copy to results database
      cli::cli_alert_info("Copying sampled data to results database")

      # Check if table exists before copying
      if (!DBI::dbExistsTable(comment_property_rating, sampled_table_name)) {
        cli::cli_alert_warning("Table {sampled_table_name} not found in source database, skipping product line: {product_line_id}")
        next  # Skip to next product line
      }

      dbCopyTable(
        comment_property_rating,
        comment_property_rating_results,
        sampled_table_name,
        overwrite = TRUE
      )

      # Get sampled table
      sampled_tbl <- tbl2(comment_property_rating_results, sampled_table_name)
    }
    
    # Create append table with proper structure if it doesn't exist
    # Uses collect() + dbWriteTable() to avoid writing to read-only source DB
    if (!DBI::dbExistsTable(comment_property_rating_results, append_table_name)) {
      cli::cli_alert_info("Creating append table: {append_table_name}")
      empty_schema <- sampled_tbl %>%
        dplyr::filter(FALSE) %>%
        dplyr::collect() %>%
        dplyr::select(
          product_line_id,
          product_id,
          reviewer_id,
          review_date,
          review_title,
          review_body,
          property_name
        ) %>%
        dplyr::mutate(
          ai_rating_result = character(0),
          ai_rating_gpt_model = character(0),
          ai_rating_timestamp = as.POSIXct(character(0))
        )
      DBI::dbWriteTable(comment_property_rating_results, append_table_name, empty_schema)
    }
    
    # Get already processed data
    done_tbl <- tbl2(comment_property_rating_results, append_table_name)
    
    # Define key columns that uniquely identify a record
    # Use standardized field names for identifying unique records
    key_cols <- c("product_id", "reviewer_id", "property_name")
    
    # Verify all key columns exist
    sampled_cols <- colnames(sampled_tbl)
    missing_cols <- setdiff(key_cols, sampled_cols)
    if (length(missing_cols) > 0) {
      cli::cli_alert_warning("Missing key columns: {paste(missing_cols, collapse = ', ')}")
      key_cols <- intersect(key_cols, sampled_cols)
    }
    
    cols <- key_cols
    
    cli::cli_alert_info("Join columns: {paste(cols, collapse = ', ')}")
    
    # Get counts before processing
    initial_count <- tbl2(comment_property_rating_results, append_table_name) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::pull(n)
    
    # Get total sampled count
    total_sampled <- sampled_tbl %>% dplyr::count() %>% dplyr::pull(n)
    cli::cli_alert_info("Total records in sampled table: {total_sampled}")
    cli::cli_alert_info("Already processed records in append table: {initial_count}")
    
    # Find records that need processing
    # First collect both tables to avoid cross-database join issues
    sampled_data <- sampled_tbl %>% dplyr::collect()
    done_data <- done_tbl %>% dplyr::collect()

    # Define error patterns for retry mode (following API03 stored_error_recovery pattern)
    error_patterns_regex <- "Connection_error|connectionerror|HTTP_|API_error|Unknown_format"

    if (retry_errors) {
      # RETRY MODE: Find error records in append table and re-process them
      cli::cli_h3("Retry Mode: Processing previously failed records")

      # Find error records in done_data
      error_records <- done_data %>%
        dplyr::filter(grepl(error_patterns_regex, ai_rating_result, ignore.case = TRUE))

      if (nrow(error_records) == 0) {
        cli::cli_alert_success("No error records to retry for product line {product_line_id}")
        next
      }

      cli::cli_alert_info("Found {nrow(error_records)} error records to retry")

      # Use error_records directly as todo - the append table already contains
      # all original data (review_title, review_body, property_name, etc.)
      # This handles cases where sampled_data has been regenerated/reduced
      todo <- error_records

      # Store error keys for DELETE operation later
      error_keys_for_delete <- error_records %>%
        dplyr::select(dplyr::all_of(cols))

    } else {
      # NORMAL MODE: Find new records that haven't been processed
      # Perform anti-join in memory
      todo <- sampled_data %>%
        dplyr::anti_join(done_data, by = cols)

      # No error keys to delete in normal mode
      error_keys_for_delete <- NULL
    }

    # Skip if no records need processing
    if (nrow(todo) == 0) {
      if (retry_errors) {
        cli::cli_alert_success("No error records to retry for product line {product_line_id}")
      } else {
        cli::cli_alert_success("No new records to process for product line {product_line_id} (Total existing: {initial_count})")
      }
      next
    }
    
    # Log processing information
    if (retry_errors) {
      cli::cli_alert_info("Processing {nrow(todo)} retry records for product line {product_line_id} (Already processed: {initial_count})")
    } else {
      cli::cli_alert_info("Processing {nrow(todo)} new records for product line {product_line_id} (Already processed: {initial_count})")
    }

    # --- Batch mode: group by unique review, call rate_comments_batch() per review ---
    # Source batch function if not loaded
    if (!exists("rate_comments_batch", mode = "function")) {
      batch_fn_path <- file.path("scripts", "global_scripts", "08_ai", "fn_rate_comments_batch.R")
      if (file.exists(batch_fn_path)) source(batch_fn_path)
    }

    # Get unique reviews from todo (each review appears N times in long format, once per property)
    review_id_cols <- c("product_id", "reviewer_id", "review_date")
    review_id_cols <- intersect(review_id_cols, names(todo))

    unique_reviews <- todo %>%
      dplyr::distinct(dplyr::across(dplyr::all_of(review_id_cols)),
                      .keep_all = TRUE) %>%
      dplyr::select(dplyr::all_of(review_id_cols),
                    dplyr::any_of(c(title, body, "product_line_id")))

    # Get property definitions for this product line
    all_properties <- todo %>%
      dplyr::distinct(property_name, .keep_all = TRUE) %>%
      dplyr::select(dplyr::any_of(c("property_id", "property_name",
                                      "property_name_english", "type", "scale")))
    # Ensure required columns exist
    if (!"property_id" %in% names(all_properties)) {
      all_properties$property_id <- seq_len(nrow(all_properties))
    }
    if (!"scale" %in% names(all_properties)) {
      all_properties$scale <- "5尺度"
    }

    n_reviews <- nrow(unique_reviews)
    n_props <- nrow(all_properties)
    # Each review = 1-2 API calls (likert + binary split)
    n_likert <- sum(!vapply(all_properties$scale, identical, logical(1), "2尺度"))
    n_binary <- sum(vapply(all_properties$scale, identical, logical(1), "2尺度"))
    calls_per_review <- (n_likert > 0) + (n_binary > 0)
    cli::cli_alert_info("Batch mode: {n_reviews} reviews × {n_props} properties ({n_likert} likert + {n_binary} binary) = {n_reviews * calls_per_review} API calls")

    # Get product line name for prompt
    product_line_name <- if ("property_name_english" %in% names(all_properties)) {
      product_line_id  # Use ID as fallback
    } else {
      product_line_id
    }

    # Process each review
    for (ri in seq_len(n_reviews)) {
      review_row <- unique_reviews[ri, ]
      review_title_val <- if (title %in% names(review_row)) review_row[[title]] else NA_character_
      review_body_val <- if (body %in% names(review_row)) review_row[[body]] else NA_character_
      review_product_id <- review_row$product_id

      cli::cli_alert_info("Review {ri}/{n_reviews}: {review_product_id}")

      tryCatch({
        DBI::dbBegin(comment_property_rating_results)

        batch_result <- rate_comments_batch(
          title = review_title_val,
          body = review_body_val,
          product_line_name = product_line_name,
          properties = all_properties,
          gpt_key = gpt_key,
          model = model,
          reasoning_effort = "medium"
        )

        # Join batch results back to the long-format todo rows for this review
        review_todo <- todo %>%
          dplyr::filter(product_id == review_product_id)
        if (length(review_id_cols) > 1) {
          for (col in review_id_cols[-1]) {
            review_todo <- review_todo %>%
              dplyr::filter(.data[[col]] == review_row[[col]])
          }
        }

        # Map batch results to property_name via property_id
        prop_lookup <- all_properties %>%
          dplyr::select(property_id, property_name)

        batch_mapped <- batch_result %>%
          dplyr::left_join(prop_lookup, by = "property_id")

        # Build append rows
        timestamp_now <- Sys.time()
        append_rows <- review_todo %>%
          dplyr::left_join(
            batch_mapped %>% dplyr::select(property_name, score, reason),
            by = "property_name"
          ) %>%
          dplyr::mutate(
            ai_rating_result = dplyr::if_else(
              is.na(score),
              "[NaN,NaN]",
              paste0("[", score, ",", reason, "]")
            ),
            ai_rating_gpt_model = model,
            ai_rating_timestamp = timestamp_now
          ) %>%
          dplyr::select(
            product_line_id, product_id, reviewer_id, review_date,
            dplyr::any_of(c(title, body)),
            property_name, ai_rating_result, ai_rating_gpt_model, ai_rating_timestamp
          )

        # Rename title/body columns to standard names
        if (title != "review_title" && title %in% names(append_rows)) {
          names(append_rows)[names(append_rows) == title] <- "review_title"
        }
        if (body != "review_body" && body %in% names(append_rows)) {
          names(append_rows)[names(append_rows) == body] <- "review_body"
        }

        # Ensure column set matches append table
        expected_cols <- c("product_line_id", "product_id", "reviewer_id", "review_date",
                          "review_title", "review_body", "property_name",
                          "ai_rating_result", "ai_rating_gpt_model", "ai_rating_timestamp")
        append_rows <- append_rows %>% dplyr::select(dplyr::any_of(expected_cols))

        DBI::dbAppendTable(comment_property_rating_results, append_table_name, append_rows)
        DBI::dbCommit(comment_property_rating_results)

        # Sample output
        n_scored <- sum(!is.na(batch_result$score))
        cli::cli_alert_success("  {nrow(append_rows)} properties rated ({n_scored} scored, {nrow(batch_result) - n_scored} NA/0)")

      }, error = function(e) {
        tryCatch(DBI::dbRollback(comment_property_rating_results), error = function(re) NULL)
        error_msg <- as.character(e$message)
        if (grepl("connection|network|timeout|curl|ssl|tls|socket", error_msg, ignore.case = TRUE)) {
          cli::cli_alert_danger("Connection error: {error_msg}")
          stop("Connection error: ", error_msg, call. = FALSE)
        }
        cli::cli_alert_warning("Error for review {ri}: {error_msg}, skipping")
      })
    }
    
    # Get final count and calculate newly processed
    final_count <- tbl2(comment_property_rating_results, append_table_name) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::pull(n)
    
    records_added <- final_count - initial_count
    
    cli::cli_alert_success(
      "Completed processing for product line {product_line_id}"
    )
    cli::cli_alert_info("  Newly processed: {records_added} records")
    cli::cli_alert_info("  Total in database: {final_count} records")
  }
  
  # Return invisibly
  invisible(NULL)
}
