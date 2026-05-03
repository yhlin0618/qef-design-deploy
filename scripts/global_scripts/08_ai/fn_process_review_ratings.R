#' Process Review Ratings
#'
#' Processes the review ratings obtained from OpenAI API and transforms them into
#' usable property ratings by product line. This is part of D03_03 (Process Reviews) step.
#'
#' @param comment_property_rating A DBI connection to the comment property
#'   rating database (read-only, contains sampled data).
#' @param comment_property_rating_results A DBI connection to the comment
#'   property rating results database.
#' @param processed_data A DBI connection to the processed data database.
#' @param vec_product_line_id_noall A vector of product line IDs to process
#'   (excluding "all").
#'
#' @return Invisible NULL. The function writes the processed data to database tables.
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' comment_property_rating <- dbConnectDuckdb(
#'   db_path_list$comment_property_rating, read_only = TRUE)
#' comment_property_rating_results <- dbConnectDuckdb(
#'   db_path_list$comment_property_rating_results, read_only = TRUE)
#' processed_data <- dbConnectDuckdb(
#'   db_path_list$processed_data, read_only = FALSE)
#'
#' # Process review ratings
#' process_review_ratings(
#'   comment_property_rating = comment_property_rating,
#'   comment_property_rating_results = comment_property_rating_results,
#'   processed_data = processed_data,
#'   vec_product_line_id_noall = vec_product_line_id_noall
#' )
#' }
#'
#' @export
process_review_ratings <- function(comment_property_rating,
                                   comment_property_rating_results,
                                   processed_data,
                                   vec_product_line_id_noall) {

  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("tidyr", quietly = TRUE)) library(tidyr)
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)

  # decode_rating() 由 autoinit() 載入，避免於函式內再次 source()（遵守 R149）

  # Process each product line
  for (product_line_id_i in vec_product_line_id_noall) {
    # Log processing
    message("Processing ratings for product line: ", product_line_id_i)
    
    # Define table names
    source_table_name <- paste0("df_comment_property_rating_", product_line_id_i,
                                "___append_long")
    target_table_name <- paste0("df_comment_property_ratingonly_",
                                product_line_id_i)

    # Check if source table exists
    if (!DBI::dbExistsTable(comment_property_rating_results,
                           source_table_name)) {
      warning("Source table not found: ", source_table_name,
              ". Skipping product line ", product_line_id_i)
      next
    }

    # Process the ratings
    tryCatch({
      # Check if the base sampled data exists
      sampled_table_name <- paste0("df_comment_property_rating_",
                                   product_line_id_i, "___sampled")

      if (!DBI::dbExistsTable(comment_property_rating, sampled_table_name)) {
        warning("Base sampled table not found: ", sampled_table_name,
                ". Skipping product line ", product_line_id_i)
        next
      }

      # Define key columns first
      key_cols <- c("product_line_id", "product_id", "reviewer_id", 
                    "review_date")
      
      # ---- 基準資料（sampled 已經是 wide format） ------------------------------
      base_wide <- dplyr::tbl(comment_property_rating, sampled_table_name) %>%
        dplyr::collect() %>%
        dplyr::mutate(review_date = as.Date(review_date)) %>%
        dplyr::distinct(dplyr::across(dplyr::all_of(key_cols)), 
                       .keep_all = TRUE)

      # Read the AI rating results
      ratings_data <- dplyr::tbl(comment_property_rating_results,
                                 source_table_name) %>%
        dplyr::collect() %>%
        dplyr::mutate(review_date = as.Date(review_date))

      message("Processing ", nrow(ratings_data),
              " rating records for product line ", product_line_id_i)
      message("Base sampled data has ", nrow(base_wide), " reviews")

      # Decode the AI ratings and create a lookup table
      ratings_lookup <- ratings_data %>%
        # Decode the rating values from ai_rating_result
        dplyr::mutate(decoded_rating = decode_rating(ai_rating_result)) %>%
        # Keep only necessary columns for joining
        dplyr::select(product_line_id, product_id, reviewer_id, review_date,
                      property_name, decoded_rating, ai_rating_result,
                      ai_rating_timestamp) %>%
        # Create a status indicator for debugging
        dplyr::mutate(rating_status = dplyr::case_when(
          is.na(decoded_rating) ~ "failed_decode",
          grepl("\\[NaN", ai_rating_result, ignore.case = TRUE) ~ "nan_result",
          grepl("error|exception", ai_rating_result,
                ignore.case = TRUE) ~ "error_result",
          TRUE ~ "success"
        ))

      # Get property names from base_wide (sampled data) - only use properties relevant to this product line
      base_column_names <- names(base_wide)
      property_names <- base_column_names[!base_column_names %in% c(
        "platform_id", "product_line_id", "product_id", "reviewer_id", 
        "review_date", "review_title", "review_body", "rating", "verified", 
        "helpful", "url", "style", "included_competiter"
      )]

      # Debug: Check successful ratings before pivot
      successful_ratings <- ratings_lookup %>%
        dplyr::filter(rating_status == "success")
      
      message("Debug: ", nrow(successful_ratings), " successful ratings before pivot")
      message("Debug: Unique keys in successful ratings: ", 
              nrow(successful_ratings %>% 
                   dplyr::distinct(dplyr::across(dplyr::all_of(key_cols)))))
      
      # Pivot the ratings to wide format - only for properties relevant to this product line
      ratings_wide <- successful_ratings %>%
        # Filter to only include properties that exist in base_wide
        dplyr::filter(property_name %in% property_names) %>%
        dplyr::select(-ai_rating_result, -ai_rating_timestamp,
                      -rating_status) %>%
        # Pivot to wide format
        tidyr::pivot_wider(
          names_from = property_name,
          values_from = decoded_rating,
          values_fn = first,  # If multiple values, take first
          values_fill = NA_integer_
        )
      
      message("Debug: ", nrow(ratings_wide), " records after pivot_wider")
      
      # Check for NA values in pivoted data
      na_counts_by_property <- ratings_wide %>%
        dplyr::select(dplyr::any_of(property_names)) %>%
        dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.x)))) %>%
        tidyr::pivot_longer(dplyr::everything(), names_to = "property", 
                           values_to = "na_count")
      
      message("Debug: NA counts by property after pivot:")
      for (i in seq_len(nrow(na_counts_by_property))) {
        message("  ", na_counts_by_property$property[i], ": ", 
                na_counts_by_property$na_count[i], " NAs")
      }

      # Ensure uniqueness and type consistency
      ratings_wide <- ratings_wide %>%
        dplyr::distinct(dplyr::across(dplyr::all_of(key_cols)), 
                        .keep_all = TRUE) %>%
        # Ensure all property columns are integer type
        dplyr::mutate(
          dplyr::across(
            dplyr::any_of(property_names), 
            ~ as.integer(.x)
          )
        )

      # Ensure existing property columns are integer type (no need to add missing columns)
      for (prop_name in property_names) {
        base_wide[[prop_name]] <- as.integer(base_wide[[prop_name]])
      }

      # Diagnostics: keys in ratings_wide not present in base_wide
      unmatched_before <- dplyr::anti_join(ratings_wide, base_wide, by = key_cols)
      if (nrow(unmatched_before) > 0) {
        message("⚠️  ", nrow(unmatched_before),
                " rating records have keys not present in base; skipped during patching")
      } else {
        message("All patch keys present in base data")
      }

      # Update base data with ratings using rows_patch (only overwrites NA values)
      message("Attempting to patch ", nrow(ratings_wide), " rating records...")

      patch_result <- tryCatch({
        base_wide %>%
          dplyr::rows_patch(
            ratings_wide,
            by = c("product_line_id", "product_id", "reviewer_id", 
                   "review_date"),
            unmatched = "ignore"
          )
      }, error = function(e) {
        stop("rows_patch failed: ", e$message)
      })

      processed_data_df <- patch_result

      # Diagnostics: separate NA issues from system issues
      
      # 1. Records that should have been patched but still contain NA
      successfully_patched_records <- processed_data_df %>%
        dplyr::semi_join(ratings_wide, by = key_cols)
      
      na_due_to_failed_ratings <- successfully_patched_records %>%
        dplyr::filter(dplyr::if_any(dplyr::any_of(property_names), is.na))
      
      # Count successfully patched records (those that got at least some ratings)
      successfully_patched_count <- processed_data_df %>%
        dplyr::semi_join(ratings_wide, by = key_cols) %>%
        nrow()
      
      if (successfully_patched_count > 0) {
        message("✅ ", successfully_patched_count, 
                " records successfully patched with AI ratings")
      }
      
      # 2. Records that couldn't be patched due to system/structural issues
      records_with_successful_ratings <- ratings_wide %>%
        dplyr::filter(dplyr::if_all(dplyr::any_of(property_names),
                                    ~ !is.na(.x)))

      system_patch_failures <- records_with_successful_ratings %>%
        dplyr::anti_join(processed_data_df, by = key_cols)

      if (nrow(system_patch_failures) > 0) {
        warning("⚠️  ", nrow(system_patch_failures),
                " records with valid ratings failed to patch (system issue)")
      } else {
        message("✅ All records with valid ratings successfully patched")
      }

      if (nrow(processed_data_df) != nrow(base_wide)) {
        warning("Row count mismatch after patching: expected ", 
                nrow(base_wide), " but got ", nrow(processed_data_df))
      } else {
        message("Successfully patched data: ", nrow(processed_data_df), 
                " rows")
      }

      # Create a summary table for debugging
      rating_summary <- ratings_lookup %>%
        dplyr::group_by(.data$rating_status) %>%
        dplyr::summarise(count = dplyr::n(), .groups = "drop") %>%
        dplyr::mutate(
          percentage = round(count / sum(count) * 100, 1),
          status_emoji = dplyr::case_when(
            .data$rating_status == "success" ~ "✅",
            .data$rating_status == "failed_decode" ~ "❌",
            .data$rating_status == "nan_result" ~ "⚠️",
            .data$rating_status == "error_result" ~ "🚨",
            TRUE ~ "❓"
          )
        )

      message("AI Rating decode summary for ", product_line_id_i, ":")
      for (i in seq_len(nrow(rating_summary))) {
        message("  ", rating_summary$status_emoji[i], " ", 
                rating_summary$rating_status[i], ": ", 
                rating_summary$count[i], " records (", 
                rating_summary$percentage[i], "%)")
      }
      
      # Summary of patch effectiveness
      successful_ratings_count <- rating_summary %>%
        dplyr::filter(.data$rating_status == "success") %>%
        dplyr::pull(count)

      if (length(successful_ratings_count) > 0) {
        message("📊 Patch effectiveness: ", nrow(ratings_wide),
                " successful ratings → ",
                nrow(successfully_patched_records), " patched records")
      }

      # Write to database
      DBI::dbWriteTable(
        processed_data,
        target_table_name,
        processed_data_df,
        append = FALSE,
        overwrite = TRUE
      )

      message("Successfully created table: ", target_table_name, " with ",
              nrow(processed_data_df), " rows")

    }, error = function(e) {
      warning("Error processing ratings for product line ", product_line_id_i,
              ": ", e$message)
    })
  }

  # Return invisibly
  invisible(NULL)
}