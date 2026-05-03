#' Cleanse Amazon Review Data
#'
#' This function cleanses raw Amazon review data by fixing date formats,
#' renaming fields, and filtering for valid ASINs. It supports the D03_04 step
#' of the Positioning Analysis derivation flow.
#'
#' @param raw_db_connection A DBI connection to the raw_data database
#' @param cleansed_db_connection A DBI connection to the cleansed_data database
#' @param source_table The name of the source table in raw_data (default: "df_amz_review")
#' @param target_table The name of the target table in cleansed_data (default: "df_amz_review")
#' @param overwrite Logical. Whether to overwrite the target table (default: TRUE)
#'
#' @return NULL (displays summary statistics but doesn't return any data)
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' dbConnect_from_list("raw_data")
#' dbConnect_from_list("cleansed_data")
#' 
#' # Cleanse Amazon reviews
#' cleanse_amazon_reviews(
#'   raw_db_connection = raw_data,
#'   cleansed_db_connection = cleansed_data
#' )
#' }
#'
#' @export
cleanse_amazon_reviews <- function(raw_db_connection = raw_data,
                                  cleansed_db_connection = cleansed_data,
                                  source_table = "df_amz_review",
                                  target_table = "df_amz_review",
                                  overwrite = TRUE) {
  
  # Make sure required packages are loaded
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    message("Loading package 'dplyr'")
    library(dplyr)
  }
  if (!requireNamespace("stringr", quietly = TRUE)) {
    message("Loading package 'stringr'")
    library(stringr)
  }
  
  # Check that the source table exists
  if (!source_table %in% DBI::dbListTables(raw_db_connection)) {
    stop(paste("Source table", source_table, "not found in raw_data database"))
  }
  
  # Read source data
  message(paste("Reading data from", source_table))
  df_amazon_reviews <- dplyr::tbl(raw_db_connection, source_table) %>% dplyr::collect()
  
  # Count initial rows
  initial_rows <- nrow(df_amazon_reviews)
  message(paste("Processing", initial_rows, "raw Amazon reviews"))
  
  # Cleanse the data:
  # 1. Fix date format using parse_date_time3 function
  # 2. Rename 'variation' to 'asin' for consistency
  # 3. Filter for valid ASIN format (10 alphanumeric characters)
  cleansed_reviews <- df_amazon_reviews %>%
    dplyr::mutate(
      date = parse_date_time3(date)
    ) %>%
    dplyr::rename(asin = variation) %>%
    dplyr::filter(stringr::str_detect(asin, "^[A-Z0-9]{10}$"))
  
  # Count final rows
  final_rows <- nrow(cleansed_reviews)
  removed_rows <- initial_rows - final_rows
  message(paste(
    "Cleansed data contains", final_rows, "rows.",
    if (removed_rows > 0) paste(removed_rows, "invalid records were removed.") else "All records were valid."
  ))
  
  # Write to cleansed_data database
  if (final_rows > 0) {
    DBI::dbWriteTable(
      cleansed_db_connection,
      target_table,
      cleansed_reviews,
      overwrite = overwrite
    )
    message(paste("Successfully wrote", final_rows, "cleansed reviews to", target_table))
    
    # Generate and display summary statistics
    review_stats <- cleansed_reviews %>%
      dplyr::group_by(rating) %>%
      dplyr::summarize(count = n()) %>%
      dplyr::arrange(dplyr::desc(count))
    
    message("Review statistics by rating:")
    print(review_stats)
  } else {
    warning("No valid records to write to the database")
  }
  
  # Don't return any data
  return(invisible(NULL))
}