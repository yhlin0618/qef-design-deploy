#' Cleanse eBay Review Data
#'
#' This function cleanses raw eBay review data by fixing date formats,
#' standardizing fields, and filtering for valid eBay product numbers. It supports 
#' the D03_04 step of the Positioning Analysis derivation flow for eBay data.
#'
#' @param raw_db_connection A DBI connection to the raw_data database
#' @param cleansed_db_connection A DBI connection to the cleansed_data database
#' @param source_table The name of the source table in raw_data (default: "df_eby_review")
#' @param target_table The name of the target table in cleansed_data (default: "df_eby_review")
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
#' # Cleanse eBay reviews
#' cleanse_eby_reviews(
#'   raw_db_connection = raw_data,
#'   cleansed_db_connection = cleansed_data
#' )
#' }
#'
#' @export
cleanse_eby_reviews <- function(raw_db_connection = raw_data,
                               cleansed_db_connection = cleansed_data,
                               source_table = "df_eby_review",
                               target_table = "df_eby_review",
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
  df_eby_reviews <- dplyr::tbl(raw_db_connection, source_table) %>% dplyr::collect()
  
  # Count initial rows
  initial_rows <- nrow(df_eby_reviews)
  message(paste("Processing", initial_rows, "raw eBay reviews"))
  
  # Cleanse the data:
  # 1. Fix date format for fb_context_timestamp
  # 2. Extract eBay product number from link URL
  # 3. Standardize rating field (fb_rating)
  # 4. Clean reviewer field (fb_context_user)
  # First check the type of fb_context_timestamp
  if (inherits(df_eby_reviews$fb_context_timestamp, c("POSIXct", "POSIXt", "Date"))) {
    # Already a timestamp, use as is
    cleansed_reviews <- df_eby_reviews %>%
      dplyr::mutate(
        date = fb_context_timestamp,
        # Extract eBay product number from link (e.g., from "/itm/126042426613?")
        ebay_product_number = stringr::str_extract(as.character(link), "(?<=itm/)[0-9]+"),
        # Standardize rating to uppercase
        rating = toupper(as.character(fb_rating)),
        # Create reviewer field without rating score (e.g., "1***2" from "1***2 (548)")
        reviewer = stringr::str_extract(as.character(fb_context_user), "^[^\\(]+") %>% stringr::str_trim(),
        # Keep buyer rating score as numeric
        buyer_rating_score = as.numeric(as.character(fb_buyer_rating_score)),
        # Rename fields for consistency
        comment = fb_comment,
        product_name = product_name,
        store_name = store_name
      )
  } else {
    # Parse timestamp from character
    cleansed_reviews <- df_eby_reviews %>%
      dplyr::mutate(
        date = parse_date_time3(as.character(fb_context_timestamp)),
        # Extract eBay product number from link (e.g., from "/itm/126042426613?")
        ebay_product_number = stringr::str_extract(as.character(link), "(?<=itm/)[0-9]+"),
        # Standardize rating to uppercase
        rating = toupper(as.character(fb_rating)),
        # Create reviewer field without rating score (e.g., "1***2" from "1***2 (548)")
        reviewer = stringr::str_extract(as.character(fb_context_user), "^[^\\(]+") %>% stringr::str_trim(),
        # Keep buyer rating score as numeric
        buyer_rating_score = as.numeric(as.character(fb_buyer_rating_score)),
        # Rename fields for consistency
        comment = fb_comment,
        product_name = product_name,
        store_name = store_name
      )
  }
  
  # Filter for valid eBay product numbers
  # eBay product numbers are typically 12 digits but can vary (10-13 digits)
  cleansed_reviews <- cleansed_reviews %>%
    dplyr::filter(
      !is.na(ebay_product_number),
      stringr::str_detect(ebay_product_number, "^\\d{10,13}$")
    )
  
  # Remove duplicate reviews if any
  # Identify duplicates based on product number, reviewer, and date
  cleansed_reviews <- cleansed_reviews %>%
    dplyr::distinct(ebay_product_number, reviewer, date, .keep_all = TRUE)
  
  # Select only necessary columns for cleansed output
  # Use any_of() to handle columns that might not exist
  cleansed_reviews <- cleansed_reviews %>%
    dplyr::select(
      # Primary identifiers
      dplyr::any_of(c("id", "ebay_product_number")),
      
      # product information
      dplyr::any_of(c("product_name", "store_name")),
      
      # Review information
      dplyr::any_of(c("reviewer", "rating", "buyer_rating_score", "comment")),
      
      # Temporal information
      dplyr::any_of(c("date", "fb_context_time", "crawler_date")),
      
      # Metadata
      dplyr::any_of(c("link", "fb_product_summary", "fb_product_price")),
      
      # Keep all other columns that might be useful
      dplyr::everything()
    )
  
  # Count final rows
  final_rows <- nrow(cleansed_reviews)
  removed_rows <- initial_rows - final_rows
  message(paste(
    "Cleansed data contains", final_rows, "rows.",
    if (removed_rows > 0) paste(removed_rows, "invalid/duplicate records were removed.") else "All records were valid."
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
    
    message("\nReview statistics by rating:")
    print(review_stats)
    
    # Show statistics by store
    store_stats <- cleansed_reviews %>%
      dplyr::group_by(store_name) %>%
      dplyr::summarize(
        review_count = n(),
        positive_count = sum(rating == "POSITIVE", na.rm = TRUE),
        negative_count = sum(rating == "NEGATIVE", na.rm = TRUE),
        positive_rate = round(positive_count / review_count * 100, 1)
      ) %>%
      dplyr::arrange(dplyr::desc(review_count))
    
    message("\nTop 10 stores by review count:")
    print(head(store_stats, 10))
    
    # Show date range of reviews
    if ("date" %in% names(cleansed_reviews)) {
      date_range <- cleansed_reviews %>%
        dplyr::summarize(
          earliest_review = min(date, na.rm = TRUE),
          latest_review = max(date, na.rm = TRUE)
        )
      
      message(paste("\nReview date range:", 
                    date_range$earliest_review, "to", date_range$latest_review))
    }
    
  } else {
    warning("No valid records to write to the database")
  }
  
  # Don't return any data
  return(invisible(NULL))
}