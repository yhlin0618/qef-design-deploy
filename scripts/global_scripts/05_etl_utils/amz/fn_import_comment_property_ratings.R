#' Import Comment Property Ratings
#'
#' Imports or calculates ratings for specific comment properties as part of the
#' D03_06 step in the Positioning Analysis derivation flow. This function creates
#' property score tables for each product line and prepares sampled data for detailed
#' analysis.
#'
#' @param raw_data A DBI connection to the raw data database
#' @param processed_data A DBI connection to the processed data database
#' @param comment_property_rating A DBI connection to the comment property rating database
#' @param vec_product_line_id_noall A vector of product line IDs to process (excluding "all")
#' @param comment_sample_size Integer. Number of comments to sample per product for analysis (default: 10)
#' @param platform Character. Platform identifier (e.g., "amz", "eby") (default: "amz")
#' @param df_platform Data frame containing platform information (default: df_platform)
#' @param type Character vector. Filter comment properties by type (default: c("屬性"))
#'
#' @return Invisible NULL. The function creates database tables as a side effect.
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' dbConnect_from_list("raw_data") 
#' dbConnect_from_list("processed_data") 
#' dbConnect_from_list("comment_property_rating") 
#' 
#' # Import property ratings
#' import_comment_property_ratings(
#'   raw_data = raw_data,
#'   processed_data = processed_data,
#'   comment_property_rating = comment_property_rating,
#'   vec_product_line_id_noall = vec_product_line_id_noall,
#'   comment_sample_size = 10
#' )
#' }
#'
#' @export
import_comment_property_ratings <- function(raw_data = raw_data,
                                          processed_data = processed_data, 
                                          comment_property_rating = comment_property_rating, 
                                          vec_product_line_id_noall = vec_product_line_id_noall,
                                          comment_sample_size = 10,
                                          platform = "amz",
                                          type = c("屬性")) {
  
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("janitor", quietly = TRUE)) library(janitor)
  if (!requireNamespace("tidyr", quietly = TRUE)) library(tidyr)
  
  # Get product ID column name from df_platform
  platform_info <- df_platform %>%
    filter(platform_id == platform) %>%
    slice(1)
  
  if (nrow(platform_info) == 0) {
    stop(paste("Platform", platform, "not found in df_platform"))
  }
  
  product_id_col <- platform_info$product_id_coding
  
  if (is.na(product_id_col)) {
    stop(paste("No product_id_coding defined for platform", platform))
  }
  
  # Process each product line
  for (product_line_id_i in vec_product_line_id_noall) {
    
    # Log processing status
    message("Processing product line: ", product_line_id_i)
    
    # Retrieve filtered review data for this product line
    review_table_name <- paste0("df_", platform, "_review")
    df_review___filtered <- tbl(processed_data, review_table_name) %>% 
      filter(product_line_id == product_line_id_i) %>% 
      collect()
    
    # Retrieve property definitions for this product line
    df_comment_property___filtered <- tbl(comment_property_rating, "df_all_comment_property") %>%
      filter(product_line_id == product_line_id_i) %>% 
      filter(type %in% !!type) %>%  # Filter by specified type(s)
      collect()
    
    colnames(df_comment_property___filtered)
    
    # Create clean column names from property names
    new_columns <- make_names(df_comment_property___filtered$property_name)
    
    # Log the property columns being created
    message("Creating property columns: ", paste(new_columns, collapse = ", "))
    
    # Create review data with new property columns (initialized to NA)
    df_review___new_columns <- df_review___filtered %>% 
      arrange(!!sym(product_id_col), date) %>%       # Sort by product ID and date
      group_by(!!sym(product_id_col)) %>%            # Group by product ID
      mutate(
        # Add empty columns for each property
        !!!setNames(rep(list(NA_character_), length(new_columns)), new_columns)
      ) 
    
    # Create raw table with property columns
    table_name_raw <- paste0("df_comment_property_rating_", product_line_id_i, "___raw")
    dbWriteTable(comment_property_rating, table_name_raw, df_review___new_columns, overwrite = TRUE)
    message("Created raw table: ", table_name_raw)
    
    # Create sampled data for competitor products (for manual review or NLP processing)
    # Using the configurable comment_sample_size parameter
    # Note: For eBay, all products in processed data are competitors by definition
    df_review___sampled <- df_review___new_columns %>%
      slice_tail(n = comment_sample_size) %>% 
      ungroup()
    
    # Log sample size
    message("Sampled competitors: ", paste(names(table(df_review___sampled[[product_id_col]])), 
                                       collapse = ", "))
    
    # Calculate and log estimated processing time
    n_operations <- nrow(df_review___sampled) * length(new_columns)  # Total iterations
    interval <- 10/20                                                    # Seconds per iteration
    
    # Time calculations
    total_seconds <- n_operations * interval
    total_minutes <- total_seconds / 60
    total_hours   <- total_minutes / 60
    total_days    <- total_hours / 24
    
    # Format time display
    time_display <- if (total_seconds < 60) {
      sprintf("%.0f seconds", total_seconds)
    } else if (total_minutes < 60) {
      sprintf("%.1f minutes (%.0f seconds)", total_minutes, total_seconds)
    } else if (total_hours < 24) {
      sprintf("%.2f hours (%.1f minutes)", total_hours, total_minutes)
    } else {
      sprintf("%.2f days (%.2f hours)", total_days, total_hours)
    }
    
    # Log estimated processing time
    message(sprintf(
      "Estimated total processing time: %d operations × %.1f seconds = %s",
      n_operations, interval, time_display
    ))
    
    # Create sampled table
    table_name_sampled <- paste0("df_comment_property_rating_", product_line_id_i, "___sampled")
    dbWriteTable(comment_property_rating, table_name_sampled, df_review___sampled, overwrite = TRUE)
    message("Created sampled table: ", table_name_sampled)
    
    # Create long-format sample data with property definitions for each review
    df_review___long <- df_review___sampled %>%
      tidyr::pivot_longer(
        cols      = tidyselect::all_of(new_columns),      # Columns to expand
        names_to  = "property_name",                      # New column: property name
        values_to = "result"                              # New column: result value
      ) %>% 
      left_join(df_comment_property___filtered)
    
    # Create long-format table for easier processing
    table_name_long <- paste0("df_comment_property_rating_", product_line_id_i, "___sampled_long")
    dbWriteTable(comment_property_rating, table_name_long, df_review___long, overwrite = TRUE)
    message("Created long-format table: ", table_name_long)
  }
  
  # Return invisibly
  invisible(NULL)
}