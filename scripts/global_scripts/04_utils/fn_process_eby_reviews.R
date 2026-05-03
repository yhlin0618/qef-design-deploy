#' Process eBay Reviews
#' 
#' This function processes cleansed eBay reviews by:
#' 1. Filtering to keep only reviews for competitor products
#' 2. Adding product line information from competitor data
#' 3. Enriching with additional metadata (brand, seller) if available
#' 4. Writing the processed data to the processed database
#' 
#' Note: Reviews for products not in the competitor list are removed during processing
#' 
#' @param cleansed_db_connection Connection to cleansed data database (default: cleansed_data)
#' @param processed_db_connection Connection to processed data database (default: processed_data)
#' @param raw_db_connection Connection to raw data database (default: raw_data)
#' @param product_line_df Product line dataframe (default: df_product_line)
#' @param source_table Name of source table in cleansed database (default: "df_eby_review")
#' @param target_table Name of target table in processed database (default: "df_eby_review")
#' @param competitor_table Name of competitor table (default: "df_eby_competitor_product_id")
#' @param overwrite Logical indicating whether to overwrite existing data (default: TRUE)
#' 
#' @return Invisibly returns the processed dataframe
#' 
#' @examples
#' \dontrun{
#' # Connect to databases
#' dbConnect_from_list("raw_data")
#' dbConnect_from_list("cleansed_data")
#' dbConnect_from_list("processed_data")
#'
#' # Process eBay reviews
#' process_eby_reviews()
#' }
#' 
#' @export
process_eby_reviews <- function(cleansed_db_connection = cleansed_data,
                                processed_db_connection = processed_data,
                                raw_db_connection = raw_data,
                                product_line_df = df_product_line,
                                source_table = "df_eby_review",
                                target_table = "df_eby_review",
                                competitor_table = "df_eby_competitor_product_id",
                                overwrite = TRUE) {
  
  # Load required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    library(dplyr)
  }
  if (!requireNamespace("stringr", quietly = TRUE)) {
    library(stringr)
  }
  
  # Retrieve cleansed reviews
  message("Reading cleansed eBay reviews...")
  df_eby_review <- dplyr::tbl(cleansed_db_connection, source_table) %>% 
    dplyr::collect()
  
  initial_rows <- nrow(df_eby_review)
  message(paste("Processing", initial_rows, "cleansed eBay reviews"))
  
  # Remove duplicate reviews based on key fields
  # 使用 ebay_product_number, reviewer, comment 來判斷是否為重複評論
  # 當有重複時，保留最舊的日期（最早的評論）
  df_eby_review_dedup <- df_eby_review %>%
    dplyr::group_by(ebay_product_number, reviewer, comment) %>%
    dplyr::slice_min(date, n = 1, with_ties = FALSE) %>%  # Keep row with minimum date
    dplyr::ungroup()
  
  duplicate_count <- initial_rows - nrow(df_eby_review_dedup)
  if (duplicate_count > 0) {
    message(paste("Removed", duplicate_count, "duplicate reviews from crawler data (kept oldest date for each unique review)"))
  }
  
  df_eby_review <- df_eby_review_dedup
  
  # Check if competitor table exists
  competitor_table_exists <- competitor_table %in% DBI::dbListTables(raw_db_connection)
  
  if (competitor_table_exists) {
    # Retrieve competitor products with product line information
    message(paste("Reading competitor data from", competitor_table))
    # First get all competitor products before deduplication
    df_competitor_products_all <- dplyr::tbl(raw_db_connection, competitor_table) %>% 
      dplyr::collect() %>% 
      dplyr::select(
        dplyr::any_of(c("ebay_product_number", "product_line_id", "brand", "seller", "country"))
      ) %>%
      dplyr::mutate(
        # Ensure ebay_product_number is character for joining
        ebay_product_number = as.character(ebay_product_number)
      )
    
    # Find duplicates
    duplicate_products <- df_competitor_products_all %>%
      dplyr::count(ebay_product_number) %>%
      dplyr::filter(n > 1) %>%
      dplyr::arrange(dplyr::desc(n))
    
    if (nrow(duplicate_products) > 0) {
      message(paste("\nFound", nrow(duplicate_products), "duplicate ebay_product_numbers in competitor table:"))
      # Show details of duplicates
      for (i in 1:min(10, nrow(duplicate_products))) {
        product_num <- duplicate_products$ebay_product_number[i]
        count <- duplicate_products$n[i]
        
        # Get details of this duplicate
        dup_details <- df_competitor_products_all %>%
          dplyr::filter(ebay_product_number == product_num) %>%
          dplyr::select(dplyr::any_of(c("ebay_product_number", "product_line_id", "brand", "seller")))
        
        message(paste0("\n  product: ", product_num, " (", count, " occurrences)"))
        print(dup_details)
      }
      
      if (nrow(duplicate_products) > 10) {
        message(paste("\n  ... and", nrow(duplicate_products) - 10, "more duplicate products"))
      }
      
      message("\nRemoving duplicates - keeping first occurrence of each product number")
    }
    
    # Now deduplicate
    df_competitor_products <- df_competitor_products_all %>%
      dplyr::distinct(ebay_product_number, .keep_all = TRUE)
    
    # Join reviews with competitor data to get product line information
    # Use inner_join to keep only reviews for products in competitor list
    df_eby_review_with_info <- df_eby_review %>% 
      dplyr::inner_join(
        df_competitor_products, 
        by = "ebay_product_number"
      )
    
    # Report how many reviews were filtered out or duplicated
    final_review_rows <- nrow(df_eby_review_with_info)
    filtered_out <- initial_rows - final_review_rows
    
    if (filtered_out > 0) {
      message(paste("Filtered out", filtered_out, 
                    "reviews for non-competitor products"))
    } else if (filtered_out < 0) {
      message(paste("Warning: Join created", abs(filtered_out), 
                    "duplicate rows. This shouldn't happen after deduplication."))
    } else {
      message("All reviews matched competitor products (no filtering needed)")
    }
    
    # All remaining reviews are for competitor products by definition (inner join)
    
    # If brand was missing in reviews but exists in competitor data, update it
    if ("brand_competitor" %in% names(df_eby_review_with_info)) {
      df_eby_review_with_info <- df_eby_review_with_info %>%
        dplyr::mutate(
          brand = dplyr::coalesce(brand, brand_competitor)
        ) %>%
        dplyr::select(-brand_competitor)
    }
    
    # If seller info is available from competitor data, keep it
    if ("seller_competitor" %in% names(df_eby_review_with_info)) {
      df_eby_review_with_info <- df_eby_review_with_info %>%
        dplyr::mutate(
          seller = dplyr::coalesce(seller, seller_competitor)
        ) %>%
        dplyr::select(-seller_competitor)
    }
    
  } else {
    warning(paste("Competitor table", competitor_table, "not found. Cannot process reviews."))
    # Return empty dataframe if no competitor data
    return(invisible(data.frame()))
  }
  
  # Clean up and organize columns
  df_eby_review_processed <- df_eby_review_with_info %>%
    # Remove duplicates if any were introduced by joining
    dplyr::distinct() %>%
    # Reorder columns for better readability
    dplyr::relocate(
      dplyr::any_of(c("date", "ebay_product_number", "product_line_id"))
    ) %>%
    # Sort by product number and date
    dplyr::arrange(ebay_product_number, date)
  
  # Show processing statistics
  message("\nProcessing statistics:")
  
  # Total reviews kept (all are competitor products)
  message(paste("Total reviews retained:", nrow(df_eby_review_processed), 
                "(all for competitor products)"))
  
  # Product line statistics if available
  if ("product_line_id" %in% names(df_eby_review_processed)) {
    product_line_stats <- df_eby_review_processed %>%
      dplyr::group_by(product_line_id) %>%
      dplyr::summarize(
        review_count = n(),
        .groups = 'drop'
      ) %>%
      dplyr::arrange(dplyr::desc(review_count))
    
    message("\nReviews by product line:")
    print(product_line_stats)
  }
  
  # Rating statistics
  if ("rating" %in% names(df_eby_review_processed)) {
    rating_stats <- df_eby_review_processed %>%
      dplyr::group_by(rating) %>%
      dplyr::summarize(count = n(), .groups = 'drop') %>%
      dplyr::arrange(dplyr::desc(count))
    
    message("\nRating distribution:")
    print(rating_stats)
  }
  
  # Write to processed database
  DBI::dbWriteTable(
    processed_db_connection,
    target_table,
    df_eby_review_processed,
    overwrite = overwrite
  )
  
  final_rows <- nrow(df_eby_review_processed)
  message(paste("\nSuccessfully processed", final_rows, "eBay reviews to", target_table))
  
  # Return the processed dataframe invisibly
  return(invisible(df_eby_review_processed))
}