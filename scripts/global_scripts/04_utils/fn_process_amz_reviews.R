#' Process AMZ Reviews
#' 
#' This function processes cleansed Amazon reviews by:
#' 1. Extracting product line information from paths
#' 2. Adding product line IDs via joining with product_line_df
#' 3. Adding competitor flags based on existing competitor data
#' 4. Writing the processed data to the processed database
#' 
#' @param cleansed_db_connection Connection to cleansed data database (default: cleansed_data)
#' @param processed_db_connection Connection to processed data database (default: processed_data)
#' @param product_line_df Product line dataframe (default: df_product_line)
#' @param source_table Name of source table in cleansed database (default: "df_amz_review")
#' @param target_table Name of target table in processed database (default: "df_amz_review")
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
#' # Process AMZ reviews
#' process_amz_reviews()
#' }
process_amz_reviews <- function(cleansed_db_connection = cleansed_data,
                                processed_db_connection = processed_data,
                                product_line_df = df_product_line,
                                source_table = "df_amz_review",
                                target_table = "df_amz_review",
                                overwrite = TRUE) {
  
  # Normalize product line data for matching
  product_line_df_norm <- product_line_df %>% 
    dplyr::mutate(cat_name = make_names(product_line_name_english))
  
  # Retrieve cleansed reviews
  df_amz_review <- dplyr::tbl(cleansed_db_connection, source_table) %>% 
    dplyr::collect() %>% 
    dplyr::mutate(
      # Extract category from path
      raw_cat = stringr::str_extract(path, "(?<=amazon_reviews/)[^/]+"),
      # Clean category name for joining
      cat_name = raw_cat %>% 
        stringr::str_remove("^\\d+_") %>% 
        make_names()
    ) %>% 
    # Join with product line data
    dplyr::left_join(product_line_df_norm, by = "cat_name") %>% 
    dplyr::select(-raw_cat, -cat_name)
  
  # Verify product line ID assignment
  if (!all(!is.na(df_amz_review$product_line_id))) {
    warning("Some reviews could not be assigned to a product line!")
  }
  
  # Retrieve competitor products with flag
  df_competitor_product_id_with_flag <- dplyr::tbl(raw_data, "df_competiter_product_id") %>% 
    dplyr::collect() %>% 
    dplyr::mutate(included_competiter = TRUE)
  
  # Add competitor flag to reviews
  df_amz_review_with_flags <- df_amz_review %>% 
    dplyr::left_join(df_competitor_product_id_with_flag) %>% 
    dplyr::mutate(included_competiter = dplyr::coalesce(included_competiter, FALSE)) %>% 
    dplyr::distinct_all() %>% 
    dplyr::relocate(date, asin) %>% 
    dplyr::arrange(asin, date)
  
  # Show competitor flag statistics
  flag_stats <- table(df_amz_review_with_flags$included_competiter, useNA = "always")
  message("Reviews by competitor flag status:")
  print(flag_stats)
  
  # Write to processed database
  DBI::dbWriteTable(
    processed_db_connection,
    target_table,
    df_amz_review_with_flags,
    overwrite = overwrite
  )
  
  # Return the processed dataframe invisibly
  return(invisible(df_amz_review_with_flags))
}