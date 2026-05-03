#' Process Position Table for a Product Line
#'
#' Processes position data for a specific product line, integrating comment properties, 
#' competitor data, and sales data. Calculates ideal values for positioning analysis.
#' Part of the D03_11 step in the Positioning Analysis derivation flow.
#'
#' @param product_line_id Character. The product line ID to process.
#' @param raw_data A DBI connection to the raw data database.
#' @param processed_data A DBI connection to the processed data database.
#' @param app_data A DBI connection to the app data database.
#' @param paste_ Function. The string concatenation function.
#' @param platform Character. Platform identifier ("amz" or "eby") (default: "amz").
#'
#' @return Logical. TRUE if processing succeeded, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # Process a single product line
#' # Process for Amazon
#' process_position_table(
#'   product_line_id = "jew",
#'   raw_data = raw_data,
#'   processed_data = processed_data,
#'   app_data = app_data,
#'   paste_ = paste_,
#'   platform = "amz"
#' )
#' 
#' # Process for eBay
#' process_position_table(
#'   product_line_id = "tur",
#'   raw_data = raw_data,
#'   processed_data = processed_data,
#'   app_data = app_data,
#'   paste_ = paste_,
#'   platform = "eby"
#' )
#' }
#'
#' @export
process_position_table <- function(product_line_id, 
                                  raw_data, 
                                  processed_data, 
                                  app_data,
                                  paste_,
                                  platform = "amz") {
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  message("\n▶ Processing product line: ", product_line_id, " (Platform: ", platform, ")")
  
  # Define platform-specific variables
  product_col <- switch(platform,
    "amz" = "asin",
    "eby" = "ebay_product_number",
    stop(paste("Unknown platform:", platform))
  )
  
  # Table name patterns
  rating_table_suffix <- if (platform == "amz") "by_asin" else "by_product"
  competitor_table <- switch(platform,
    "amz" = "df_amz_competitor_product_id",
    "eby" = "df_eby_competitor_product_id"
  )
  sales_table <- switch(platform,
    "amz" = "df_amz_competitor_sales",
    "eby" = "df_eby_competitor_sales"  # May not exist
  )
  
  # 1. Read original property ratings
  orig_table_name <- paste0("df_comment_property_ratingonly_", rating_table_suffix, "_", product_line_id)
  
  if (!DBI::dbExistsTable(processed_data, orig_table_name)) {
    message("  ⚠️ Table not found in database: ", orig_table_name, 
            ", skipping product line ", product_line_id)
    return(FALSE)
  }
  
  Dta <- dplyr::tbl(processed_data, orig_table_name) %>%
    dplyr::collect()

  # Normalize product ID column: upstream may use "product_id" instead of "asin" (#356)
  if (!product_col %in% names(Dta) && "product_id" %in% names(Dta)) {
    message("  ↳ Renaming product_id → ", product_col, " for compatibility")
    Dta <- Dta %>% dplyr::rename(!!product_col := product_id)
  }

  message("  ✓ Read ", nrow(Dta), " records")

  # 2. Filter product data and apply type filtering
  # Get product list for this product line
  product_list <- dplyr::tbl(raw_data, competitor_table) %>%
    dplyr::collect() %>%
    dplyr::filter(product_line_id == !!product_line_id) %>%
    dplyr::select(!!sym(product_col)) %>%
    dplyr::pull()

  # Keep only products in the list
  Dta <- Dta %>% dplyr::filter(!!sym(product_col) %in% product_list)
  message("  ✓ After filtering: ", nrow(Dta), " records")
  
  # 2a. Get property type information to filter out non-"屬性" columns
  property_types <- dplyr::tbl(raw_data, "df_all_comment_property") %>%
    dplyr::filter(product_line_id == !!product_line_id) %>%
    dplyr::select(property_name, type) %>%
    dplyr::collect()
  
  # Only keep columns for properties with type = "屬性"
  if (nrow(property_types) > 0) {
    # Get property names that should be kept (type = "屬性")
    # Source make_names if not available (may not be loaded by autoinit)
    if (!exists("make_names", mode = "function")) {
      mn_path <- file.path("scripts", "global_scripts", "05_etl_utils",
                           "common", "string", "fn_make_names.R")
      if (file.exists(mn_path)) source(mn_path)
    }
    make_names_safe <- if (exists("make_names", mode = "function")) make_names else identity
    properties_to_keep <- property_types %>%
      dplyr::filter(type == "屬性") %>%
      dplyr::pull(property_name) %>%
      make_names_safe()
    
    # Identify system columns to always keep
    system_cols <- c(product_col, "brand", "rating", "sales", "product_line_id")
    
    # Get current column names and filter to keep only desired properties
    current_cols <- colnames(Dta)
    cols_to_keep <- c(
      intersect(system_cols, current_cols),  # System columns that exist
      intersect(properties_to_keep, current_cols)  # Property columns that should be kept
    )
    
    # Select only the columns we want to keep
    Dta <- Dta %>% dplyr::select(dplyr::all_of(cols_to_keep))
    
    message("  ✓ Applied type='屬性' filter: keeping ", length(properties_to_keep), " property columns")
    message("    Properties kept: ", paste(properties_to_keep, collapse = ", "))
  } else {
    message("  ⚠️ No property type information found, keeping all columns")
  }
  
  # 3. Remove columns with too many missing values
  # Keep columns with less than 50% missing values
  threshold <- nrow(Dta) / 2
  Dta <- Dta[, colSums(is.na(Dta)) <= threshold]
  
  # 4. Read sales data (if available)
  if (DBI::dbExistsTable(raw_data, sales_table)) {
    cl_used_sales <- dplyr::tbl(raw_data, sales_table) %>% 
      dplyr::group_by(!!sym(product_col)) %>% 
      dplyr::summarise(sales = sum(sales, na.rm = TRUE)) %>% 
      dplyr::collect()
  } else {
    message("  ⚠️ Sales table not found: ", sales_table, ", using review counts as proxy")
    # For eBay, use review counts as a proxy for sales
    if (platform == "eby") {
      cl_used_sales <- dplyr::tbl(processed_data, "df_eby_review") %>%
        dplyr::group_by(!!sym(product_col)) %>%
        dplyr::summarise(sales = n()) %>%  # Count of reviews as proxy
        dplyr::collect()
    } else {
      cl_used_sales <- data.frame()  # Empty dataframe
      cl_used_sales[[product_col]] <- character()
      cl_used_sales$sales <- numeric()
    }
  }
  
  # 5. Integrate data
  combined_position <- dplyr::tbl(raw_data, competitor_table) %>%
    dplyr::collect() %>%
    dplyr::filter(product_line_id == !!product_line_id) %>%
    dplyr::select(brand, !!sym(product_col)) %>%
    dplyr::left_join(Dta, by = product_col) %>%
    dplyr::left_join(cl_used_sales, by = product_col) %>%
    dplyr::mutate(
      # Fix ISSUE_113: Convert empty strings to NA first (MP114 - input validation)
      brand = dplyr::na_if(brand, ""),
      # Then handle NA values
      brand = tidyr::replace_na(brand, "UNKNOWN")
    )
  
  # 6. Calculate ideal values
  ideal_data <- calculate_ideal_values(combined_position, product_col = product_col)
  
  # 7. Integrate and store data
  combined_position___with_ideal <- combined_position %>% 
    dplyr::rows_append(ideal_data) %>% 
    dplyr::relocate(!!sym(product_col), brand, .before = dplyr::everything()) %>%  # Move product ID and brand to the front
    dplyr::relocate(dplyr::any_of("rating"), .after = dplyr::last_col()) %>%    # Move rating to the end if exists
    dplyr::mutate(product_line_id = product_line_id) %>%
    # Rename product column to standardized product_id
    dplyr::rename(product_id = !!sym(product_col))
  
  # Save to temporary table
  new_table_name <- paste_("df_position", product_line_id)
  DBI::dbWriteTable(
    app_data, 
    new_table_name, 
    combined_position___with_ideal, 
    temporary = TRUE, 
    overwrite = TRUE
  )
  
  message("  ✅ Product line processing complete, saved to table ", new_table_name)
  return(TRUE)
}

#' Calculate Ideal Values for Positioning
#'
#' Calculates ideal values for product positioning based on ratings and sales data.
#' Creates a weighted average where ideal values are 60% sales-based and 40% rating-based.
#'
#' @param combined_position Data frame. The combined position data with brand, product ID, ratings, and sales.
#' @param product_col Character. The name of the product ID column ("asin" or "ebay_product_number").
#'
#' @return Data frame. The ideal values for the "Ideal" row.
#'
#' @noRd
calculate_ideal_values <- function(combined_position, product_col = "asin") {
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  
  # Prepare calculation data
  combined_position___without_brand_product <- combined_position %>% 
    dplyr::select(-dplyr::any_of(c(product_col, "brand")))
  
  columns_to_remove <- c("rating", "sales")
  
  # Rating-based ideal calculation (#357: use where(is.numeric) to exclude character columns)
  numeric_data <- combined_position___without_brand_product %>%
    dplyr::select(-dplyr::any_of(columns_to_remove)) %>%
    dplyr::select(where(is.numeric))

  rating_ideal <- numeric_data %>%
    dplyr::summarise(dplyr::across(dplyr::everything(),
                    ~ sum(.x * combined_position___without_brand_product$rating /
                           sum(combined_position___without_brand_product$rating, na.rm = TRUE),
                         na.rm = TRUE)))

  # Sales-based ideal calculation
  sales_ideal <- numeric_data %>%
    dplyr::summarise(dplyr::across(dplyr::everything(),
                    ~ sum(.x * combined_position___without_brand_product$sales /
                           sum(combined_position___without_brand_product$sales, na.rm = TRUE),
                         na.rm = TRUE)))
  
  # Combined ideal: 60% sales + 40% rating  
  Ideal <- dplyr::bind_rows(rating_ideal, sales_ideal, 0.6 * sales_ideal + 0.4 * rating_ideal)
  
  if (ncol(Ideal) > 0) {
    Ideal <- Ideal %>% dplyr::mutate(
      brand = c("Rating", "Revenue", "Ideal")
    )
    Ideal[[product_col]] <- c("Rating", "Revenue", "Ideal")
    # Return all three special records: Rating, Revenue, and Ideal
    return(Ideal)
  } else {
    # If no numeric columns, create minimal special records
    special_records <- data.frame(
      brand = c("Rating", "Revenue", "Ideal")
    )
    special_records[[product_col]] <- c("Rating", "Revenue", "Ideal")
    return(special_records)
  }
  
  # This line should never be reached, but keep for safety
  return(Ideal)
}