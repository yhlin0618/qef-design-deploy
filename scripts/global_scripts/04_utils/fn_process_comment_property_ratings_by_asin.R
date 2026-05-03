#' Process Comment Property Ratings by ASIN
#'
#' Processes comment property ratings by ASIN for a specific product line. 
#' This function handles data aggregation, NA handling, and imputation.
#' Part of D03_09 step in the positioning analysis derivation flow.
#'
#' @param processed_data A DBI connection to the processed data database.
#' @param product_line_id Character. The product line ID to process.
#' @param impute_missing Logical. Whether to impute missing values using mice (default: TRUE).
#' @param m Integer. Number of imputations to create (default: 5).
#' @param maxit Integer. Maximum number of iterations for mice (default: 20).
#' @param seed Integer. Random seed for reproducibility (default: 500).
#'
#' @return Logical. TRUE if processing succeeded, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # Connect to database
#' dbConnect_from_list("processed_data", read_only = FALSE)
#' 
#' # Process a single product line
#' process_comment_property_ratings_by_asin(
#'   processed_data = processed_data,
#'   product_line_id = "jew",
#'   impute_missing = TRUE
#' )
#' }
#'
#' @export
process_comment_property_ratings_by_asin <- function(processed_data,
                                                   product_line_id,
                                                   impute_missing = TRUE,
                                                   m = 5,
                                                   maxit = 20,
                                                   seed = 500) {
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("mice", quietly = TRUE)) library(mice)
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  # Source required utility functions if not already in global environment
  if (!exists("safe_mean", mode = "function")) {
    safe_mean <- function(x) {
      m <- mean(x, na.rm = TRUE)
      if (is.nan(m)) NA_real_ else m
    }
  }
  
  if (!exists("replace_nan", mode = "function")) {
    replace_nan <- function(df) {
      df[] <- lapply(df, function(col) if (is.numeric(col)) dplyr::na_if(col, NaN) else col)
      df
    }
  }
  
  # Define table names
  orig_table <- paste0("df_comment_property_ratingonly_", product_line_id)
  new_table <- paste0("df_comment_property_ratingonly_by_asin_", product_line_id)
  
  # Check if original table exists
  if (!DBI::dbExistsTable(processed_data, orig_table)) {
    message("✘ Source table not found: ", orig_table)
    return(FALSE)
  }
  
  # Step 1: Export, group by ASIN/product_id, and handle NAs
  message("▶ Processing product line: ", product_line_id)
  Dta <- tryCatch({
    collected <- dplyr::tbl(processed_data, orig_table) |> dplyr::collect()
    # Detect product ID column: prefer "asin", fall back to "product_id" (#356)
    id_col <- if ("asin" %in% names(collected)) "asin" else "product_id"
    message("  ↳ Using product ID column: ", id_col)
    collected |>
      dplyr::group_by(dplyr::across(dplyr::all_of(id_col))) |>
      dplyr::summarise(dplyr::across(where(is.numeric), safe_mean), .groups = "drop") |>
      dplyr::select(-dplyr::any_of(c("likes_count", "image_count"))) |>
      replace_nan()
  }, error = function(e) {
    message("✘ Data retrieval/summarization failed: ", e$message)
    return(NULL)
  })
  
  if (is.null(Dta)) {
    return(FALSE)
  }
  
  message("  ↳ Number of records after summarization: ", nrow(Dta))
  
  # Step 2: Clean column names
  # Defensive loading: make_names/clean_column_names may not be loaded by autoinit (#356)
  if (!exists("make_names", mode = "function")) {
    mn_path <- file.path("scripts", "global_scripts", "05_etl_utils",
                         "common", "string", "fn_make_names.R")
    if (file.exists(mn_path)) source(mn_path)
  }
  if (!exists("clean_column_names", mode = "function")) {
    ccn_path <- file.path("scripts", "global_scripts", "05_etl_utils",
                          "common", "string", "fn_clean_column_names.R")
    if (file.exists(ccn_path)) source(ccn_path)
  }
  make_names_safe <- if (exists("make_names", mode = "function")) make_names else identity
  clean_col_safe <- if (exists("clean_column_names", mode = "function")) clean_column_names else identity
  tmp_names <- make_names_safe(colnames(Dta)) |> clean_col_safe()
  tmp_names[tmp_names == "品牌"] <- "品牌價值"
  colnames(Dta) <- tmp_names
  
  # Step 3: Handle missing values
  na_row_cnt <- rowSums(is.na(Dta))
  max_na_count <- max(na_row_cnt)
  tab_na <- table(na_row_cnt)
  message("  ↳ Missing value distribution: ", toString(paste(names(tab_na), tab_na, sep = ":")))
  
  # Step 3a: Impute missing values if required
  if (max_na_count == 0) {
    message("  ↳ No missing values to impute, writing directly")
    Dta_imp_full <- Dta
  } else if (impute_missing) {
    # Create subset for imputation (exclude rows with too many NAs)
    Dta_subset <- Dta[na_row_cnt < max_na_count, ] |> as.data.frame()
    
    # Replace NaN values again (for safety)
    Dta_subset <- replace_nan(Dta_subset)
    
    # Set up mice method (only for numeric columns)
    mice_cols <- which(sapply(Dta_subset, is.numeric))
    mice_methods <- rep("", ncol(Dta_subset))
    mice_methods[mice_cols] <- "pmm"
    
    # Run mice imputation
    message("  ↳ Starting mice() imputation...")
    Dta_imp <- tryCatch({
      mice::mice(Dta_subset, m = m, method = mice_methods,
                 maxit = maxit, seed = seed, printFlag = FALSE) |>
        mice::complete()
    }, error = function(e) {
      message("✘ mice() imputation failed: ", e$message, " —— using original data with NAs")
      return(Dta_subset)
    })
    
    # Merge imputed data back to full dataset
    Dta_imp_full <- Dta
    Dta_imp_full[na_row_cnt < max_na_count, ] <- Dta_imp
  } else {
    # Skip imputation if not required
    message("  ↳ Imputation disabled, using data with NAs")
    Dta_imp_full <- Dta
  }
  
  # Step 4: Write data to new table
  ok <- tryCatch({
    DBI::dbWriteTable(processed_data, new_table, Dta_imp_full,
                     overwrite = TRUE, row.names = FALSE)
    TRUE
  }, error = function(e) {
    message("✘ Database write failed: ", e$message)
    FALSE
  })
  
  if (ok) {
    message("✓ Product line ", product_line_id,
            " processing complete, data written to table ", new_table)
  }
  
  return(ok)
}