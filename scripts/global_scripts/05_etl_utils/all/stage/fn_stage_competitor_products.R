#' Stage Competitor products Data
#'
#' This function stages competitor product data from raw format to staged format,
#' applying data validation, type optimization, and quality checks.
#' It supports the ETL04 staging phase of the competitor analysis pipeline.
#'
#' @param raw_data A data frame containing raw competitor products data
#' @param platform Character string specifying the platform (e.g., "amz", "eby")
#' @param perform_validation Logical. Whether to perform data validation checks (default: TRUE)
#' @param optimize_types Logical. Whether to optimize data types for storage (default: TRUE)
#' @param check_duplicates Logical. Whether to check for duplicate entries (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of staged competitor products with validation metadata
#'
#' @examples
#' \dontrun{
#' # Stage competitor products data
#' staged_data <- stage_competitor_products(
#'   raw_data = raw_competitor_products,
#'   platform = "amz",
#'   perform_validation = TRUE,
#'   optimize_types = TRUE,
#'   check_duplicates = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
stage_competitor_products <- function(raw_data,
                                  platform = "amz",
                                  perform_validation = TRUE,
                                  optimize_types = TRUE,
                                  check_duplicates = TRUE,
                                  encoding_target = "UTF-8") {
  
  # Validate input parameters
  if (!is.data.frame(raw_data)) {
    stop("raw_data must be a data frame")
  }
  
  if (nrow(raw_data) == 0) {
    stop("raw_data cannot be empty")
  }
  
  message("STAGE: Starting competitor products staging for platform: ", platform)
  message("STAGE: Processing ", nrow(raw_data), " raw records")
  
  # Initialize staged data as copy of raw data
  staged_data <- raw_data
  
  # =====================================================================
  # STEP 1: Data Type Optimization
  # =====================================================================
  if (optimize_types) {
    message("STAGE: Optimizing data types...")
    
    # Convert character columns to appropriate types
    if ("product_line_id" %in% names(staged_data)) {
      staged_data$product_line_id <- as.character(staged_data$product_line_id)
    }
    
    if ("asin" %in% names(staged_data)) {
      staged_data$asin <- as.character(staged_data$asin)
    }
    
    if ("brand" %in% names(staged_data)) {
      staged_data$brand <- as.character(staged_data$brand)
    }
    
    # Handle numeric columns if they exist
    numeric_cols <- c("price", "rating", "num_rating", "sales_rank")
    for (col in numeric_cols) {
      if (col %in% names(staged_data)) {
        staged_data[[col]] <- as.numeric(staged_data[[col]])
      }
    }
    
    message("STAGE: Data type optimization completed")
  }
  
  # =====================================================================
  # STEP 2: Encoding Normalization
  # =====================================================================
  if (encoding_target == "UTF-8") {
    message("STAGE: Normalizing text encoding to UTF-8...")
    
    # Convert character columns to UTF-8
    char_cols <- names(staged_data)[sapply(staged_data, is.character)]
    for (col in char_cols) {
      if (col %in% names(staged_data)) {
        staged_data[[col]] <- iconv(staged_data[[col]], to = "UTF-8")
      }
    }
    
    message("STAGE: Encoding normalization completed")
  }
  
  # =====================================================================
  # STEP 3: Data Validation
  # =====================================================================
  if (perform_validation) {
    message("STAGE: Performing data validation...")
    
    # Initialize validation results
    validation_results <- list()
    
    # Check for required columns
    required_cols <- c("product_line_id", "asin", "brand")
    missing_cols <- setdiff(required_cols, names(staged_data))
    
    if (length(missing_cols) > 0) {
      validation_results$missing_columns <- missing_cols
      warning("Missing required columns: ", paste(missing_cols, collapse = ", "))
    }
    
    # Check for empty values in critical columns
    for (col in required_cols) {
      if (col %in% names(staged_data)) {
        empty_count <- sum(is.na(staged_data[[col]]) | staged_data[[col]] == "")
        if (empty_count > 0) {
          validation_results[[paste0(col, "_empty_count")]] <- empty_count
          message("STAGE: Warning - ", empty_count, " empty values found in ", col)
        }
      }
    }
    
    # Add validation metadata to staged data
    staged_data$etl_validation_status <- "validated"
    staged_data$etl_validation_timestamp <- Sys.time()
    
    # Calculate data quality score
    total_cells <- nrow(staged_data) * length(required_cols)
    empty_cells <- sum(sapply(required_cols, function(col) {
      if (col %in% names(staged_data)) {
        sum(is.na(staged_data[[col]]) | staged_data[[col]] == "")
      } else {
        nrow(staged_data)  # All cells are "missing" if column doesn't exist
      }
    }))
    
    data_quality_score <- round((1 - empty_cells / total_cells) * 100, 2)
    staged_data$etl_data_quality_score <- data_quality_score
    
    message("STAGE: Data quality score: ", data_quality_score, "%")
    message("STAGE: Data validation completed")
  }
  
  # =====================================================================
  # STEP 4: Duplicate Detection
  # =====================================================================
  if (check_duplicates) {
    message("STAGE: Checking for duplicates...")
    
    # Identify duplicate detection column
    product_id_col <- if ("asin" %in% names(staged_data)) "asin" else "product_id"
    
    if (product_id_col %in% names(staged_data)) {
      # Count duplicates
      duplicate_counts <- table(staged_data[[product_id_col]])
      duplicates <- names(duplicate_counts[duplicate_counts > 1])
      
      if (length(duplicates) > 0) {
        message("STAGE: Found ", length(duplicates), " duplicate ", product_id_col, " values")
        
        # Add duplicate flag
        staged_data$etl_duplicate_flag <- staged_data[[product_id_col]] %in% duplicates
        
        # Log duplicate details
        dup_summary <- staged_data[staged_data$etl_duplicate_flag, c("product_line_id", product_id_col, "brand")]
        message("STAGE: Duplicate summary (first 5):")
        print(head(dup_summary, 5))
        
      } else {
        message("STAGE: No duplicates found")
        staged_data$etl_duplicate_flag <- FALSE
      }
    } else {
      message("STAGE: No product ID column found for duplicate checking")
      staged_data$etl_duplicate_flag <- FALSE
    }
    
    message("STAGE: Duplicate checking completed")
  }
  
  # =====================================================================
  # STEP 5: Add Staging Metadata
  # =====================================================================
  staged_data$etl_phase <- "staged"
  staged_data$etl_staging_timestamp <- Sys.time()
  staged_data$etl_platform <- platform
  staged_data$etl_records_processed <- nrow(raw_data)
  
  # Add row processing order
  staged_data$etl_processing_order <- 1:nrow(staged_data)
  
  message("STAGE: Added staging metadata")
  message("STAGE: Staging completed - ", nrow(staged_data), " records processed")
  
  return(staged_data)
}