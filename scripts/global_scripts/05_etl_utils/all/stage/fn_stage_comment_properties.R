#' Stage Comment Properties Data
#'
#' This function stages comment properties data from raw format to staged format,
#' applying data validation, type optimization, and quality checks.
#' It supports the ETL05 staging phase of the comment properties pipeline.
#'
#' @param raw_data A data frame containing raw comment properties data
#' @param perform_validation Logical. Whether to perform data validation checks (default: TRUE)
#' @param optimize_types Logical. Whether to optimize data types for storage (default: TRUE)
#' @param check_duplicates Logical. Whether to check for duplicate entries (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of staged comment properties with validation metadata
#'
#' @examples
#' \dontrun{
#' # Stage comment properties data
#' staged_data <- stage_comment_properties(
#'   raw_data = raw_comment_properties,
#'   perform_validation = TRUE,
#'   optimize_types = TRUE,
#'   check_duplicates = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
stage_comment_properties <- function(raw_data,
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
  
  message("STAGE: Starting comment properties staging...")
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
    
    if ("property_id" %in% names(staged_data)) {
      staged_data$property_id <- as.integer(staged_data$property_id)
    }
    
    if ("property_name" %in% names(staged_data)) {
      staged_data$property_name <- as.character(staged_data$property_name)
    }
    
    if ("property_name_english" %in% names(staged_data)) {
      staged_data$property_name_english <- as.character(staged_data$property_name_english)
    }
    
    # Handle numeric columns
    numeric_cols <- c("frequency", "proportion")
    for (col in numeric_cols) {
      if (col %in% names(staged_data)) {
        staged_data[[col]] <- as.numeric(staged_data[[col]])
      }
    }
    
    # Handle text columns
    text_cols <- c("definition", "review_1", "review_2", "review_3", 
                   "translation_1", "translation_2", "translation_3", "type", "note")
    for (col in text_cols) {
      if (col %in% names(staged_data)) {
        staged_data[[col]] <- as.character(staged_data[[col]])
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
    required_cols <- c("product_line_id", "property_id", "property_name", "property_name_english")
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
    
    # Validate property_id uniqueness within product lines
    if (all(c("product_line_id", "property_id") %in% names(staged_data))) {
      duplicate_keys <- staged_data %>%
        dplyr::group_by(product_line_id, property_id) %>%
        dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
        dplyr::filter(n > 1)
      
      if (nrow(duplicate_keys) > 0) {
        validation_results$duplicate_property_ids <- duplicate_keys
        message("STAGE: Warning - ", nrow(duplicate_keys), " duplicate property_id values found within product lines")
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
    
    # Check for duplicate property_id within product lines
    if (all(c("product_line_id", "property_id") %in% names(staged_data))) {
      # Mark duplicate records
      staged_data <- staged_data %>%
        dplyr::group_by(product_line_id, property_id) %>%
        dplyr::mutate(
          etl_duplicate_flag = dplyr::n() > 1,
          etl_duplicate_group_size = dplyr::n()
        ) %>%
        dplyr::ungroup()
      
      duplicate_count <- sum(staged_data$etl_duplicate_flag, na.rm = TRUE)
      
      if (duplicate_count > 0) {
        message("STAGE: Found ", duplicate_count, " duplicate records")
        
        # Log duplicate details
        dup_summary <- staged_data[staged_data$etl_duplicate_flag, c("product_line_id", "property_id", "property_name")]
        message("STAGE: Duplicate summary (first 5):")
        print(head(dup_summary, 5))
        
      } else {
        message("STAGE: No duplicates found")
      }
    } else {
      message("STAGE: Cannot check duplicates - missing key columns")
      staged_data$etl_duplicate_flag <- FALSE
      staged_data$etl_duplicate_group_size <- 1
    }
    
    message("STAGE: Duplicate checking completed")
  }
  
  # =====================================================================
  # STEP 5: Add Staging Metadata
  # =====================================================================
  staged_data$etl_phase <- "staged"
  staged_data$etl_staging_timestamp <- Sys.time()
  staged_data$etl_records_processed <- nrow(raw_data)
  
  # Add row processing order
  staged_data$etl_processing_order <- 1:nrow(staged_data)
  
  message("STAGE: Added staging metadata")
  message("STAGE: Staging completed - ", nrow(staged_data), " records processed")
  
  return(staged_data)
}