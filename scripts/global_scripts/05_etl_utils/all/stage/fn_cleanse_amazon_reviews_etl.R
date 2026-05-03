#' Cleanse Amazon Reviews for ETL Pipeline
#'
#' This function cleanses raw Amazon review data for the ETL06 staging phase,
#' applying data validation, date formatting, and quality scoring.
#' It supports the ETL06 staging phase of the reviews processing pipeline.
#'
#' @param raw_data A data frame containing raw Amazon reviews data
#' @param perform_validation Logical. Whether to perform data validation checks (default: TRUE)
#' @param fix_dates Logical. Whether to fix date formats (default: TRUE)
#' @param filter_valid_asins Logical. Whether to filter for valid ASINs (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of cleansed Amazon reviews with validation metadata
#'
#' @examples
#' \dontrun{
#' # Cleanse Amazon reviews data
#' cleansed_data <- cleanse_amazon_reviews_etl(
#'   raw_data = raw_reviews,
#'   perform_validation = TRUE,
#'   fix_dates = TRUE,
#'   filter_valid_asins = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
cleanse_amazon_reviews_etl <- function(raw_data,
                                      perform_validation = TRUE,
                                      fix_dates = TRUE,
                                      filter_valid_asins = TRUE,
                                      encoding_target = "UTF-8") {
  
  # Validate input parameters
  if (!is.data.frame(raw_data)) {
    stop("raw_data must be a data frame")
  }
  
  if (nrow(raw_data) == 0) {
    stop("raw_data cannot be empty")
  }
  
  message("STAGE: Starting Amazon reviews cleansing...")
  message("STAGE: Processing ", nrow(raw_data), " raw reviews")
  
  # Initialize cleansed data as copy of raw data
  cleansed_data <- raw_data
  
  # =====================================================================
  # STEP 1: Date Format Fixing
  # =====================================================================
  if (fix_dates) {
    message("STAGE: Fixing date formats...")
    
    if ("review_date" %in% names(cleansed_data)) {
      # Convert various date formats to standard format
      cleansed_data$review_date <- tryCatch({
        as.Date(cleansed_data$review_date)
      }, error = function(e) {
        # Try alternative date parsing
        lubridate::ymd(cleansed_data$review_date)
      })
      
      # Create year and month columns for analysis
      cleansed_data$review_year <- lubridate::year(cleansed_data$review_date)
      cleansed_data$review_month <- lubridate::month(cleansed_data$review_date)
    }
    
    message("STAGE: Date format fixing completed")
  }
  
  # =====================================================================
  # STEP 2: Field Standardization
  # =====================================================================
  message("STAGE: Standardizing fields...")
  
  # Standardize ASIN format
  if ("asin" %in% names(cleansed_data)) {
    cleansed_data$asin <- toupper(trimws(cleansed_data$asin))
  }
  
  # Standardize rating format
  if ("rating" %in% names(cleansed_data)) {
    cleansed_data$rating <- as.numeric(cleansed_data$rating)
  }
  
  # Clean review text
  if ("review_text" %in% names(cleansed_data)) {
    cleansed_data$review_text <- trimws(cleansed_data$review_text)
    cleansed_data$review_text_length <- nchar(cleansed_data$review_text)
  }
  
  # Clean reviewer ID
  if ("reviewer_id" %in% names(cleansed_data)) {
    cleansed_data$reviewer_id <- trimws(cleansed_data$reviewer_id)
  }
  
  message("STAGE: Field standardization completed")
  
  # =====================================================================
  # STEP 3: ASIN Validation and Filtering
  # =====================================================================
  if (filter_valid_asins) {
    message("STAGE: Filtering for valid ASINs...")
    
    if ("asin" %in% names(cleansed_data)) {
      # Count records before filtering
      original_count <- nrow(cleansed_data)
      
      # Filter for valid ASINs (10 characters, alphanumeric)
      valid_asin_pattern <- "^[A-Z0-9]{10}$"
      cleansed_data <- cleansed_data %>%
        dplyr::filter(grepl(valid_asin_pattern, asin, perl = TRUE))
      
      filtered_count <- nrow(cleansed_data)
      removed_count <- original_count - filtered_count
      
      message("STAGE: Filtered ", removed_count, " records with invalid ASINs")
      message("STAGE: ", filtered_count, " records with valid ASINs remain")
    }
    
    message("STAGE: ASIN filtering completed")
  }
  
  # =====================================================================
  # STEP 4: Encoding Normalization
  # =====================================================================
  if (encoding_target == "UTF-8") {
    message("STAGE: Normalizing text encoding to UTF-8...")
    
    # Convert character columns to UTF-8
    char_cols <- names(cleansed_data)[sapply(cleansed_data, is.character)]
    for (col in char_cols) {
      if (col %in% names(cleansed_data)) {
        cleansed_data[[col]] <- iconv(cleansed_data[[col]], to = "UTF-8")
      }
    }
    
    message("STAGE: Encoding normalization completed")
  }
  
  # =====================================================================
  # STEP 5: Data Validation
  # =====================================================================
  if (perform_validation) {
    message("STAGE: Performing data validation...")
    
    # Check for required columns
    required_cols <- c("asin", "review_date", "review_text", "rating")
    missing_cols <- setdiff(required_cols, names(cleansed_data))
    
    if (length(missing_cols) > 0) {
      warning("Missing required columns: ", paste(missing_cols, collapse = ", "))
    }
    
    # Check for empty values in critical columns
    for (col in required_cols) {
      if (col %in% names(cleansed_data)) {
        empty_count <- sum(is.na(cleansed_data[[col]]) | cleansed_data[[col]] == "")
        if (empty_count > 0) {
          message("STAGE: Warning - ", empty_count, " empty values found in ", col)
        }
      }
    }
    
    # Calculate data quality score
    total_cells <- nrow(cleansed_data) * length(required_cols)
    empty_cells <- sum(sapply(required_cols, function(col) {
      if (col %in% names(cleansed_data)) {
        sum(is.na(cleansed_data[[col]]) | cleansed_data[[col]] == "")
      } else {
        nrow(cleansed_data)  # All cells are "missing" if column doesn't exist
      }
    }))
    
    data_quality_score <- round((1 - empty_cells / total_cells) * 100, 2)
    cleansed_data$etl_data_quality_score <- data_quality_score
    
    message("STAGE: Data quality score: ", data_quality_score, "%")
    message("STAGE: Data validation completed")
  }
  
  # =====================================================================
  # STEP 6: Add Staging Metadata
  # =====================================================================
  cleansed_data$etl_phase <- "staged"
  cleansed_data$etl_staging_timestamp <- Sys.time()
  cleansed_data$etl_records_processed <- nrow(raw_data)
  cleansed_data$etl_records_output <- nrow(cleansed_data)
  
  # Add processing statistics
  cleansed_data$etl_processing_order <- 1:nrow(cleansed_data)
  
  message("STAGE: Added staging metadata")
  message("STAGE: Reviews cleansing completed - ", nrow(cleansed_data), " records processed")
  
  return(cleansed_data)
}