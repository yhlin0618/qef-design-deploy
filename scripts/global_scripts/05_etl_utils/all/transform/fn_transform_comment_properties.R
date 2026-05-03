#' Transform Comment Properties to Business-Ready Format
#'
#' This function transforms staged comment properties data into business-ready format,
#' applying standardization, business rules, and generating lookup keys.
#' It supports the ETL05 transform phase of the comment properties pipeline.
#'
#' @param staged_data A data frame containing staged comment properties data
#' @param standardize_fields Logical. Whether to standardize field names and values (default: TRUE)
#' @param apply_business_rules Logical. Whether to apply business logic rules (default: TRUE)
#' @param generate_lookup_keys Logical. Whether to generate lookup keys for joins (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of transformed comment properties in business-ready format
#'
#' @examples
#' \dontrun{
#' # Transform comment properties data
#' transformed_data <- transform_comment_properties(
#'   staged_data = staged_comment_properties,
#'   standardize_fields = TRUE,
#'   apply_business_rules = TRUE,
#'   generate_lookup_keys = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
transform_comment_properties <- function(staged_data,
                                       standardize_fields = TRUE,
                                       apply_business_rules = TRUE,
                                       generate_lookup_keys = TRUE,
                                       encoding_target = "UTF-8") {
  
  # Validate input parameters
  if (!is.data.frame(staged_data)) {
    stop("staged_data must be a data frame")
  }
  
  if (nrow(staged_data) == 0) {
    stop("staged_data cannot be empty")
  }
  
  message("TRANSFORM: Starting comment properties transformation...")
  message("TRANSFORM: Processing ", nrow(staged_data), " staged records")
  
  # Initialize transformed data as copy of staged data
  transformed_data <- staged_data
  
  # =====================================================================
  # STEP 1: Field Standardization
  # =====================================================================
  if (standardize_fields) {
    message("TRANSFORM: Standardizing field names and values...")
    
    # Standardize property names - clean whitespace and formatting
    if ("property_name" %in% names(transformed_data)) {
      transformed_data$property_name <- trimws(transformed_data$property_name)
      transformed_data$property_name_clean <- gsub("[^\\p{L}\\p{N}\\s]", "", transformed_data$property_name, perl = TRUE)
    }
    
    if ("property_name_english" %in% names(transformed_data)) {
      transformed_data$property_name_english <- trimws(transformed_data$property_name_english)
      transformed_data$property_name_english_clean <- gsub("[^a-zA-Z0-9\\s]", "", transformed_data$property_name_english)
    }
    
    # Standardize definition field
    if ("definition" %in% names(transformed_data)) {
      transformed_data$definition <- trimws(transformed_data$definition)
      # Mark empty definitions
      transformed_data$has_definition <- !is.na(transformed_data$definition) & transformed_data$definition != ""
    }
    
    # Standardize review examples
    review_cols <- c("review_1", "review_2", "review_3", "translation_1", "translation_2", "translation_3")
    for (col in review_cols) {
      if (col %in% names(transformed_data)) {
        transformed_data[[col]] <- trimws(transformed_data[[col]])
      }
    }
    
    # Count available review examples
    transformed_data$review_examples_count <- rowSums(!is.na(transformed_data[c("review_1", "review_2", "review_3")]) & 
                                                     transformed_data[c("review_1", "review_2", "review_3")] != "")
    
    # Standardize property type
    if ("type" %in% names(transformed_data)) {
      transformed_data$type <- trimws(transformed_data$type)
      # Standardize common type variations
      type_mappings <- c(
        "正面" = "positive",
        "負面" = "negative", 
        "中性" = "neutral",
        "功能" = "functional",
        "情感" = "emotional",
        "外觀" = "appearance"
      )
      
      transformed_data$type_standardized <- type_mappings[transformed_data$type]
      transformed_data$type_standardized[is.na(transformed_data$type_standardized)] <- transformed_data$type[is.na(transformed_data$type_standardized)]
    }
    
    message("TRANSFORM: Field standardization completed")
  }
  
  # =====================================================================
  # STEP 2: Business Rules Application
  # =====================================================================
  if (apply_business_rules) {
    message("TRANSFORM: Applying business rules...")
    
    # Rule 1: Calculate property completeness score
    required_fields <- c("property_name", "property_name_english", "definition")
    if (all(required_fields %in% names(transformed_data))) {
      transformed_data$completeness_score <- rowSums(!is.na(transformed_data[required_fields]) & 
                                                    transformed_data[required_fields] != "") / length(required_fields)
    } else {
      transformed_data$completeness_score <- 0.5  # Default score
    }
    
    # Rule 2: Assign property quality rank based on completeness and examples
    transformed_data$property_quality_rank <- ifelse(
      transformed_data$completeness_score >= 0.8 & transformed_data$review_examples_count >= 2, "A",
      ifelse(transformed_data$completeness_score >= 0.6 & transformed_data$review_examples_count >= 1, "B", "C")
    )
    
    # Rule 3: Create analysis flags
    transformed_data$is_analysis_ready <- transformed_data$completeness_score >= 0.6 & 
                                         !is.na(transformed_data$property_name_english) & 
                                         transformed_data$property_name_english != ""
    
    transformed_data$requires_translation <- is.na(transformed_data$property_name_english) | 
                                           transformed_data$property_name_english == ""
    
    # Rule 4: Calculate frequency percentile within product line
    if (all(c("product_line_id", "frequency") %in% names(transformed_data))) {
      transformed_data <- transformed_data %>%
        dplyr::group_by(product_line_id) %>%
        dplyr::mutate(
          frequency_percentile = dplyr::percent_rank(frequency),
          is_high_frequency = frequency_percentile >= 0.8
        ) %>%
        dplyr::ungroup()
    }
    
    # Rule 5: Data quality enhancements
    transformed_data$has_sufficient_examples <- transformed_data$review_examples_count >= 2
    transformed_data$has_translations <- rowSums(!is.na(transformed_data[c("translation_1", "translation_2", "translation_3")]) & 
                                                 transformed_data[c("translation_1", "translation_2", "translation_3")] != "") > 0
    
    message("TRANSFORM: Business rules application completed")
  }
  
  # =====================================================================
  # STEP 3: Lookup Key Generation
  # =====================================================================
  if (generate_lookup_keys) {
    message("TRANSFORM: Generating lookup keys...")
    
    # Generate primary lookup key
    product_line_vals <- ifelse(is.na(transformed_data$product_line_id), "unknown", transformed_data$product_line_id)
    property_id_vals <- ifelse(is.na(transformed_data$property_id), "unknown", as.character(transformed_data$property_id))
    transformed_data$property_lookup_key <- paste(product_line_vals, property_id_vals, sep = "_")
    
    # Generate product-property combination key
    transformed_data$product_property_key <- paste(
      product_line_vals,
      gsub("[^a-zA-Z0-9]", "", transformed_data$property_name_english %||% "unknown"),
      sep = "_"
    )
    
    # Generate analysis group key based on quality
    quality_vals <- ifelse(is.na(transformed_data$property_quality_rank), "B", transformed_data$property_quality_rank)
    transformed_data$analysis_group_key <- paste(product_line_vals, quality_vals, sep = "_")
    
    # Generate temporal key for tracking
    transformed_data$temporal_key <- paste(
      format(Sys.Date(), "%Y%m"),
      transformed_data$property_lookup_key,
      sep = "_"
    )
    
    # Generate semantic key for property matching
    if ("property_name_english_clean" %in% names(transformed_data)) {
      transformed_data$semantic_key <- paste(
        product_line_vals,
        tolower(gsub("\\s+", "_", transformed_data$property_name_english_clean)),
        sep = "_"
      )
    }
    
    message("TRANSFORM: Lookup key generation completed")
  }
  
  # =====================================================================
  # STEP 4: Data Quality Enhancements
  # =====================================================================
  message("TRANSFORM: Enhancing data quality...")
  
  # Add data lineage information
  transformed_data$data_source <- "google_sheets"
  transformed_data$source_sheet <- "comment_properties"
  transformed_data$extraction_method <- "etl_pipeline"
  
  # Add processing timestamps
  transformed_data$processed_date <- Sys.Date()
  transformed_data$processed_timestamp <- Sys.time()
  
  # Add version information
  transformed_data$schema_version <- "ETL05_v1.0"
  transformed_data$pipeline_version <- "2TR_transform"
  
  # Calculate final data quality metrics
  total_fields <- ncol(transformed_data)
  non_na_fields <- rowSums(!is.na(transformed_data))
  transformed_data$final_quality_score <- round((non_na_fields / total_fields) * 100, 2)
  
  message("TRANSFORM: Data quality enhancement completed")
  
  # =====================================================================
  # STEP 5: Add Transform Metadata
  # =====================================================================
  transformed_data$etl_phase <- "transformed"
  transformed_data$etl_transform_timestamp <- Sys.time()
  transformed_data$etl_records_transformed <- nrow(staged_data)
  
  # Add transformation summary
  transform_summary <- list(
    input_records = nrow(staged_data),
    output_records = nrow(transformed_data),
    fields_added = ncol(transformed_data) - ncol(staged_data),
    avg_quality_score = round(mean(transformed_data$final_quality_score, na.rm = TRUE), 2),
    completion_rate = round(mean(transformed_data$completeness_score, na.rm = TRUE), 2),
    analysis_ready_count = sum(transformed_data$is_analysis_ready, na.rm = TRUE)
  )
  
  # Store summary as attribute
  attr(transformed_data, "transform_summary") <- transform_summary
  
  message("TRANSFORM: Added transform metadata")
  message("TRANSFORM: Transformation completed - ", nrow(transformed_data), " records transformed")
  message("TRANSFORM: Average quality score: ", transform_summary$avg_quality_score, "%")
  message("TRANSFORM: Completion rate: ", transform_summary$completion_rate * 100, "%")
  message("TRANSFORM: Analysis ready: ", transform_summary$analysis_ready_count, " properties")
  
  return(transformed_data)
}