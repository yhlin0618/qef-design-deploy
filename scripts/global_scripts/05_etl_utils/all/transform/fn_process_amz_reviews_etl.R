#' Process Amazon Reviews for ETL Pipeline
#'
#' This function processes staged Amazon review data for the ETL06 transform phase,
#' adding product line information, competitor flags, and category extraction.
#' It supports the ETL06 transform phase of the reviews processing pipeline.
#'
#' @param staged_data A data frame containing staged Amazon reviews data
#' @param product_line_df A data frame containing product line information
#' @param add_product_line_info Logical. Whether to add product line information (default: TRUE)
#' @param add_competitor_flags Logical. Whether to add competitor flags (default: TRUE)
#' @param extract_category_path Logical. Whether to extract category from path (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of processed Amazon reviews in business-ready format
#'
#' @examples
#' \dontrun{
#' # Process Amazon reviews data
#' processed_data <- process_amz_reviews_etl(
#'   staged_data = staged_reviews,
#'   product_line_df = df_product_line,
#'   add_product_line_info = TRUE,
#'   add_competitor_flags = TRUE,
#'   extract_category_path = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
process_amz_reviews_etl <- function(staged_data,
                                   product_line_df = df_product_line,
                                   add_product_line_info = TRUE,
                                   add_competitor_flags = TRUE,
                                   extract_category_path = TRUE,
                                   encoding_target = "UTF-8") {
  
  # Validate input parameters
  if (!is.data.frame(staged_data)) {
    stop("staged_data must be a data frame")
  }
  
  if (nrow(staged_data) == 0) {
    stop("staged_data cannot be empty")
  }
  
  if (!is.data.frame(product_line_df)) {
    stop("product_line_df must be a data frame")
  }
  
  message("TRANSFORM: Starting Amazon reviews processing...")
  message("TRANSFORM: Processing ", nrow(staged_data), " staged reviews")
  
  # Initialize processed data as copy of staged data
  processed_data <- staged_data
  
  # =====================================================================
  # STEP 1: Extract Category Information from Path
  # =====================================================================
  if (extract_category_path) {
    message("TRANSFORM: Extracting category information from path...")
    
    if ("path" %in% names(processed_data)) {
      # Normalize product line data for matching
      product_line_df_norm <- product_line_df %>% 
        dplyr::mutate(cat_name = make.names(product_line_name_english))
      
      # Extract category from path and normalize
      processed_data <- processed_data %>%
        dplyr::mutate(
          # Extract category from path
          raw_cat = stringr::str_extract(path, "(?<=amazon_reviews/)[^/]+"),
          # Clean category name for joining
          cat_name = raw_cat %>% 
            stringr::str_remove("^\\d+_") %>% 
            make.names()
        )
      
      message("TRANSFORM: Category extraction completed")
    } else {
      message("TRANSFORM: No path column found, skipping category extraction")
    }
  }
  
  # =====================================================================
  # STEP 2: Add Product Line Information
  # =====================================================================
  if (add_product_line_info) {
    message("TRANSFORM: Adding product line information...")
    
    if ("cat_name" %in% names(processed_data)) {
      # Normalize product line data for matching
      product_line_df_norm <- product_line_df %>% 
        dplyr::mutate(cat_name = make.names(product_line_name_english))
      
      # Join with product line data
      processed_data <- processed_data %>%
        dplyr::left_join(
          product_line_df_norm %>% 
            dplyr::select(product_line_id, product_line_name_chinese, 
                         product_line_name_english, cat_name),
          by = "cat_name"
        )
      
      # Count records with product line information
      matched_count <- sum(!is.na(processed_data$product_line_id))
      message("TRANSFORM: ", matched_count, " records matched with product line information")
      
      # Add flags for analysis
      processed_data$has_product_line_info <- !is.na(processed_data$product_line_id)
      
    } else {
      message("TRANSFORM: No category information available for product line matching")
      processed_data$has_product_line_info <- FALSE
    }
    
    message("TRANSFORM: Product line information addition completed")
  }
  
  # =====================================================================
  # STEP 3: Add Competitor Flags
  # =====================================================================
  if (add_competitor_flags) {
    message("TRANSFORM: Adding competitor flags...")
    
    # This would typically involve checking against competitor data
    # For now, we'll add placeholder logic
    if ("asin" %in% names(processed_data)) {
      # Placeholder: mark as competitor if ASIN exists in some competitor list
      # In practice, this would join with competitor data
      processed_data$is_competitor <- FALSE  # Placeholder
      processed_data$competitor_brand <- NA_character_  # Placeholder
      processed_data$competitor_category <- NA_character_  # Placeholder
      
      message("TRANSFORM: Competitor flags added (placeholder logic)")
    }
    
    message("TRANSFORM: Competitor flag addition completed")
  }
  
  # =====================================================================
  # STEP 4: Data Quality Enhancements
  # =====================================================================
  message("TRANSFORM: Enhancing data quality...")
  
  # Add review quality metrics
  if ("review_text" %in% names(processed_data)) {
    processed_data$review_text_length <- nchar(processed_data$review_text)
    processed_data$review_quality_score <- ifelse(
      processed_data$review_text_length >= 50, 100,
      ifelse(processed_data$review_text_length >= 20, 75, 50)
    )
  }
  
  # Add rating validation
  if ("rating" %in% names(processed_data)) {
    processed_data$rating_valid <- !is.na(processed_data$rating) & 
                                  processed_data$rating >= 1 & 
                                  processed_data$rating <= 5
  }
  
  # Add temporal analysis fields
  if ("review_date" %in% names(processed_data)) {
    processed_data$review_recency_days <- as.numeric(Sys.Date() - processed_data$review_date)
    processed_data$is_recent_review <- processed_data$review_recency_days <= 365
  }
  
  # Add data lineage information
  processed_data$data_source <- "amazon_reviews"
  processed_data$extraction_method <- "etl_pipeline"
  processed_data$processed_date <- Sys.Date()
  processed_data$processed_timestamp <- Sys.time()
  
  # Add version information
  processed_data$schema_version <- "ETL06_v1.0"
  processed_data$pipeline_version <- "2TR_transform"
  
  message("TRANSFORM: Data quality enhancement completed")
  
  # =====================================================================
  # STEP 5: Calculate Final Quality Metrics
  # =====================================================================
  message("TRANSFORM: Calculating final quality metrics...")
  
  # Calculate overall data completeness
  required_fields <- c("asin", "review_date", "review_text", "rating")
  available_fields <- intersect(required_fields, names(processed_data))
  
  if (length(available_fields) > 0) {
    processed_data$completeness_score <- rowSums(!is.na(processed_data[available_fields])) / length(available_fields)
  } else {
    processed_data$completeness_score <- 0
  }
  
  # Calculate final quality score
  quality_components <- c()
  if ("etl_data_quality_score" %in% names(processed_data)) {
    quality_components <- c(quality_components, processed_data$etl_data_quality_score / 100)
  }
  if ("review_quality_score" %in% names(processed_data)) {
    quality_components <- c(quality_components, processed_data$review_quality_score / 100)
  }
  if ("completeness_score" %in% names(processed_data)) {
    quality_components <- c(quality_components, processed_data$completeness_score)
  }
  
  if (length(quality_components) > 0) {
    processed_data$final_quality_score <- round(rowMeans(matrix(quality_components, ncol = length(quality_components) / nrow(processed_data))) * 100, 2)
  } else {
    processed_data$final_quality_score <- 50  # Default score
  }
  
  message("TRANSFORM: Final quality metrics calculated")
  
  # =====================================================================
  # STEP 6: Add Transform Metadata
  # =====================================================================
  processed_data$etl_phase <- "transformed"
  processed_data$etl_transform_timestamp <- Sys.time()
  processed_data$etl_records_transformed <- nrow(staged_data)
  
  # Add transformation summary
  transform_summary <- list(
    input_records = nrow(staged_data),
    output_records = nrow(processed_data),
    fields_added = ncol(processed_data) - ncol(staged_data),
    avg_quality_score = round(mean(processed_data$final_quality_score, na.rm = TRUE), 2),
    completion_rate = round(mean(processed_data$completeness_score, na.rm = TRUE), 2),
    product_line_matched = sum(processed_data$has_product_line_info, na.rm = TRUE)
  )
  
  # Store summary as attribute
  attr(processed_data, "transform_summary") <- transform_summary
  
  message("TRANSFORM: Added transform metadata")
  message("TRANSFORM: Processing completed - ", nrow(processed_data), " records transformed")
  message("TRANSFORM: Average quality score: ", transform_summary$avg_quality_score, "%")
  message("TRANSFORM: Completion rate: ", transform_summary$completion_rate * 100, "%")
  message("TRANSFORM: Product line matched: ", transform_summary$product_line_matched, " records")
  
  return(processed_data)
}