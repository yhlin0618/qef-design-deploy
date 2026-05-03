#' Transform Competitor products to Business-Ready Format
#'
#' This function transforms staged competitor product data into business-ready format,
#' applying standardization, business rules, and generating lookup keys.
#' It supports the ETL04 transform phase of the competitor analysis pipeline.
#'
#' @param staged_data A data frame containing staged competitor products data
#' @param platform Character string specifying the platform (e.g., "amz", "eby")
#' @param standardize_fields Logical. Whether to standardize field names and values (default: TRUE)
#' @param apply_business_rules Logical. Whether to apply business logic rules (default: TRUE)
#' @param generate_lookup_keys Logical. Whether to generate lookup keys for joins (default: TRUE)
#' @param encoding_target Character string specifying target encoding (default: "UTF-8")
#'
#' @return A data frame of transformed competitor products in business-ready format
#'
#' @examples
#' \dontrun{
#' # Transform competitor products data
#' transformed_data <- transform_competitor_products(
#'   staged_data = staged_competitor_products,
#'   platform = "amz",
#'   standardize_fields = TRUE,
#'   apply_business_rules = TRUE,
#'   generate_lookup_keys = TRUE,
#'   encoding_target = "UTF-8"
#' )
#' }
#'
#' @export
transform_competitor_products <- function(staged_data,
                                      platform = "amz",
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
  
  message("TRANSFORM: Starting competitor products transformation for platform: ", platform)
  message("TRANSFORM: Processing ", nrow(staged_data), " staged records")
  
  # Initialize transformed data as copy of staged data
  transformed_data <- staged_data
  
  # =====================================================================
  # STEP 1: Field Standardization
  # =====================================================================
  if (standardize_fields) {
    message("TRANSFORM: Standardizing field names and values...")
    
    # Standardize column names to business-friendly names
    # ASIN becomes product_id (will remove asin after business rules)
    if ("asin" %in% names(transformed_data)) {
      transformed_data$product_id <- transformed_data$asin
    }
    
    # Ensure product_line_id is preserved and create additional category fields
    if ("product_line_id" %in% names(transformed_data)) {
      # Keep original product_line_id intact
      # Create additional standardized fields
      transformed_data$product_category <- transformed_data$product_line_id
    } else {
      message("TRANSFORM WARNING: product_line_id column not found in staged data")
    }
    
    # Standardize brand names using configuration file
    if ("brand" %in% names(transformed_data)) {
      message("TRANSFORM: Applying brand name standardization...")
      
      # Load brand configuration
      brand_config_path <- file.path(
        .InitEnv$GLOBAL_DIR %||% "scripts/global_scripts",
        "30_global_data", "parameters", "scd_type2", "brand_aliases.yaml"
      )
      
      if (file.exists(brand_config_path)) {
        tryCatch({
          brand_config <- yaml::read_yaml(brand_config_path)
          
          # Clean brand names - basic trimming
          transformed_data$brand <- trimws(transformed_data$brand)
          
          # Handle unknown/missing brand variations
          unknown_variations <- brand_config$unknown_brand_handling$unknown_variations
          default_unknown <- brand_config$unknown_brand_handling$default_unknown_brand
          
          for (unknown_var in unknown_variations) {
            transformed_data$brand[transformed_data$brand == unknown_var] <- default_unknown
          }
          
          # Apply brand standardization mappings
          brand_mappings <- brand_config$brand_standardization
          for (standard_name in names(brand_mappings)) {
            variations <- brand_mappings[[standard_name]]
            for (variation in variations) {
              transformed_data$brand[transformed_data$brand == variation] <- standard_name
            }
          }
          
          # Apply platform-specific mappings if available
          if (platform %in% names(brand_config$platform_specific)) {
            platform_mappings <- brand_config$platform_specific[[platform]]$brand_mappings
            for (standard_name in names(platform_mappings)) {
              variations <- platform_mappings[[standard_name]]
              for (variation in variations) {
                transformed_data$brand[transformed_data$brand == variation] <- standard_name
              }
            }
          }
          
          message("TRANSFORM: Brand standardization completed using configuration")
          
        }, error = function(e) {
          message("TRANSFORM WARNING: Could not load brand configuration, using fallback logic")
          message("TRANSFORM WARNING: Error - ", e$message)
          
          # Fallback to simple brand cleaning
          transformed_data$brand[transformed_data$brand == ""] <<- "Unknown Brand"
          transformed_data$brand[transformed_data$brand %in% c("UNKNOWN", "N/A", "NA")] <<- "Unknown Brand"
        })
      } else {
        message("TRANSFORM WARNING: Brand configuration file not found, using simple cleaning")
        
        # Fallback to simple brand cleaning
        transformed_data$brand[transformed_data$brand == ""] <- "Unknown Brand"
        transformed_data$brand[transformed_data$brand %in% c("UNKNOWN", "N/A", "NA")] <- "Unknown Brand"
      }
    }
    
    # Standardize product line names
    if ("product_line_id" %in% names(transformed_data)) {
      product_line_mappings <- c(
        "jew" = "jewelry_organizer",
        "sop" = "soap_dispenser"
      )
      
      transformed_data$product_category_name <- product_line_mappings[transformed_data$product_line_id]
      transformed_data$product_category_name[is.na(transformed_data$product_category_name)] <- transformed_data$product_line_id[is.na(transformed_data$product_category_name)]
    }
    
    message("TRANSFORM: Field standardization completed")
  }
  
  # =====================================================================
  # STEP 2: Business Rules Application
  # =====================================================================
  if (apply_business_rules) {
    message("TRANSFORM: Applying business rules...")
    
    # Rule 1: Assign competitor ranking based on data quality and completeness
    if (exists("etl_data_quality_score", transformed_data)) {
      transformed_data$competitor_rank <- ifelse(
        transformed_data$etl_data_quality_score >= 90, "A",
        ifelse(transformed_data$etl_data_quality_score >= 70, "B", "C")
      )
    } else {
      transformed_data$competitor_rank <- "B"  # Default rank
    }
    
    # Rule 2: Create competitive analysis flags
    transformed_data$is_direct_competitor <- !is.na(transformed_data$brand) & transformed_data$brand != "Unknown Brand"
    
    # Safe handling of etl_duplicate_flag column
    if ("etl_duplicate_flag" %in% names(transformed_data)) {
      transformed_data$requires_analysis <- !(transformed_data$etl_duplicate_flag %||% FALSE)
    } else {
      transformed_data$requires_analysis <- TRUE  # Default to requiring analysis if flag not present
    }
    
    # Rule 3: Platform-specific business rules
    if (platform == "amz") {
      # Amazon-specific rules
      transformed_data$marketplace <- "Amazon"
      transformed_data$product_url_base <- "https://www.amazon.com/dp/"
      
      # ASIN validation
      if ("asin" %in% names(transformed_data)) {
        transformed_data$asin_valid <- nchar(transformed_data$asin) == 10 & 
                                      !grepl("[^A-Z0-9]", transformed_data$asin)
      }
      
    } else if (platform == "eby") {
      # eBay-specific rules
      transformed_data$marketplace <- "eBay"
      transformed_data$product_url_base <- "https://www.ebay.com/itm/"
      
      # eBay product number validation
      if ("ebay_product_number" %in% names(transformed_data)) {
        transformed_data$product_number_valid <- nchar(transformed_data$ebay_product_number) >= 10
      }
    }
    
    # Rule 4: Data completeness scoring
    required_fields <- c("product_line_id", "brand", "product_id")
    if (all(required_fields %in% names(transformed_data))) {
      transformed_data$completeness_score <- rowSums(!is.na(transformed_data[required_fields])) / length(required_fields)
    } else {
      transformed_data$completeness_score <- 0.5  # Default score
    }
    
    message("TRANSFORM: Business rules application completed")
  }
  
  # =====================================================================
  # STEP 3: Lookup Key Generation
  # =====================================================================
  if (generate_lookup_keys) {
    message("TRANSFORM: Generating lookup keys...")
    
    # Generate primary lookup key (product_line_id + product_id)
    # Ensure we have both required fields
    if (!"product_line_id" %in% names(transformed_data)) {
      message("TRANSFORM WARNING: product_line_id missing for lookup key generation")
      transformed_data$product_line_id <- "unknown"
    }
    if (!"product_id" %in% names(transformed_data)) {
      message("TRANSFORM WARNING: product_id missing for lookup key generation")
      transformed_data$product_id <- "unknown"
    }
    
    product_line_vals <- ifelse(is.na(transformed_data$product_line_id), "unknown", transformed_data$product_line_id)
    product_id_vals <- ifelse(is.na(transformed_data$product_id), "unknown", transformed_data$product_id)
    transformed_data$lookup_key <- paste(product_line_vals, product_id_vals, sep = "_")
    
    # Generate secondary lookup keys for joins
    brand_vals <- ifelse(is.na(transformed_data$brand), "unknown", transformed_data$brand)
    brand_clean <- gsub("[^A-Za-z0-9]", "", brand_vals)
    transformed_data$brand_lookup_key <- paste(product_line_vals, brand_clean, sep = "_")
    
    # Generate analysis group key
    rank_vals <- ifelse(is.na(transformed_data$competitor_rank), "B", transformed_data$competitor_rank)
    transformed_data$analysis_group_key <- paste(product_line_vals, rank_vals, sep = "_")
    
    # Generate temporal key for tracking
    transformed_data$temporal_key <- paste(
      format(Sys.Date(), "%Y%m"),
      transformed_data$lookup_key,
      sep = "_"
    )
    
    message("TRANSFORM: Lookup key generation completed")
  }
  
  # =====================================================================
  # STEP 4: Data Quality Enhancements
  # =====================================================================
  message("TRANSFORM: Enhancing data quality...")
  
  # Add data lineage information
  transformed_data$data_source <- "google_sheets"
  transformed_data$source_sheet <- "competitor_products"
  transformed_data$extraction_method <- "etl_pipeline"
  
  # Add processing timestamps
  transformed_data$processed_date <- Sys.Date()
  transformed_data$processed_timestamp <- Sys.time()
  
  # Add version information
  transformed_data$schema_version <- "ETL04_v1.0"
  transformed_data$pipeline_version <- "2TR_transform"
  
  # Calculate final data quality metrics
  total_fields <- ncol(transformed_data)
  non_na_fields <- rowSums(!is.na(transformed_data))
  transformed_data$final_quality_score <- round((non_na_fields / total_fields) * 100, 2)
  
  message("TRANSFORM: Data quality enhancement completed")
  
  # =====================================================================
  # STEP 5: Final Data Cleanup
  # =====================================================================
  message("TRANSFORM: Performing final data cleanup...")
  
  # Remove redundant columns after transformation
  # Remove original asin column since we now have standardized product_id
  if ("asin" %in% names(transformed_data) && "product_id" %in% names(transformed_data)) {
    transformed_data$asin <- NULL
    message("TRANSFORM: Removed redundant 'asin' column (standardized as 'product_id')")
  }
  
  message("TRANSFORM: Final data cleanup completed")
  
  # =====================================================================
  # STEP 6: Add Transform Metadata
  # =====================================================================
  transformed_data$etl_phase <- "transformed"
  transformed_data$etl_transform_timestamp <- Sys.time()
  transformed_data$etl_platform <- platform
  transformed_data$etl_records_transformed <- nrow(staged_data)
  
  # Add transformation summary
  transform_summary <- list(
    input_records = nrow(staged_data),
    output_records = nrow(transformed_data),
    fields_added = ncol(transformed_data) - ncol(staged_data),
    avg_quality_score = round(mean(transformed_data$final_quality_score, na.rm = TRUE), 2),
    completion_rate = round(mean(transformed_data$completeness_score, na.rm = TRUE), 2)
  )
  
  # Store summary as attribute
  attr(transformed_data, "transform_summary") <- transform_summary
  
  message("TRANSFORM: Added transform metadata")
  message("TRANSFORM: Transformation completed - ", nrow(transformed_data), " records transformed")
  message("TRANSFORM: Average quality score: ", transform_summary$avg_quality_score, "%")
  message("TRANSFORM: Completion rate: ", transform_summary$completion_rate * 100, "%")
  
  return(transformed_data)
}