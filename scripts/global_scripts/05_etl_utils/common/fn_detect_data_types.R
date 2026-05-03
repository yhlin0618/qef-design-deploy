#' Data Type Detection and Analysis for ETL Operations
#'
#' This module provides intelligent data type detection capabilities for
#' database schema optimization. It analyzes column names and content to
#' determine optimal data types for various database systems.
#'
#' @author WISER Team
#' @date 2025-07-13

# ==============================================================================
# STANDARD DATA TYPE PATTERNS
# ==============================================================================

#' Standard Data Type Patterns for Intelligent Type Detection
#'
#' Define patterns for common data types across different domains.
#' This can be customized for specific business needs.
#'
#' @export
get_standard_type_patterns <- function() {
  list(
    # Identifiers and Keys
    id_patterns = list(
      patterns = c("_id$", "^id$", "^asin$", "^sku$", "^product_id$", 
                  "^product_id$", "^customer_id$", "^order_id$", "^user_id$"),
      duckdb_type = "VARCHAR",
      postgresql_type = "VARCHAR",
      mysql_type = "VARCHAR(255)",
      description = "Identifiers, keys, SKUs, ASINs"
    ),
    
    # Numeric Types - Integer
    integer_patterns = list(
      patterns = c("count$", "quantity$", "^qty$", "number$", "rank$", 
                  "position$", "_num$", "^num_", "total$", "size$"),
      duckdb_type = "INTEGER",
      postgresql_type = "INTEGER",
      mysql_type = "INT",
      description = "Counts, quantities, rankings, positions"
    ),
    
    # Numeric Types - Decimal
    decimal_patterns = list(
      patterns = c("price$", "cost$", "amount$", "rate$", "score$", 
                  "rating$", "percentage$", "percent$", "_pct$", "ratio$"),
      duckdb_type = "DECIMAL(10,2)",
      postgresql_type = "NUMERIC(10,2)",
      mysql_type = "DECIMAL(10,2)",
      description = "Prices, costs, ratings, percentages"
    ),
    
    # Date and Time - Full Timestamp
    datetime_patterns = list(
      patterns = c("_datetime$", "_timestamp$", "_at$", "created_at$", 
                  "updated_at$", "modified_at$", "deleted_at$", "logged_at$"),
      duckdb_type = "TIMESTAMP",
      postgresql_type = "TIMESTAMP WITH TIME ZONE",
      mysql_type = "DATETIME",
      description = "Full timestamps with date and time"
    ),
    
    # Date and Time - Date Only
    date_patterns = list(
      patterns = c("_date$", "^date$", "birth_date$", "start_date$", 
                  "end_date$", "due_date$", "expired_date$"),
      duckdb_type = "DATE",
      postgresql_type = "DATE",
      mysql_type = "DATE",
      description = "Date-only fields"
    ),
    
    # Boolean Types
    boolean_patterns = list(
      patterns = c("^is_", "^has_", "^can_", "^should_", "^was_", "^will_",
                  "enabled$", "active$", "valid$", "deleted$", "completed$"),
      duckdb_type = "BOOLEAN",
      postgresql_type = "BOOLEAN",
      mysql_type = "TINYINT(1)",
      description = "Boolean flags and status indicators"
    ),
    
    # Text Types - Short
    text_short_patterns = list(
      patterns = c("^name$", "title$", "code$", "status$", "type$", 
                  "category$", "brand$", "tag$", "label$", "keyword$"),
      duckdb_type = "VARCHAR(255)",
      postgresql_type = "VARCHAR(255)",
      mysql_type = "VARCHAR(255)",
      description = "Short text fields"
    ),
    
    # Text Types - Medium
    text_medium_patterns = list(
      patterns = c("address$", "email$", "phone$", "subject$", "headline$"),
      duckdb_type = "VARCHAR(500)",
      postgresql_type = "VARCHAR(500)",
      mysql_type = "VARCHAR(500)",
      description = "Medium-length text fields"
    ),
    
    # Text Types - Long
    text_long_patterns = list(
      patterns = c("description$", "content$", "comment$", "note$", 
                  "review$", "summary$", "body$", "text$", "message$"),
      duckdb_type = "TEXT",
      postgresql_type = "TEXT",
      mysql_type = "TEXT",
      description = "Long text fields"
    ),
    
    # JSON/JSONB Types
    json_patterns = list(
      patterns = c("_json$", "_data$", "metadata$", "attributes$", 
                  "properties$", "settings$", "config$"),
      duckdb_type = "JSON",
      postgresql_type = "JSONB",
      mysql_type = "JSON",
      description = "JSON structured data"
    ),
    
    # URLs and Paths
    url_patterns = list(
      patterns = c("_url$", "_link$", "^url$", "^link$", "image_", 
                  "photo_", "_src$", "_href$", "website$"),
      duckdb_type = "VARCHAR(1000)",
      postgresql_type = "VARCHAR(1000)",
      mysql_type = "VARCHAR(1000)",
      description = "URLs, links, and paths"
    ),
    
    # UUID Types
    uuid_patterns = list(
      patterns = c("_uuid$", "^uuid$", "_guid$", "^guid$"),
      duckdb_type = "VARCHAR(36)",
      postgresql_type = "UUID",
      mysql_type = "CHAR(36)",
      description = "UUID/GUID identifiers"
    ),
    
    # Default fallback
    default_type = list(
      duckdb_type = "VARCHAR",
      postgresql_type = "VARCHAR",
      mysql_type = "VARCHAR(255)",
      description = "Default text type"
    )
  )
}

#' Detect Data Type Based on Column Name and Content
#'
#' Analyzes column names using pattern matching and optionally analyzes
#' sample data to determine the most appropriate data type.
#'
#' @param column_name Character string of the column name
#' @param sample_data Optional vector of sample values from the column
#' @param max_samples Maximum number of samples to analyze (default: 100)
#' @param target_db Target database system: "duckdb", "postgresql", "mysql" (default: "duckdb")
#' @param patterns Custom patterns list (default: uses standard patterns)
#' @param analyze_content Whether to analyze content if no pattern match (default: TRUE)
#'
#' @return List containing:
#'   - type: Recommended data type for target database
#'   - reason: Explanation of why this type was chosen
#'   - description: Human-readable description of the type
#'   - confidence: Confidence level of the detection (0-1)
#'
#' @examples
#' \dontrun{
#' # Simple column name detection
#' detect_data_type("product_id")
#' # Returns: list(type = "VARCHAR", reason = "Pattern match: _id$", ...)
#' 
#' # With content analysis
#' detect_data_type("total", sample_data = c(10, 20, 30, 40))
#' # Returns: list(type = "INTEGER", reason = "Content analysis: All numeric values are integers", ...)
#' }
#'
#' @export
detect_data_type <- function(column_name, 
                           sample_data = NULL, 
                           max_samples = 100,
                           target_db = "duckdb",
                           patterns = NULL,
                           analyze_content = TRUE) {
  
  # Use standard patterns if not provided
  if (is.null(patterns)) {
    patterns <- get_standard_type_patterns()
  }
  
  # Validate target database
  valid_dbs <- c("duckdb", "postgresql", "mysql")
  if (!target_db %in% valid_dbs) {
    stop("target_db must be one of: ", paste(valid_dbs, collapse = ", "))
  }
  
  # Convert column name to lowercase for pattern matching
  col_lower <- tolower(column_name)
  
  # First try pattern matching on column name
  for (pattern_group in patterns) {
    if (!is.null(pattern_group$patterns)) {
      for (pattern in pattern_group$patterns) {
        if (grepl(pattern, col_lower, perl = TRUE)) {
          type_field <- paste0(target_db, "_type")
          return(list(
            type = pattern_group[[type_field]],
            reason = paste("Pattern match:", pattern),
            description = pattern_group$description,
            confidence = 0.9  # High confidence for pattern matches
          ))
        }
      }
    }
  }
  
  # If no pattern match and content analysis is enabled
  if (analyze_content && !is.null(sample_data) && length(sample_data) > 0) {
    content_type <- analyze_content_type(sample_data, max_samples, target_db)
    if (!is.null(content_type)) {
      return(content_type)
    }
  }
  
  # Default fallback
  type_field <- paste0(target_db, "_type")
  return(list(
    type = patterns$default_type[[type_field]],
    reason = "No pattern match, using default",
    description = patterns$default_type$description,
    confidence = 0.3  # Low confidence for defaults
  ))
}

#' Analyze Content to Determine Data Type
#'
#' Analyzes actual data content to infer the most appropriate data type
#' when pattern matching fails.
#'
#' @param sample_data Vector of sample values
#' @param max_samples Maximum samples to analyze
#' @param target_db Target database system
#' @return List with type information or NULL
#'
#' @keywords internal
analyze_content_type <- function(sample_data, max_samples = 100, target_db = "duckdb") {
  
  # Take a sample for analysis
  sample_size <- min(length(sample_data), max_samples)
  sample_subset <- sample_data[seq_len(sample_size)]
  
  # Remove NA and empty values
  sample_subset <- sample_subset[!is.na(sample_subset) & sample_subset != ""]
  
  if (length(sample_subset) == 0) {
    return(NULL)
  }
  
  # Check for boolean patterns
  boolean_values <- c("true", "false", "1", "0", "yes", "no", "y", "n", 
                     "t", "f", "TRUE", "FALSE", "Yes", "No", "Y", "N")
  boolean_count <- sum(as.character(sample_subset) %in% boolean_values)
  
  if (boolean_count / length(sample_subset) > 0.8) {
    return(list(
      type = switch(target_db,
                   duckdb = "BOOLEAN",
                   postgresql = "BOOLEAN",
                   mysql = "TINYINT(1)"),
      reason = "Content analysis: Boolean-like values detected",
      description = "Boolean values detected from content",
      confidence = 0.8
    ))
  }
  
  # Check for numeric patterns
  numeric_test <- suppressWarnings(as.numeric(as.character(sample_subset)))
  numeric_valid <- !is.na(numeric_test)
  
  if (sum(numeric_valid) / length(sample_subset) > 0.8) {
    # Mostly numeric
    numeric_values <- numeric_test[numeric_valid]
    
    # Check if all integers
    if (all(numeric_values == floor(numeric_values))) {
      # Determine integer size
      max_val <- max(abs(numeric_values))
      
      if (max_val <= 32767) {
        int_type <- switch(target_db,
                          duckdb = "SMALLINT",
                          postgresql = "SMALLINT",
                          mysql = "SMALLINT")
      } else if (max_val <= 2147483647) {
        int_type <- switch(target_db,
                          duckdb = "INTEGER",
                          postgresql = "INTEGER",
                          mysql = "INT")
      } else {
        int_type <- switch(target_db,
                          duckdb = "BIGINT",
                          postgresql = "BIGINT",
                          mysql = "BIGINT")
      }
      
      return(list(
        type = int_type,
        reason = paste("Content analysis: Integer values, max:", max_val),
        description = "Integer values detected from content",
        confidence = 0.8
      ))
    } else {
      # Has decimals
      max_decimals <- max(nchar(gsub("^[^.]*\\.?", "", as.character(numeric_values))))
      precision <- max(nchar(gsub("\\.", "", as.character(numeric_values))))
      
      decimal_type <- switch(target_db,
                           duckdb = paste0("DECIMAL(", min(precision, 18), ",", min(max_decimals, 4), ")"),
                           postgresql = paste0("NUMERIC(", min(precision, 18), ",", min(max_decimals, 4), ")"),
                           mysql = paste0("DECIMAL(", min(precision, 18), ",", min(max_decimals, 4), ")"))
      
      return(list(
        type = decimal_type,
        reason = paste("Content analysis: Decimal values with up to", max_decimals, "decimal places"),
        description = "Decimal values detected from content",
        confidence = 0.8
      ))
    }
  }
  
  # Check for date patterns
  date_formats <- c("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", 
                   "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S")
  
  for (fmt in date_formats) {
    parsed_dates <- suppressWarnings(as.POSIXct(as.character(sample_subset), format = fmt))
    if (sum(!is.na(parsed_dates)) / length(sample_subset) > 0.8) {
      # Mostly dates
      if (grepl("%H:%M:%S", fmt)) {
        # Has time component
        return(list(
          type = switch(target_db,
                       duckdb = "TIMESTAMP",
                       postgresql = "TIMESTAMP WITH TIME ZONE",
                       mysql = "DATETIME"),
          reason = paste("Content analysis: Datetime values detected, format:", fmt),
          description = "Datetime values detected from content",
          confidence = 0.7
        ))
      } else {
        # Date only
        return(list(
          type = switch(target_db,
                       duckdb = "DATE",
                       postgresql = "DATE",
                       mysql = "DATE"),
          reason = paste("Content analysis: Date values detected, format:", fmt),
          description = "Date values detected from content",
          confidence = 0.7
        ))
      }
    }
  }
  
  # Text analysis based on length
  char_data <- as.character(sample_subset)
  avg_length <- mean(nchar(char_data))
  max_length <- max(nchar(char_data))
  
  # Determine text type based on length
  if (max_length <= 50) {
    text_type <- switch(target_db,
                       duckdb = "VARCHAR(100)",
                       postgresql = "VARCHAR(100)",
                       mysql = "VARCHAR(100)")
    desc <- "Short text"
  } else if (max_length <= 255) {
    text_type <- switch(target_db,
                       duckdb = "VARCHAR(255)",
                       postgresql = "VARCHAR(255)",
                       mysql = "VARCHAR(255)")
    desc <- "Medium text"
  } else if (max_length <= 1000) {
    text_type <- switch(target_db,
                       duckdb = "VARCHAR(1000)",
                       postgresql = "VARCHAR(1000)",
                       mysql = "VARCHAR(1000)")
    desc <- "Long text"
  } else {
    text_type <- switch(target_db,
                       duckdb = "TEXT",
                       postgresql = "TEXT",
                       mysql = "TEXT")
    desc = "Very long text"
  }
  
  return(list(
    type = text_type,
    reason = paste("Content analysis: Text with max length", max_length, "chars"),
    description = paste(desc, "based on content analysis"),
    confidence = 0.6
  ))
}

#' Detect Data Types for Entire Data Frame
#'
#' Analyzes all columns in a data frame and returns optimal data types
#' for each column.
#'
#' @param df Data frame to analyze
#' @param target_db Target database system (default: "duckdb")
#' @param sample_size Number of rows to sample for analysis (default: 1000)
#' @param patterns Custom patterns (default: uses standard patterns)
#'
#' @return Data frame with columns: column_name, data_type, reason, confidence
#'
#' @examples
#' \dontrun{
#' # Analyze entire data frame
#' type_info <- detect_dataframe_types(my_data, target_db = "postgresql")
#' print(type_info)
#' }
#'
#' @export
detect_dataframe_types <- function(df, 
                                 target_db = "duckdb", 
                                 sample_size = 1000,
                                 patterns = NULL) {
  
  # Sample data if needed
  if (nrow(df) > sample_size) {
    sample_rows <- sample(nrow(df), sample_size)
    sample_df <- df[sample_rows, , drop = FALSE]
  } else {
    sample_df <- df
  }
  
  # Analyze each column
  results <- data.frame(
    column_name = character(),
    data_type = character(),
    reason = character(),
    confidence = numeric(),
    stringsAsFactors = FALSE
  )
  
  for (col_name in names(df)) {
    type_info <- detect_data_type(
      column_name = col_name,
      sample_data = sample_df[[col_name]],
      target_db = target_db,
      patterns = patterns
    )
    
    results <- rbind(results, data.frame(
      column_name = col_name,
      data_type = type_info$type,
      reason = type_info$reason,
      confidence = type_info$confidence,
      stringsAsFactors = FALSE
    ))
  }
  
  return(results)
}