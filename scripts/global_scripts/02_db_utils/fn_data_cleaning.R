# ============================================================================
# Database-Agnostic Data Cleaning Utilities
# ============================================================================
# Purpose: Generic data cleaning and type handling functions for any database
# Extracted from DuckDB-specific functions but work with any DBI connection
# Created: 2025-08-28
# ============================================================================

library(dplyr)
library(purrr)
library(lubridate)

#' Process numeric data with type safety
#' 
#' @param df Data frame to process
#' @return Data frame with cleaned numeric types
#' @export
process_numeric_data <- function(df) {
  df %>%
    mutate(
      # Ensure integer types
      across(
        where(~is.numeric(.) && all(. == as.integer(.), na.rm = TRUE)),
        as.integer
      ),
      
      # Handle potential overflow
      across(
        where(is.numeric),
        ~case_when(
          . > .Machine$integer.max ~ as.numeric(.),
          TRUE ~ .
        )
      ),
      
      # Round decimal columns to 2 places
      across(
        where(~is.numeric(.) && any(. %% 1 != 0, na.rm = TRUE)),
        ~round(., 2)
      )
    )
}

#' Clean and normalize string data
#' 
#' @param df Data frame to process
#' @return Data frame with cleaned strings
#' @export
clean_string_data <- function(df) {
  df %>%
    mutate(across(
      where(is.character),
      ~{
        # Trim whitespace
        x <- trimws(.)
        
        # Handle encoding
        x <- iconv(x, from = "UTF-8", to = "UTF-8", sub = "")
        
        # Handle NULL vs empty string
        x <- case_when(
          is.na(x) | x == "" ~ NA_character_,
          TRUE ~ x
        )
        
        x
      }
    ))
}

#' Process temporal data with timezone handling
#' 
#' @param df Data frame to process
#' @param date_cols Optional vector of column names to treat as dates
#' @param datetime_cols Optional vector of column names to treat as datetimes
#' @param tz Timezone for datetime columns (default: "UTC")
#' @return Data frame with cleaned date/time types
#' @export
process_temporal_data <- function(df, date_cols = NULL, datetime_cols = NULL, tz = "UTC") {
  # Auto-detect date columns if not specified
  if (is.null(date_cols)) {
    date_cols <- names(df)[sapply(df, function(x) {
      inherits(x, "Date") || 
      (is.character(x) && any(grepl("^\\d{4}-\\d{2}-\\d{2}$", head(x, 100), na.rm = TRUE)))
    })]
  }
  
  # Auto-detect datetime columns if not specified
  if (is.null(datetime_cols)) {
    datetime_cols <- names(df)[sapply(df, function(x) {
      inherits(x, c("POSIXct", "POSIXlt")) ||
      (is.character(x) && any(grepl("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", head(x, 100), na.rm = TRUE)))
    })]
  }
  
  # Process date columns
  if (length(date_cols) > 0) {
    df <- df %>%
      mutate(across(
        all_of(date_cols),
        ~as.Date(., tryFormats = c("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"))
      ))
  }
  
  # Process datetime columns
  if (length(datetime_cols) > 0) {
    df <- df %>%
      mutate(across(
        all_of(datetime_cols),
        ~as.POSIXct(., tz = tz)
      ))
  }
  
  return(df)
}

#' Safe type conversion with error handling
#' 
#' @param x Value to convert
#' @param target_type Target type name
#' @return Converted value or NA on error
#' @export
safe_convert <- function(x, target_type) {
  result <- tryCatch({
    switch(target_type,
      "integer" = as.integer(x),
      "numeric" = as.numeric(x),
      "character" = as.character(x),
      "logical" = as.logical(x),
      "date" = as.Date(x),
      "datetime" = as.POSIXct(x),
      stop("Unknown target type: ", target_type)
    )
  }, error = function(e) {
    warning(paste("Conversion error:", e$message))
    return(NA)
  }, warning = function(w) {
    warning(paste("Conversion warning:", w$message))
    return(NA)
  })
  
  return(result)
}

#' Apply safe type conversion to dataframe
#' 
#' @param df Data frame to convert
#' @param type_map Named list of column types
#' @return Data frame with converted types
#' @export
safe_type_conversion <- function(df, type_map) {
  for (col in names(type_map)) {
    if (col %in% names(df)) {
      df[[col]] <- safe_convert(df[[col]], type_map[[col]])
    } else {
      warning(paste("Column", col, "not found in dataframe"))
    }
  }
  return(df)
}

#' Validate data types before database insertion
#' 
#' @param df Data frame to validate
#' @param schema Expected schema definition (named list)
#' @return TRUE if valid, error otherwise
#' @export
validate_types <- function(df, schema) {
  errors <- list()
  
  for (col in names(schema)) {
    if (!col %in% names(df)) {
      errors <- append(errors, 
                      paste("Missing column:", col))
      next
    }
    
    expected_type <- schema[[col]]
    actual_type <- class(df[[col]])[1]
    
    if (!compatible_types(actual_type, expected_type)) {
      errors <- append(errors, 
        paste("Type mismatch for", col, ":",
              "expected", expected_type, 
              "got", actual_type))
    }
  }
  
  if (length(errors) > 0) {
    stop(paste(errors, collapse = "\n"))
  }
  
  return(TRUE)
}

#' Check if two types are compatible
#' 
#' @param actual_type Actual type name
#' @param expected_type Expected type name
#' @return Logical indicating compatibility
#' @export
compatible_types <- function(actual_type, expected_type) {
  # Define type compatibility rules
  compatibility_map <- list(
    "integer" = c("integer", "numeric"),
    "numeric" = c("numeric", "double", "integer"),
    "character" = c("character", "factor"),
    "logical" = c("logical"),
    "Date" = c("Date", "POSIXct", "POSIXlt"),
    "POSIXct" = c("POSIXct", "POSIXlt", "Date"),
    "factor" = c("factor", "character")
  )
  
  if (expected_type %in% names(compatibility_map)) {
    return(actual_type %in% compatibility_map[[expected_type]])
  }
  
  return(actual_type == expected_type)
}

#' Optimize data types for storage and query performance
#' 
#' @param df Data frame to optimize
#' @param cardinality_threshold Threshold for converting to factor (default 100)
#' @return Data frame with optimized types
#' @export
optimize_types <- function(df, cardinality_threshold = 100) {
  df %>%
    mutate(across(
      where(is.character),
      ~{
        if (n_distinct(.) < cardinality_threshold) {
          factor(.)
        } else {
          .
        }
      }
    )) %>%
    mutate(across(
      where(is.numeric),
      ~{
        if (all(. == as.integer(.), na.rm = TRUE) && 
            all(abs(.) <= .Machine$integer.max, na.rm = TRUE)) {
          as.integer(.)
        } else {
          .
        }
      }
    ))
}

#' Clean column names for database compatibility
#' 
#' @param df Data frame with columns to clean
#' @return Data frame with cleaned column names
#' @export
clean_column_names <- function(df) {
  names(df) <- names(df) %>%
    tolower() %>%
    gsub("[^a-z0-9_]", "_", .) %>%
    gsub("^([0-9])", "col_\\1", .) %>%
    gsub("__+", "_", .) %>%
    gsub("^_|_$", "", .)
  
  return(df)
}

#' Handle missing values with various strategies
#' 
#' @param df Data frame to process
#' @param strategy Strategy for handling NA ("drop", "fill", "interpolate")
#' @param fill_value Value to use for "fill" strategy
#' @return Data frame with handled missing values
#' @export
handle_missing_values <- function(df, strategy = "drop", fill_value = NULL) {
  switch(strategy,
    "drop" = df %>% tidyr::drop_na(),
    "fill" = {
      if (is.null(fill_value)) {
        stop("fill_value must be provided for 'fill' strategy")
      }
      df %>% tidyr::replace_na(fill_value)
    },
    "interpolate" = {
      df %>%
        mutate(across(
          where(is.numeric),
          ~zoo::na.approx(., na.rm = FALSE)
        ))
    },
    stop("Unknown strategy: ", strategy)
  )
}