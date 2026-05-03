# ============================================================================
# Database-Agnostic List Column Handling
# ============================================================================
# Purpose: Generic functions for handling list columns in data frames
# Works with any database via DBI - not specific to DuckDB
# Created: 2025-08-28
# ============================================================================

library(dplyr)
library(purrr)
library(tidyr)
library(jsonlite)

#' Flatten simple list columns
#' 
#' @param df Data frame with list column
#' @param list_col Name of list column to flatten
#' @param keep_index Whether to keep element index
#' @return Data frame with flattened list column
#' @export
flatten_list_column <- function(df, list_col, keep_index = TRUE) {
  result <- df %>%
    mutate(.temp_id = row_number()) %>%
    tidyr::unnest(cols = all_of(list_col))
  
  if (keep_index) {
    result <- result %>%
      group_by(.temp_id) %>%
      mutate(element_index = row_number()) %>%
      ungroup()
  }
  
  result %>% select(-.temp_id)
}

#' Store list column as JSON string
#' 
#' @param df Data frame with list column
#' @param list_cols Vector of list column names (or NULL for all)
#' @param auto_unbox Whether to auto-unbox JSON (default TRUE)
#' @param remove_original Whether to remove original list columns
#' @return Data frame with JSON string columns
#' @export
store_list_as_json <- function(df, list_cols = NULL, auto_unbox = TRUE, remove_original = TRUE) {
  # Auto-detect list columns if not specified
  if (is.null(list_cols)) {
    list_cols <- names(df)[sapply(df, is.list)]
  }
  
  for (col in list_cols) {
    if (col %in% names(df)) {
      json_col_name <- paste0(col, "_json")
      df[[json_col_name]] <- map_chr(
        df[[col]], 
        ~jsonlite::toJSON(., auto_unbox = auto_unbox, null = "null")
      )
      
      if (remove_original) {
        df[[col]] <- NULL
      }
    }
  }
  
  return(df)
}

#' Extract specific elements from list columns
#' 
#' @param df Data frame with list column
#' @param list_col Name of list column
#' @param extract_first Extract first element
#' @param extract_length Extract list length
#' @param extract_summary Create concatenated summary
#' @return Data frame with extracted elements
#' @export
extract_list_elements <- function(df, list_col, 
                                extract_first = TRUE,
                                extract_length = TRUE,
                                extract_summary = TRUE) {
  if (!list_col %in% names(df)) {
    stop("Column '", list_col, "' not found in data frame")
  }
  
  if (extract_first) {
    df[[paste0(list_col, "_first")]] <- map_chr(
      df[[list_col]], 
      ~ifelse(length(.) > 0, as.character(.[[1]]), NA_character_),
      .default = NA_character_
    )
  }
  
  if (extract_length) {
    df[[paste0(list_col, "_length")]] <- map_int(
      df[[list_col]], 
      length,
      .default = 0L
    )
  }
  
  if (extract_summary) {
    df[[paste0(list_col, "_summary")]] <- map_chr(
      df[[list_col]], 
      ~paste(., collapse = ", "),
      .default = ""
    )
  }
  
  return(df)
}

#' Extract first element from multiple list columns
#' 
#' @param df Data frame
#' @param list_cols Character vector of list column names
#' @param remove_original Whether to remove original columns
#' @return Data frame with first elements extracted
#' @export
extract_first_element <- function(df, list_cols = NULL, remove_original = TRUE) {
  # Auto-detect list columns if not specified
  if (is.null(list_cols)) {
    list_cols <- names(df)[sapply(df, is.list)]
  }
  
  for (col in list_cols) {
    if (col %in% names(df)) {
      df[[paste0(col, "_first")]] <- sapply(
        df[[col]], 
        function(x) if(length(x) > 0) x[[1]] else NA,
        USE.NAMES = FALSE
      )
      
      if (remove_original) {
        df[[col]] <- NULL
      }
    }
  }
  
  return(df)
}

#' Process struct/nested data
#' 
#' @param df Data frame
#' @param struct_col Column containing struct data
#' @param method Method for processing ("flatten" or "extract")
#' @param fields Specific fields to extract (for "extract" method)
#' @return Data frame with processed struct data
#' @export
process_struct_data <- function(df, struct_col, method = "flatten", fields = NULL) {
  if (!struct_col %in% names(df)) {
    stop("Column '", struct_col, "' not found in data frame")
  }
  
  if (method == "flatten") {
    # Flatten all fields
    return(df %>%
      tidyr::unnest_wider(all_of(struct_col), 
                          names_sep = "_"))
  } else if (method == "extract") {
    # Extract specific fields
    if (is.null(fields)) {
      # Try to get field names from first non-null element
      first_non_null <- Find(function(x) !is.null(x), df[[struct_col]])
      if (!is.null(first_non_null)) {
        fields <- names(first_non_null)
      } else {
        stop("No fields specified and unable to detect field names")
      }
    }
    
    for (field in fields) {
      df[[paste0(struct_col, "_", field)]] <- map(
        df[[struct_col]], 
        ~.x[[field]] %||% NA
      )
    }
    
    return(df)
  } else {
    stop("Unknown method: ", method)
  }
}

#' Summarize list column with statistics
#' 
#' @param df Data frame
#' @param list_col List column name
#' @param remove_original Whether to remove original list column
#' @return Data frame with summary statistics
#' @export
summarize_list_column <- function(df, list_col, remove_original = TRUE) {
  if (!list_col %in% names(df)) {
    stop("Column '", list_col, "' not found in data frame")
  }
  
  df <- df %>%
    mutate(
      # Count of elements
      !!paste0(list_col, "_count") := sapply(
        .data[[list_col]], length
      ),
      
      # Concatenated string (for display)
      !!paste0(list_col, "_text") := sapply(
        .data[[list_col]], 
        function(x) paste(x, collapse = ", ")
      )
    )
  
  # Add numeric summaries if applicable
  if (any(sapply(df[[list_col]], function(x) is.numeric(x) && length(x) > 0))) {
    df <- df %>%
      mutate(
        !!paste0(list_col, "_min") := sapply(
          .data[[list_col]], 
          function(x) {
            if(length(x) > 0 && is.numeric(x)) min(x, na.rm = TRUE) else NA_real_
          }
        ),
        
        !!paste0(list_col, "_max") := sapply(
          .data[[list_col]], 
          function(x) {
            if(length(x) > 0 && is.numeric(x)) max(x, na.rm = TRUE) else NA_real_
          }
        ),
        
        !!paste0(list_col, "_mean") := sapply(
          .data[[list_col]], 
          function(x) {
            if(length(x) > 0 && is.numeric(x)) mean(x, na.rm = TRUE) else NA_real_
          }
        )
      )
  }
  
  if (remove_original) {
    df[[list_col]] <- NULL
  }
  
  return(df)
}

#' Convert all list columns to JSON strings
#' 
#' @param df Data frame
#' @return Data frame with lists converted to JSON
#' @export
lists_to_json <- function(df) {
  list_cols <- names(df)[sapply(df, is.list)]
  
  for (col in list_cols) {
    df[[col]] <- sapply(
      df[[col]], 
      function(x) jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"),
      USE.NAMES = FALSE
    )
  }
  
  return(df)
}

#' Analyze list column structure
#' 
#' @param df Data frame
#' @param list_col List column to analyze
#' @return List with analysis results
#' @export
analyze_list_column <- function(df, list_col) {
  if (!list_col %in% names(df)) {
    stop("Column '", list_col, "' not found in data frame")
  }
  
  col_data <- df[[list_col]]
  
  list(
    column_name = list_col,
    total_rows = nrow(df),
    null_count = sum(sapply(col_data, is.null)),
    empty_count = sum(sapply(col_data, function(x) length(x) == 0)),
    lengths = table(sapply(col_data, length)),
    is_uniform_length = length(unique(sapply(col_data, length))) == 1,
    element_types = unique(sapply(col_data, function(x) class(x)[1])),
    max_length = max(sapply(col_data, length)),
    min_length = min(sapply(col_data, length)),
    avg_length = mean(sapply(col_data, length)),
    is_simple = all(sapply(col_data, function(x) length(x) <= 1)),
    recommendation = if(all(sapply(col_data, function(x) length(x) <= 1))) {
      "Extract first element"
    } else if(length(unique(sapply(col_data, length))) == 1) {
      "Consider pivoting to columns"
    } else if(mean(sapply(col_data, length)) > 10) {
      "Consider normalized table or JSON storage"
    } else {
      "Summary extraction or JSON storage"
    }
  )
}

#' Choose optimal list column strategy based on data characteristics
#' 
#' @param df Data frame
#' @param list_col List column to process
#' @param auto_apply Whether to automatically apply the recommended strategy
#' @return Processed data frame or recommendation
#' @export
auto_process_list_column <- function(df, list_col, auto_apply = TRUE) {
  analysis <- analyze_list_column(df, list_col)
  
  if (!auto_apply) {
    return(analysis)
  }
  
  # Apply recommended strategy
  if (analysis$is_simple) {
    # Simple lists - extract first element
    return(extract_first_element(df, list_col))
  } else if (analysis$is_uniform_length && analysis$max_length <= 5) {
    # Uniform short lists - pivot to columns
    return(df %>%
      mutate(.temp_id = row_number()) %>%
      tidyr::unnest(cols = all_of(list_col)) %>%
      group_by(.temp_id) %>%
      mutate(element_num = paste0(list_col, "_", row_number())) %>%
      tidyr::pivot_wider(
        names_from = element_num,
        values_from = all_of(list_col)
      ) %>%
      ungroup() %>%
      select(-.temp_id))
  } else if (analysis$avg_length < 10) {
    # Medium lists - summary extraction
    return(summarize_list_column(df, list_col))
  } else {
    # Large lists - JSON storage
    return(store_list_as_json(df, list_col))
  }
}