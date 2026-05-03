#' Template Function Following MAMBA Principles
#'
#' This is a template function that demonstrates proper implementation
#' following all MAMBA principles. Use this as a reference when creating
#' new utility functions.
#'
#' Following principles:
#' - R021: One function one file rule
#' - R069: Function file naming (fn_ prefix)
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function, no side effects)
#' - R067: Functional encapsulation
#' - R051: Lowercase variable naming
#' - MP070: Type prefix naming (verb_noun pattern)
#' - MP018: Don't repeat yourself
#' - P076: Error handling patterns
#'
#' @param input_data A data frame, vector, or list to process
#' @param operation Character string specifying the operation ('transform', 'filter', 'aggregate')
#' @param options Named list of additional options (default: NULL)
#' @param validate Logical, whether to validate inputs (default: TRUE)
#' 
#' @return Processed data in the same format as input_data, or NULL if error
#' @export
#'
#' @examples
#' # Basic usage
#' df <- data.frame(x = 1:5, y = 6:10)
#' template_example(df, "transform")
#' 
#' # With options
#' template_example(df, "filter", options = list(threshold = 3))
#' 
#' # Error handling
#' template_example(NULL, "transform")  # Returns NULL with warning
#'
#' @seealso 
#' \code{\link{remove_na_columns}} for NA handling
#' \code{\link{clean_column_names}} for column name cleaning
#'
template_example <- function(input_data, 
                           operation = c("transform", "filter", "aggregate"),
                           options = NULL,
                           validate = TRUE) {
  
  # Following P076: Error handling patterns - Input validation
  if (validate) {
    # Check for NULL or missing input
    if (is.null(input_data) || missing(input_data)) {
      warning("Input data is NULL or missing")
      return(NULL)
    }
    
    # Validate operation parameter
    operation <- match.arg(operation)
    
    # Validate options if provided
    if (!is.null(options) && !is.list(options)) {
      stop("Options must be a named list or NULL")
    }
  }
  
  # Following MP047: Functional programming - Create result without modifying input
  result <- tryCatch({
    
    # Process based on operation type
    switch(operation,
      "transform" = {
        # Example transformation (following R051: lowercase naming)
        transformed_data <- input_data
        
        if (is.data.frame(transformed_data)) {
          # Apply transformation to numeric columns only
          numeric_cols <- sapply(transformed_data, is.numeric)
          if (any(numeric_cols)) {
            transformed_data[numeric_cols] <- lapply(
              transformed_data[numeric_cols], 
              function(x) x * 2
            )
          }
        } else if (is.numeric(transformed_data)) {
          transformed_data <- transformed_data * 2
        }
        
        transformed_data
      },
      
      "filter" = {
        # Example filtering
        threshold <- options$threshold %||% 0
        
        if (is.data.frame(input_data)) {
          # Filter rows where first numeric column > threshold
          numeric_cols <- which(sapply(input_data, is.numeric))
          if (length(numeric_cols) > 0) {
            first_numeric <- numeric_cols[1]
            input_data[input_data[[first_numeric]] > threshold, , drop = FALSE]
          } else {
            input_data
          }
        } else if (is.numeric(input_data)) {
          input_data[input_data > threshold]
        } else {
          input_data
        }
      },
      
      "aggregate" = {
        # Example aggregation
        if (is.data.frame(input_data)) {
          # Aggregate numeric columns
          numeric_cols <- sapply(input_data, is.numeric)
          if (any(numeric_cols)) {
            data.frame(
              mean = colMeans(input_data[numeric_cols], na.rm = TRUE),
              sd = apply(input_data[numeric_cols], 2, sd, na.rm = TRUE)
            )
          } else {
            data.frame(message = "No numeric columns to aggregate")
          }
        } else if (is.numeric(input_data)) {
          c(mean = mean(input_data, na.rm = TRUE),
            sd = sd(input_data, na.rm = TRUE))
        } else {
          NULL
        }
      }
    )
    
  }, error = function(e) {
    # Following P076: Defensive error handling
    warning(paste("Error in template_example:", e$message))
    return(NULL)
  })
  
  # Return result (following MP047: pure function, no side effects)
  return(result)
}

# Define the NULL-coalescing operator if not available
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}