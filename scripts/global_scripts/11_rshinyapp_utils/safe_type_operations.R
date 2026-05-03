# Safe Type Operations
# Following MP41: Type-Dependent Operations
# Provides utility functions for safely handling different types

#' Safely extract a value regardless of its container type
#'
#' This function handles multiple input types safely:
#' - Reactive expressions are called with error handling
#' - Lists and environments are accessed with safe lookups
#' - Other values are returned as-is
#'
#' @param x The value or container to extract from
#' @param key For list-like objects, the key to extract
#' @param default Value to return if extraction fails
#' @return The extracted value or default
#' @export
safe_extract <- function(x, key = NULL, default = NULL) {
  # Handle NULL input
  if (is.null(x)) {
    return(default)
  }
  
  # Handle reactive expressions
  if (is.function(x) && 
      (inherits(x, "reactive") || inherits(x, "reactiveVal"))) {
    tryCatch({
      value <- x()
      # If we need to extract a key from the result
      if (!is.null(key) && (is.list(value) || is.environment(value))) {
        if (is.list(value) && key %in% names(value)) {
          return(value[[key]])
        } else if (is.environment(value) && exists(key, envir = value, inherits = FALSE)) {
          return(get(key, envir = value, inherits = FALSE))
        } else {
          return(default)
        }
      } else {
        return(value)
      }
    }, error = function(e) {
      message("Error extracting from reactive: ", e$message)
      return(default)
    })
  }
  
  # Handle list-like objects
  if (!is.null(key)) {
    if (is.list(x) && key %in% names(x)) {
      return(x[[key]])
    } else if (is.environment(x) && exists(key, envir = x, inherits = FALSE)) {
      return(get(key, envir = x, inherits = FALSE))
    } else if (is.data.frame(x) && key %in% names(x)) {
      return(x[[key]])
    } else {
      return(default)
    }
  }
  
  # For non-container types, just return the value
  return(x)
}

#' Safe logical operations on any type
#'
#' Evaluates the logical negation of a value regardless of its type:
#' - Reactive expressions are called first
#' - NULL values are treated as TRUE (they're "empty")
#' - Non-logical types are converted safely
#'
#' @param x The value to check
#' @param default Default value if the operation fails
#' @return The logical negation (TRUE or FALSE)
#' @export
is_empty <- function(x, default = TRUE) {
  # Handle NULL
  if (is.null(x)) {
    return(TRUE)
  }
  
  # Handle reactive expressions
  if (is.function(x) && 
      (inherits(x, "reactive") || inherits(x, "reactiveVal"))) {
    value <- tryCatch({
      x()
    }, error = function(e) {
      message("Error calling reactive: ", e$message)
      return(NULL)
    })
    return(is_empty(value, default))
  }
  
  # Handle different types
  if (is.logical(x)) {
    return(!x)
  } else if (is.character(x)) {
    return(x == "" || is.na(x))
  } else if (is.numeric(x)) {
    return(length(x) == 0 || is.na(x) || x == 0)
  } else if (is.list(x)) {
    return(length(x) == 0)
  } else if (is.data.frame(x)) {
    return(nrow(x) == 0 || ncol(x) == 0)
  } else {
    tryCatch({
      # Try to convert to logical
      return(!as.logical(x))
    }, error = function(e) {
      message("Error in logical conversion: ", e$message)
      return(default)
    })
  }
}

#' Safe logical operation that works with any type
#'
#' Safely checks if a value is TRUE, handling various types:
#' - Reactive expressions are called first
#' - NULL is always FALSE
#' - Non-logical types are converted safely
#'
#' @param x The value to check
#' @param default Default value if the operation fails
#' @return TRUE or FALSE
#' @export
is_true <- function(x, default = FALSE) {
  # Handle NULL
  if (is.null(x)) {
    return(FALSE)
  }
  
  # Handle reactive expressions
  if (is.function(x) && 
      (inherits(x, "reactive") || inherits(x, "reactiveVal"))) {
    value <- tryCatch({
      x()
    }, error = function(e) {
      message("Error calling reactive: ", e$message)
      return(NULL)
    })
    return(is_true(value, default))
  }
  
  # Use isTRUE for logical vectors
  if (is.logical(x)) {
    return(isTRUE(x))
  }
  
  # Handle different types
  if (is.character(x)) {
    x_lower <- tolower(x)
    return(x_lower %in% c("true", "t", "yes", "y", "1"))
  } else if (is.numeric(x)) {
    return(x == 1)
  } else {
    tryCatch({
      # Try to convert to logical
      return(isTRUE(as.logical(x)))
    }, error = function(e) {
      message("Error in logical conversion: ", e$message)
      return(default)
    })
  }
}

#' Safe comparison operation that works with multiple types
#'
#' Compares two values, handling type differences safely:
#' - Reactive expressions are called first
#' - Values are coerced to the same type when possible
#' - NULL values use the default result
#'
#' @param x First value to compare
#' @param y Second value to compare
#' @param comparator Function to use for comparison (default: `==`)
#' @param default Default result if comparison fails
#' @return TRUE or FALSE
#' @export
safe_compare <- function(x, y, comparator = `==`, default = FALSE) {
  # Handle reactive expressions
  if (is.function(x) && 
      (inherits(x, "reactive") || inherits(x, "reactiveVal"))) {
    x <- tryCatch(x(), error = function(e) NULL)
  }
  
  if (is.function(y) && 
      (inherits(y, "reactive") || inherits(y, "reactiveVal"))) {
    y <- tryCatch(y(), error = function(e) NULL)
  }
  
  # Handle NULL values
  if (is.null(x) || is.null(y)) {
    return(default)
  }
  
  # Try to compare directly
  tryCatch({
    return(comparator(x, y))
  }, error = function(e) {
    # If direct comparison fails, try type coercion
    tryCatch({
      # If one is character, convert both to character
      if (is.character(x) || is.character(y)) {
        return(comparator(as.character(x), as.character(y)))
      }
      
      # If one is numeric, try numeric comparison
      if (is.numeric(x) || is.numeric(y)) {
        return(comparator(as.numeric(x), as.numeric(y)))
      }
      
      # If one is logical, try logical comparison
      if (is.logical(x) || is.logical(y)) {
        return(comparator(as.logical(x), as.logical(y)))
      }
      
      # If all else fails, convert to character
      return(comparator(as.character(x), as.character(y)))
    }, error = function(e) {
      message("Comparison failed: ", e$message)
      return(default)
    })
  })
}

#' Check if a value is a reactive expression
#'
#' @param x Value to check
#' @return TRUE if x is a reactive expression, FALSE otherwise
#' @export
is_reactive <- function(x) {
  is.function(x) && (inherits(x, "reactive") || inherits(x, "reactiveVal"))
}

#' Safe access to reactive values
#'
#' If the value is reactive, calls it and returns the result.
#' If not, returns the value as-is.
#'
#' @param x Value or reactive expression
#' @param default Default value if access fails
#' @return The value or result of calling the reactive expression
#' @export
get_reactive_value <- function(x, default = NULL) {
  if (is.null(x)) {
    return(default)
  }
  
  if (is_reactive(x)) {
    tryCatch({
      return(x())
    }, error = function(e) {
      message("Error calling reactive: ", e$message)
      return(default)
    })
  } else {
    return(x)
  }
}

#' Create a type-safe accessor for nested reactive values
#'
#' Creates a function that safely accesses a specific field in a reactive value,
#' handling both the reactive nature and the nested access safely.
#'
#' @param reactive_expr A reactive expression
#' @param field The field to access in the result
#' @param default Default value if the field is missing
#' @return A function that returns the field value when called
#' @export
create_field_accessor <- function(reactive_expr, field, default = NULL) {
  force(reactive_expr)
  force(field)
  force(default)
  
  function() {
    value <- get_reactive_value(reactive_expr)
    if (is.null(value)) {
      return(default)
    }
    
    if (is.list(value) && field %in% names(value)) {
      return(value[[field]])
    } else if (is.environment(value) && exists(field, envir = value, inherits = FALSE)) {
      return(get(field, envir = value, inherits = FALSE))
    } else if (is.data.frame(value) && field %in% names(value)) {
      return(value[[field]])
    } else {
      return(default)
    }
  }
}

# Example usage in Shiny app:
# 
# # Instead of direct reactive access:
# output$plot <- renderPlot({
#   if (!filters) { # Error: invalid argument type
#     return(NULL)
#   }
#   # Plot code...
# })
# 
# # Use safe type operations:
# output$plot <- renderPlot({
#   if (is_empty(filters)) {
#     return(NULL)
#   }
#   # Plot code...
# })
# 
# # For reactive chains:
# filters <- reactive({ ... })
# region_filter <- create_field_accessor(filters, "region", "all")