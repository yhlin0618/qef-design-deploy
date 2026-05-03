#' Functionalize Module
#'
#' This module implements the process of converting procedural code blocks into proper functional form,
#' following MP47 (Functional Programming Metaprinciple) and R67 (Functional Encapsulation Rule).
#'
#' @author Claude
#' @date 2025-04-08
#' @implements MP47 Functional Programming Metaprinciple
#' @implements R67 Functional Encapsulation Rule
#' @implements R21 One Function One File
#' @implements MP17 Separation of Concerns

#' Analyze and functionally restructure code
#'
#' Takes a code block and analyzes it to create a proper function with explicit
#' inputs and outputs, following the functional programming paradigm.
#'
#' @param code_block String containing the procedural code to functionalize
#' @param target_directory Directory where the resulting function file should be saved
#' @param function_name Optional name for the new function (will be derived from code if not provided)
#' @param description Optional description of the function's purpose
#' @param parameters Optional list of parameter descriptions
#' @return A list containing: function_file (path to created file), usage_code (code to use the function), original_scope_variables (variables that need to be passed as parameters)
#' @export
functionalize <- function(code_block, target_directory, function_name = NULL, description = NULL, parameters = NULL) {
  # Input validation
  if (missing(code_block) || is.null(code_block) || code_block == "") {
    stop("Code block is required")
  }
  
  if (missing(target_directory) || is.null(target_directory) || target_directory == "") {
    # Default to utils if not specified
    target_directory <- file.path("update_scripts", "global_scripts", "04_utils")
    message("No target directory specified, using default: ", target_directory)
  }
  
  # Ensure target directory exists
  if (!dir.exists(target_directory)) {
    stop("Target directory does not exist: ", target_directory)
  }
  
  # Parse the code block
  parsed_result <- parse_code_block(code_block)
  
  # If function name not provided, derive it from code purpose
  if (is.null(function_name)) {
    function_name <- derive_function_name(parsed_result$purpose)
    message("Derived function name: ", function_name)
  }
  
  # Create function file path
  function_file <- file.path(target_directory, paste0("fn_", function_name, ".R"))
  
  # Generate the function code
  function_code <- generate_function_code(
    parsed_result,
    function_name,
    description,
    parameters
  )
  
  # Generate usage code
  usage_code <- generate_usage_code(
    function_name,
    parsed_result$inputs,
    parsed_result$outputs
  )
  
  # Write the function to a file
  write_function_file(function_file, function_code)
  
  # Return the result
  return(list(
    function_file = function_file,
    usage_code = usage_code,
    original_scope_variables = parsed_result$inputs
  ))
}

#' Parse a code block to identify inputs, outputs, and purpose
#'
#' @param code_block The code block to parse
#' @return A list containing inputs, outputs, and inferred purpose
#' @noexport
parse_code_block <- function(code_block) {
  # This is a simplified implementation that would need to be enhanced
  # with more sophisticated static analysis for a production version
  
  # Parse the code using R's built-in parser
  parsed_expr <- tryCatch({
    parse(text = code_block)
  }, error = function(e) {
    stop("Failed to parse code block: ", e$message)
  })
  
  # Initialize result
  result <- list(
    inputs = character(0),
    outputs = character(0),
    purpose = "data_processing" # Default purpose
  )
  
  # Find potential input variables (used but not defined in this scope)
  all_symbols <- all.names(parsed_expr, unique = TRUE)
  assigned_symbols <- find_assigned_symbols(parsed_expr)
  input_symbols <- setdiff(all_symbols, assigned_symbols)
  
  # Remove common R functions and operators
  base_symbols <- c(names(baseenv()), names(.GlobalEnv), 
                   "if", "for", "while", "function", "return",
                   "+", "-", "*", "/", "==", "!=", "<", ">", "<=", ">=",
                   "&&", "||", "!", "&", "|", "^", "%*%", "%%", "%/%",
                   "c", "list", "data.frame", "matrix", "array")
  input_symbols <- setdiff(input_symbols, base_symbols)
  
  # Find output variables (last assigned or explicitly returned)
  output_symbols <- find_output_symbols(parsed_expr)
  
  # Infer purpose based on code patterns
  purpose <- infer_purpose(code_block, all_symbols, assigned_symbols)
  
  # Update result
  result$inputs <- input_symbols
  result$outputs <- output_symbols
  result$purpose <- purpose
  
  return(result)
}

#' Find symbols that are assigned values in the code
#'
#' @param parsed_expr Parsed expression
#' @return Character vector of assigned symbols
#' @noexport
find_assigned_symbols <- function(parsed_expr) {
  # This is a simplified implementation that needs enhancement
  assigned <- character(0)
  
  # Walk through the parse tree looking for assignments
  walk_tree <- function(expr) {
    if (is.call(expr)) {
      if (as.character(expr[[1]]) %in% c("<-", "=", "<<-")) {
        if (is.name(expr[[2]])) {
          assigned <<- c(assigned, as.character(expr[[2]]))
        }
      }
      for (i in 1:length(expr)) {
        if (is.call(expr[[i]]) || is.pairlist(expr[[i]])) {
          walk_tree(expr[[i]])
        }
      }
    }
  }
  
  # Apply to each top-level expression
  for (i in seq_along(parsed_expr)) {
    walk_tree(parsed_expr[[i]])
  }
  
  return(unique(assigned))
}

#' Find symbols that are likely outputs from the code
#'
#' @param parsed_expr Parsed expression
#' @return Character vector of output symbols
#' @noexport
find_output_symbols <- function(parsed_expr) {
  # Find explicitly returned values
  returned <- character(0)
  
  # Walk through the parse tree looking for return statements
  walk_tree <- function(expr) {
    if (is.call(expr)) {
      if (as.character(expr[[1]]) == "return") {
        if (is.name(expr[[2]])) {
          returned <<- c(returned, as.character(expr[[2]]))
        }
      }
      for (i in 1:length(expr)) {
        if (is.call(expr[[i]]) || is.pairlist(expr[[i]])) {
          walk_tree(expr[[i]])
        }
      }
    }
  }
  
  # Apply to each top-level expression
  for (i in seq_along(parsed_expr)) {
    walk_tree(parsed_expr[[i]])
  }
  
  # If no explicit returns, look for last assigned variable
  if (length(returned) == 0) {
    # Get all assignments in order
    assigned <- character(0)
    
    for (i in seq_along(parsed_expr)) {
      expr <- parsed_expr[[i]]
      if (is.call(expr) && as.character(expr[[1]]) %in% c("<-", "=")) {
        if (is.name(expr[[2]])) {
          assigned <- c(assigned, as.character(expr[[2]]))
        }
      }
    }
    
    # If there are assignments, the last one is likely the output
    if (length(assigned) > 0) {
      returned <- assigned[length(assigned)]
    }
  }
  
  return(unique(returned))
}

#' Infer the purpose of the code based on patterns
#'
#' @param code_block Original code block
#' @param all_symbols All symbols in the code
#' @param assigned_symbols Symbols assigned values in the code
#' @return A string describing the inferred purpose
#' @noexport
infer_purpose <- function(code_block, all_symbols, assigned_symbols) {
  # Check for common patterns
  
  # Database operations
  if (any(grepl("db|conn|connect|query|sql", code_block, ignore.case = TRUE))) {
    if (any(grepl("check|detect|available", code_block, ignore.case = TRUE))) {
      return("detect_data_availability")
    }
    return("database_operation")
  }
  
  # Data transformation
  if (any(grepl("transform|convert|process", code_block, ignore.case = TRUE))) {
    return("data_transformation")
  }
  
  # Initialization
  if (any(grepl("init|setup|configure", code_block, ignore.case = TRUE))) {
    return("initialization")
  }
  
  # Validation
  if (any(grepl("valid|check|verify", code_block, ignore.case = TRUE))) {
    return("validation")
  }
  
  # Logging
  if (any(grepl("log|message|warning", code_block, ignore.case = TRUE))) {
    return("logging")
  }
  
  # Default to data processing
  return("data_processing")
}

#' Derive a function name from the inferred purpose
#'
#' @param purpose The inferred purpose of the code
#' @return A suggested function name
#' @noexport
derive_function_name <- function(purpose) {
  # Map purposes to function name prefixes
  prefix_map <- list(
    "detect_data_availability" = "detect",
    "database_operation" = "query",
    "data_transformation" = "transform",
    "initialization" = "initialize",
    "validation" = "validate",
    "logging" = "log",
    "data_processing" = "process"
  )
  
  # Get the prefix or default to "process"
  prefix <- prefix_map[[purpose]]
  if (is.null(prefix)) {
    prefix <- "process"
  }
  
  # Add a suffix based on purpose
  suffix <- switch(purpose,
    "detect_data_availability" = "availability",
    "database_operation" = "data",
    "data_transformation" = "data",
    "initialization" = "system",
    "validation" = "input",
    "logging" = "status",
    "data_processing" = "data"
  )
  
  # Combine to create function name
  function_name <- paste(prefix, suffix, sep = "_")
  
  return(function_name)
}

#' Generate the function code
#'
#' @param parsed_result The result of parsing the code block
#' @param function_name The name for the function
#' @param description Optional description of the function
#' @param parameters Optional list of parameter descriptions
#' @return A string containing the complete function code
#' @noexport
generate_function_code <- function(parsed_result, function_name, description = NULL, parameters = NULL) {
  # Start with the roxygen documentation
  if (is.null(description)) {
    description <- paste("Process data according to", function_name, "logic")
  }
  
  roxygen <- paste0(
    "#' ", description, "\n",
    "#'\n"
  )
  
  # Add parameter documentation
  if (length(parsed_result$inputs) > 0) {
    for (param in parsed_result$inputs) {
      param_desc <- if (!is.null(parameters) && !is.null(parameters[[param]])) {
        parameters[[param]]
      } else {
        paste("The", param, "input")
      }
      roxygen <- paste0(roxygen, "#' @param ", param, " ", param_desc, "\n")
    }
  }
  
  # Add return documentation
  if (length(parsed_result$outputs) > 0) {
    output_desc <- paste("The processed", paste(parsed_result$outputs, collapse = ", "))
    roxygen <- paste0(roxygen, "#' @return ", output_desc, "\n")
  } else {
    roxygen <- paste0(roxygen, "#' @return The processing result\n")
  }
  
  # Add export tag
  roxygen <- paste0(roxygen, "#' @export\n")
  
  # Function signature
  signature <- paste0(
    function_name, " <- function(",
    ifelse(length(parsed_result$inputs) > 0, 
          paste(parsed_result$inputs, collapse = ", "), 
          ""),
    ") {\n"
  )
  
  # Function body - indent each line
  body_lines <- strsplit(parsed_result$code_block, "\n")[[1]]
  body <- paste0("  ", body_lines, collapse = "\n")
  
  # Add return statement if there are outputs and no explicit return
  if (length(parsed_result$outputs) > 0) {
    # Check if there's already a return statement
    if (!any(grepl("return\\(", body_lines))) {
      body <- paste0(body, "\n  return(", parsed_result$outputs[1], ")")
    }
  }
  
  # Combine all parts
  function_code <- paste0(roxygen, signature, body, "\n}\n")
  
  return(function_code)
}

#' Generate code to use the function
#'
#' @param function_name The name of the function
#' @param inputs The input variables
#' @param outputs The output variables
#' @return A string containing the code to use the function
#' @noexport
generate_usage_code <- function(function_name, inputs, outputs) {
  # Create the function call
  call <- paste0(
    function_name, "(",
    ifelse(length(inputs) > 0, 
          paste(inputs, collapse = ", "), 
          ""),
    ")"
  )
  
  # Assign to output variable if there are outputs
  if (length(outputs) > 0) {
    usage <- paste0(outputs[1], " <- ", call)
  } else {
    usage <- call
  }
  
  return(usage)
}

#' Write the function code to a file
#'
#' @param file_path The path to write the file to
#' @param function_code The function code to write
#' @return TRUE if successful, FALSE otherwise
#' @noexport
write_function_file <- function(file_path, function_code) {
  tryCatch({
    # Create directory if it doesn't exist
    dir_path <- dirname(file_path)
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE)
    }
    
    # Write the file
    cat(function_code, file = file_path)
    message("Function written to: ", file_path)
    return(TRUE)
  }, error = function(e) {
    warning("Failed to write function file: ", e$message)
    return(FALSE)
  })
}

#' Execute the functionalization process on a code block
#'
#' This is a convenience wrapper around the functionalize function
#' that handles the full process from code block to function file.
#'
#' @param code_block The code block to functionalize
#' @param target_directory The directory to save the function to
#' @param function_name Optional name for the function
#' @param description Optional description of the function
#' @param parameters Optional list of parameter descriptions
#' @return The result of functionalization
#' @export
run_functionalization <- function(code_block, target_directory, function_name = NULL, 
                                description = NULL, parameters = NULL) {
  result <- functionalize(
    code_block = code_block,
    target_directory = target_directory,
    function_name = function_name,
    description = description,
    parameters = parameters
  )
  
  # Print the usage instructions
  cat("\nFunctionalization complete!\n")
  cat("Function saved to:", result$function_file, "\n")
  cat("\nTo use this function, replace your original code with:\n")
  cat(result$usage_code, "\n")
  cat("\nMake sure these variables are in scope:", 
      paste(result$original_scope_variables, collapse = ", "), "\n")
  
  return(result)
}