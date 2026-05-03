#' Parse Implementation Directives
#'
#' @description
#' Parses IMPLEMENT directives in NSQL and translates them to executable R code.
#'
#' @param nsql_text The NSQL text containing IMPLEMENT directives
#' @param registry_path Path to the implementation phrase registry (xlsx file)
#' @return A list of parsed implementation directives with their R code translations
#'
#' @importFrom readxl read_excel
#' @importFrom stringr str_match str_extract_all str_replace_all str_trim
#'
#' @examples
#' nsql_text <- "IMPLEMENT D00_01_00 IN update_scripts
#' === Implementation Details ===
#' INITIALIZE IN UPDATE_MODE
#' CONNECT TO APP_DATA
#'
#' IMPLEMENT TABLE CREATION
#'   $connection = app_data
#'   $table_name = df_customer_profile
#'   $column_definitions = list(...)
#'   $primary_key = c(\"customer_id\", \"platform_id\")
#'   $indexes = list(list(columns = \"platform_id\"))
#' "
#' parse_implementation(nsql_text)
#'
#' @export
parse_implementation <- function(nsql_text, registry_path = NULL) {
  # Default registry path if not provided
  if (is.null(registry_path)) {
    script_path <- normalizePath(file.path(getwd(), "scripts", "nsql", "extensions", "IMPLEMENT"))
    registry_path <- file.path(script_path, "implementation_phrase_registry.xlsx")
  }
  
  # Check if registry exists
  if (!file.exists(registry_path)) {
    message("Implementation phrase registry not found at: ", registry_path)
    message("Using hardcoded implementation phrases instead")
    phrases_df <- data.frame(
      Directive = c("INITIALIZE", "INITIALIZE", "CONNECT", "CONNECT", "IMPLEMENT"),
      Context = c("UPDATE_MODE", "APP_MODE", "APP_DATA", "RAW_DATA", "TABLE CREATION"),
      `Implementation Code` = c(
        "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_update_mode.R'))",
        "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_app_mode.R'))",
        "app_data <- dbConnect_from_list('app_data')",
        "raw_data <- dbConnect_from_list('raw_data')",
        "create_table_query <- generate_create_table_query(\n  con = $connection,\n  or_replace = TRUE,\n  target_table = '$table_name',\n  source_table = NULL,\n  column_defs = $column_definitions,\n  primary_key = $primary_key,\n  indexes = $indexes\n)\ndbExecute($connection, create_table_query)"
      )
    )
  } else {
    # Load implementation phrases from registry
    phrases_df <- tryCatch({
      readxl::read_excel(registry_path, sheet = "Implementation Phrases")
    }, error = function(e) {
      message("Error reading registry: ", e$message)
      data.frame(Directive = character(), Context = character(), `Implementation Code` = character())
    })
  }
  
  # Extract IMPLEMENT directives from NSQL text
  implement_pattern <- "IMPLEMENT\\s+([^\\s]+)\\s+IN\\s+([^\\s\n]+)(?:\\s+WITH\\s+([^\n]+))?\n===\\s*Implementation\\s*Details\\s*===\n(.*?)(?=IMPLEMENT|$)"
  implement_matches <- gregexpr(implement_pattern, nsql_text, perl = TRUE, dotall = TRUE)
  
  # Initialize results list
  implementations <- list()
  
  # Process each implementation directive found
  if (length(implement_matches) > 0 && implement_matches[[1]][1] > 0) {
    matches <- regmatches(nsql_text, implement_matches)[[1]]
    
    for (i in seq_along(matches)) {
      match <- matches[i]
      parts <- unlist(strsplit(match, "\n===\\s*Implementation\\s*Details\\s*===\n", perl = TRUE))
      
      if (length(parts) >= 2) {
        # Extract parts from the directive header
        header <- parts[1]
        header_pattern <- "IMPLEMENT\\s+([^\\s]+)\\s+IN\\s+([^\\s\n]+)(?:\\s+WITH\\s+([^\n]+))?"
        header_parts <- regmatches(header, regexec(header_pattern, header, perl = TRUE))[[1]]
        
        entity_id <- header_parts[2]
        location <- header_parts[3]
        options <- if (length(header_parts) >= 4 && !is.na(header_parts[4])) header_parts[4] else ""
        
        # Extract the implementation content
        content <- parts[2]
        
        # Process implementation content - look for implementation phrases
        implementation_code <- process_implementation_content(content, phrases_df)
        
        # Add to results
        implementations[[length(implementations) + 1]] <- list(
          entity_id = entity_id,
          location = location,
          options = options,
          content = content,
          implementation_code = implementation_code
        )
      }
    }
  }
  
  return(implementations)
}

#' Process Implementation Content
#'
#' @description
#' Processes the implementation content by replacing implementation phrases with their R code equivalents.
#'
#' @param content The implementation content to process
#' @param phrases_df Data frame of implementation phrases
#' @return Processed implementation code
#'
#' @keywords internal
process_implementation_content <- function(content, phrases_df) {
  # Split content into lines
  lines <- unlist(strsplit(content, "\n"))
  processed_lines <- character(0)
  i <- 1
  
  while (i <= length(lines)) {
    line <- lines[i]
    line_trimmed <- gsub("^\\s+|\\s+$", "", line)
    
    # Check if this line is an implementation phrase
    phrase_parts <- strsplit(line_trimmed, "\\s+IN\\s+", perl = TRUE)[[1]]
    if (length(phrase_parts) == 2) {
      directive <- phrase_parts[1]
      context <- phrase_parts[2]
      
      # Look up the phrase in the registry
      phrase_match <- which(phrases_df$Directive == directive & phrases_df$Context == context)
      
      if (length(phrase_match) > 0) {
        # Found a matching phrase
        implementation_code <- phrases_df$`Implementation Code`[phrase_match[1]]
        processed_lines <- c(processed_lines, implementation_code)
        i <- i + 1
      } else {
        # Check if this is a different type of phrase (without IN)
        phrase_parts <- strsplit(line_trimmed, "\\s+", perl = TRUE)[[1]]
        if (length(phrase_parts) > 0) {
          directive <- phrase_parts[1]
          
          # Check for phrases that match just the directive
          if (directive == "IMPLEMENT") {
            # This is an IMPLEMENT TABLE CREATION or similar phrase
            # Collect parameters until blank line or next phrase
            j <- i + 1
            params <- list()
            
            while (j <= length(lines) && grepl("^\\s*\\$", lines[j])) {
              param_line <- lines[j]
              param_parts <- strsplit(gsub("^\\s*\\$", "", param_line), "\\s*=\\s*", perl = TRUE)[[1]]
              
              if (length(param_parts) == 2) {
                param_name <- param_parts[1]
                param_value <- param_parts[2]
                params[[param_name]] <- param_value
              }
              
              j <- j + 1
            }
            
            # Look for phrases that match the context after IMPLEMENT
            if (length(phrase_parts) > 1) {
              context <- paste(phrase_parts[2:length(phrase_parts)], collapse = " ")
              phrase_match <- which(phrases_df$Directive == "IMPLEMENT" & phrases_df$Context == context)
              
              if (length(phrase_match) > 0) {
                # Found a matching phrase
                implementation_code <- phrases_df$`Implementation Code`[phrase_match[1]]
                
                # Replace parameters in the implementation code
                for (param_name in names(params)) {
                  param_pattern <- paste0("\\$", param_name)
                  implementation_code <- gsub(param_pattern, params[[param_name]], implementation_code)
                }
                
                processed_lines <- c(processed_lines, implementation_code)
                i <- j
                next
              }
            }
          }
        }
        
        # If no match found, keep the line as is
        processed_lines <- c(processed_lines, line)
        i <- i + 1
      }
    } else {
      # Not a recognized phrase, keep the line as is
      processed_lines <- c(processed_lines, line)
      i <- i + 1
    }
  }
  
  # Combine processed lines back into a single string
  return(paste(processed_lines, collapse = "\n"))
}

#' Generate Implementation Script
#'
#' @description
#' Generates an implementation script from an NSQL implementation directive.
#'
#' @param implementation A parsed implementation from parse_implementation
#' @param base_path Base path for generating the script (defaults to current working directory)
#' @return Path to the generated script file
#'
#' @export
generate_implementation_script <- function(implementation, base_path = getwd()) {
  # Generate script file path
  script_name <- paste0(implementation$entity_id, ".R")
  script_path <- file.path(base_path, implementation$location, script_name)
  
  # Create the script content
  script_content <- paste0(
    "#' @file ", script_name, "\n",
    "#' @generated Generated from NSQL IMPLEMENT directive\n",
    "#' @date ", format(Sys.Date(), "%Y-%m-%d"), "\n",
    "#' @entity ", implementation$entity_id, "\n\n",
    implementation$implementation_code
  )
  
  # Ensure directory exists
  dir.create(file.path(base_path, implementation$location), recursive = TRUE, showWarnings = FALSE)
  
  # Write the script to file
  writeLines(script_content, script_path)
  
  message("Generated implementation script: ", script_path)
  
  return(script_path)
}

#' Execute Implementation
#'
#' @description
#' Executes an implementation script if automated execution is enabled.
#'
#' @param implementation A parsed implementation from parse_implementation
#' @param base_path Base path for the script (defaults to current working directory)
#' @return Result of the script execution (if executed)
#'
#' @export
execute_implementation <- function(implementation, base_path = getwd()) {
  # Check if automated execution is enabled
  options_list <- strsplit(implementation$options, ",\\s*")[[1]]
  auto_exec <- any(grepl("automated_execution=TRUE", options_list, fixed = TRUE))
  
  if (auto_exec) {
    # Generate the script if it doesn't exist
    script_path <- file.path(base_path, implementation$location, paste0(implementation$entity_id, ".R"))
    if (!file.exists(script_path)) {
      script_path <- generate_implementation_script(implementation, base_path)
    }
    
    # Execute the script
    message("Executing implementation script: ", script_path)
    result <- source(script_path)
    return(result)
  } else {
    message("Automated execution not enabled for ", implementation$entity_id)
    return(NULL)
  }
}