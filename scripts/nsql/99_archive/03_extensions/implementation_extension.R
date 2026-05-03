#' NSQL Extension for Implementation Directives
#' 
#' This extension adds implementation directives to NSQL to bridge documentation and code.
#' It defines the IMPLEMENT syntax for creating executable scripts from derivation documents.
#' 
#' See also:
#' - implementation_phrases.md - Documents standard implementation phrases
#' - implementation_syntax.md - Defines syntax for implementation directives
#' - fn_parse_implementation.R - Provides parsing functions for implementation directives
#' - implementation_phrase_registry.xlsx - Registry of implementation phrases
#' 
#' @export
#' @keywords NSQL extension

#' Register implementation extension with NSQL registry
#' 
#' @param registry The NSQL registry to extend
#' @return The updated registry
#' @export
register_extension <- function(registry) {
  # Add extension metadata
  registry$extensions$implementation <- list(
    name = "Implementation Directive Extension",
    version = "1.0.0",
    description = "Extends NSQL with implementation directives",
    author = "Claude",
    implements = "R103" # Dependency-Based Sourcing
  )
  
  # Register new statement patterns
  registry <- register_statement_patterns(registry)
  
  # Register new functions
  registry <- register_functions(registry)
  
  # Register translation rules
  registry <- register_translation_rules(registry)
  
  # Return the updated registry
  return(registry)
}

#' Register statement patterns for implementation directives
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_statement_patterns <- function(registry) {
  # Add the IMPLEMENT pattern
  registry$patterns$implement_directive <- list(
    syntax = "IMPLEMENT {section}[, {section}...] IN {location} [AND EXECUTE] === {implementation_details} ===",
    description = "Create executable scripts from derivation documents",
    translation = function(ast, target) {
      # Parse the sections to implement
      sections <- strsplit(ast$section, "\\s*,\\s*")[[1]]
      location <- ast$location
      execute <- !is.null(ast$execute) && ast$execute
      
      # Extract implementation details
      impl_details <- ast$implementation_details
      
      # Generate R code
      if (target == "rsql") {
        # Generate script creation for each section
        script_creations <- vapply(sections, function(section) {
          script_name <- paste0(section, ".R")
          return(sprintf('write_script("%s", "%s")', script_name, location))
        }, character(1))
        
        # Join the script creations
        result <- paste(script_creations, collapse = "\n")
        
        # Add execution if needed
        if (execute) {
          execution_statements <- vapply(sections, function(section) {
            script_name <- paste0(section, ".R")
            script_path <- file.path(location, script_name)
            return(sprintf('execute_script("%s")', script_path))
          }, character(1))
          
          result <- paste(c(result, execution_statements), collapse = "\n")
        }
        
        return(result)
      } else {
        return(NULL)  # Unsupported target
      }
    }
  )
  
  return(registry)
}

#' Register implementation functions
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_functions <- function(registry) {
  # Add the write_script function
  registry$functions$write_script <- list(
    name = "write_script",
    description = "Generate an R script implementing a derivation section",
    parameters = list(
      script_name = list(
        type = "string",
        description = "Name of the script file to create"
      ),
      location = list(
        type = "string",
        description = "Directory path where the script should be created"
      ),
      content = list(
        type = "string",
        description = "Optional content to include in the script",
        required = FALSE
      )
    ),
    return_type = "boolean",
    implementation = list(
      rsql = "write_script"
    )
  )
  
  # Add the execute_script function
  registry$functions$execute_script <- list(
    name = "execute_script",
    description = "Execute an R script",
    parameters = list(
      script_path = list(
        type = "string",
        description = "Path to the script to execute"
      )
    ),
    return_type = "any",
    implementation = list(
      rsql = "execute_script"
    )
  )
  
  return(registry)
}

#' Register translation rules for implementation directives
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_translation_rules <- function(registry) {
  # Add translation rule for IMPLEMENT directive to R code
  registry$translation_rules$implement_to_r <- list(
    pattern = "IMPLEMENT (\\w+) IN (\\w+)",
    targets = list(
      rsql = function(match) {
        return(sprintf(
          'write_script("%s.R", "%s")',
          match[1],
          match[2]
        ))
      }
    )
  )
  
  return(registry)
}

#' Parse NSQL IMPLEMENT statement
#' 
#' @param nsql_statement String containing the NSQL IMPLEMENT statement
#' @return A list with parsed components
#' @export
parse_implement_statement <- function(nsql_statement) {
  # Extract the intent line (IMPLEMENT sections IN location)
  intent_pattern <- "IMPLEMENT\\s+(.+?)\\s+IN\\s+(.+?)(\\s+AND\\s+EXECUTE)?\\s*\n===\\s*"
  intent_match <- regexpr(intent_pattern, nsql_statement, perl = TRUE)
  
  if (intent_match == -1) {
    return(NULL) # Invalid format
  }
  
  intent_text <- regmatches(nsql_statement, intent_match)
  
  # Extract sections and location
  sections_pattern <- "IMPLEMENT\\s+(.+?)\\s+IN\\s+(.+?)(\\s+AND\\s+EXECUTE)?"
  sections_match <- regexec(sections_pattern, intent_text, perl = TRUE)
  
  if (length(sections_match[[1]]) < 3) {
    return(NULL) # Couldn't extract components
  }
  
  # Get the matches
  matches <- regmatches(intent_text, sections_match)[[1]]
  sections_text <- matches[2]
  location <- matches[3]
  execute <- !is.na(matches[4])
  
  # Split sections if multiple
  sections <- strsplit(sections_text, "\\s*,\\s*")[[1]]
  
  # Extract implementation details
  impl_pattern <- "===\\s*(.+?)\\s*===$"
  impl_match <- regexec(impl_pattern, nsql_statement, perl = TRUE, dotall = TRUE)
  
  implementation_details <- NULL
  if (length(impl_match[[1]]) >= 2) {
    implementation_details <- regmatches(nsql_statement, impl_match)[[1]][2]
  }
  
  # Return parsed components
  return(list(
    sections = sections,
    location = location,
    execute = execute,
    implementation_details = implementation_details
  ))
}

#' Generate NSQL IMPLEMENT statement
#' 
#' @param sections Vector of section names to implement
#' @param location Directory path where scripts should be created
#' @param execute Boolean indicating whether to execute the scripts
#' @param implementation_details Details of the implementation
#' @return A properly formatted NSQL IMPLEMENT statement
#' @export
generate_implement_statement <- function(sections, location, execute = FALSE, implementation_details = "") {
  # Build the sections string
  sections_string <- paste(sections, collapse = ", ")
  
  # Build the execute part
  execute_string <- if (execute) " AND EXECUTE" else ""
  
  # Build the full NSQL statement
  nsql_statement <- sprintf(
    "IMPLEMENT %s IN %s%s\n===\n%s\n===",
    sections_string,
    location,
    execute_string,
    implementation_details
  )
  
  return(nsql_statement)
}