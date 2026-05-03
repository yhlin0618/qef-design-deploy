#' SNSQL Extension for Database Documentation
#' 
#' This extension adds database documentation commands to the SNSQL language
#' following the MP43 Database Documentation Principle.
#' 
#' @export
#' @keywords SNSQL extension

#' Register database documentation extension with NSQL registry
#' 
#' @param registry The NSQL registry to extend
#' @return The updated registry
#' @export
register_extension <- function(registry) {
  # Add extension metadata
  registry$extensions$database_documentation <- list(
    name = "Database Documentation Extension",
    version = "1.0.0",
    description = "Extends SNSQL with database documentation commands",
    author = "Claude",
    implements = "MP43"
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

#' Register statement patterns for database documentation
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_statement_patterns <- function(registry) {
  # Add the summarize database pattern
  registry$patterns$summarize_database <- list(
    syntax = "summarize database {db_path} [to {output_path}] [with {options}]",
    description = "Generate documentation for a database structure",
    translation = function(ast, target) {
      # Default output path if not specified
      output_path <- if (is.null(ast$output_path)) {
        paste0("docs/database/", basename(ast$db_path), "_structure.md")
      } else {
        ast$output_path
      }
      
      # Parse options
      options <- list(
        include_samples = TRUE,
        sample_rows = 5
      )
      
      if (!is.null(ast$options)) {
        options_list <- strsplit(ast$options, ",")[[1]]
        for (opt in options_list) {
          if (grepl("sample_rows=", opt)) {
            options$sample_rows <- as.numeric(gsub("sample_rows=", "", opt))
          } else if (opt == "no_samples") {
            options$include_samples <- FALSE
          }
        }
      }
      
      # Generate R code
      if (target == "rsql") {
        return(sprintf(
          'summarize_database(\n  db_connection = "%s",\n  output_file = "%s",\n  include_samples = %s,\n  sample_rows = %d\n)',
          ast$db_path,
          output_path,
          ifelse(options$include_samples, "TRUE", "FALSE"),
          options$sample_rows
        ))
      } else {
        return(NULL)  # Unsupported target
      }
    }
  )
  
  # Add the document all databases pattern
  registry$patterns$document_all_databases <- list(
    syntax = "document all databases [in {output_dir}]",
    description = "Generate documentation for all project databases",
    translation = function(ast, target) {
      # Default output directory if not specified
      output_dir <- if (is.null(ast$output_dir)) {
        "docs/database"
      } else {
        ast$output_dir
      }
      
      # Generate R code
      if (target == "rsql") {
        return(sprintf(
          'document_all_databases(output_dir = "%s")',
          output_dir
        ))
      } else {
        return(NULL)  # Unsupported target
      }
    }
  )
  
  return(registry)
}

#' Register database documentation functions
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_functions <- function(registry) {
  # Add the summarize_database function
  registry$functions$summarize_database <- list(
    name = "summarize_database",
    description = "Generate documentation for a database structure",
    parameters = list(
      db_connection = list(
        type = "string",
        description = "Path to database file or connection object"
      ),
      output_file = list(
        type = "string",
        description = "Path to output Markdown file"
      ),
      include_samples = list(
        type = "boolean",
        description = "Whether to include data samples",
        default = TRUE
      ),
      sample_rows = list(
        type = "integer",
        description = "Number of sample rows to include",
        default = 5
      )
    ),
    return_type = "string",
    implementation = list(
      rsql = "summarize_database"
    )
  )
  
  # Add the document_all_databases function
  registry$functions$document_all_databases <- list(
    name = "document_all_databases",
    description = "Generate documentation for all project databases",
    parameters = list(
      app_db_path = list(
        type = "string",
        description = "Path to app database",
        default = "app_data.duckdb"
      ),
      raw_db_path = list(
        type = "string",
        description = "Path to raw data database",
        default = "data/raw_data.duckdb"
      ),
      output_dir = list(
        type = "string",
        description = "Directory for output files",
        default = "docs/database"
      )
    ),
    return_type = "boolean",
    implementation = list(
      rsql = "document_all_databases"
    )
  )
  
  return(registry)
}

#' Register translation rules for database documentation
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_translation_rules <- function(registry) {
  # Add translation rules as needed
  registry$translation_rules$database_documentation <- list(
    pattern = "summarize (database|db) {path}",
    targets = list(
      rsql = "summarize_database(db_connection = \"{path}\")"
    )
  )
  
  return(registry)
}