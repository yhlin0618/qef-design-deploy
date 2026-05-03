#' Generate a comprehensive database summary in Markdown format
#'
#' This function connects to a database, analyzes its structure, and outputs
#' a comprehensive report in Markdown format. It includes table metadata,
#' column information, and sample data.
#'
#' @param db_connection A DBI connection object, or path to a database file
#' @param output_file The path to the output Markdown file
#' @param include_samples Whether to include data samples (default: TRUE)
#' @param sample_rows Number of sample rows to include (default: 5)
#' @param connection_type Type of database connection ("duckdb", "sqlite", etc.)
#' @return Invisibly returns the generated markdown text
#' @export
summarize_database <- function(db_connection, 
                              output_file, 
                              include_samples = TRUE, 
                              sample_rows = 5,
                              connection_type = "duckdb") {
  
  # Handle connection - either use provided connection or create a new one
  close_conn <- FALSE
  if (is.character(db_connection)) {
    # It's a path, create a new connection
    if (connection_type == "duckdb") {
      if (!requireNamespace("duckdb", quietly = TRUE)) {
        stop("Package 'duckdb' is required to connect to DuckDB databases.")
      }
      conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_connection)
    } else if (connection_type == "sqlite") {
      if (!requireNamespace("RSQLite", quietly = TRUE)) {
        stop("Package 'RSQLite' is required to connect to SQLite databases.")
      }
      conn <- DBI::dbConnect(RSQLite::SQLite(), db_connection)
    } else {
      stop("Unsupported connection_type: ", connection_type)
    }
    close_conn <- TRUE
  } else {
    # It's already a connection object
    conn <- db_connection
  }
  
  # Ensure the connection is valid
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }
  
  # Ensure output directory exists
  output_dir <- dirname(output_file)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Begin generating the markdown content
  md_content <- c(
    "# Database Structure Summary",
    "",
    paste("*Generated on:*", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "## Overview",
    ""
  )
  
  # Try to get database info if available
  db_info <- tryCatch({
    if (connection_type == "duckdb") {
      version_info <- DBI::dbGetQuery(conn, "SELECT version()")
      paste("**Database Type:** DuckDB", version_info[1,1])
    } else if (connection_type == "sqlite") {
      version_info <- DBI::dbGetQuery(conn, "SELECT sqlite_version()")
      paste("**Database Type:** SQLite", version_info[1,1])
    } else {
      "**Database Type:** Unknown"
    }
  }, error = function(e) {
    "**Database Type:** Unknown"
  })
  
  md_content <- c(md_content, db_info, "")
  
  # List all tables
  tables <- DBI::dbListTables(conn)
  
  if (length(tables) == 0) {
    md_content <- c(
      md_content,
      "**No tables found in this database.**",
      ""
    )
  } else {
    md_content <- c(
      md_content,
      paste("This database contains", length(tables), "tables:"),
      "",
      "| Table Name | Row Count |",
      "|------------|-----------|"
    )
    
    # Get row counts for all tables
    for (table_name in tables) {
      tryCatch({
        row_count_query <- paste0("SELECT COUNT(*) AS count FROM \"", table_name, "\"")
        row_count <- DBI::dbGetQuery(conn, row_count_query)$count[1]
        md_content <- c(
          md_content,
          paste0("| ", table_name, " | ", format(row_count, big.mark = ","), " |")
        )
      }, error = function(e) {
        md_content <- c(
          md_content,
          paste0("| ", table_name, " | Error getting count: ", e$message, " |")
        )
      })
    }
    
    md_content <- c(md_content, "", "## Table Details", "")
    
    # Process each table
    for (table_name in tables) {
      md_content <- c(
        md_content,
        paste("### Table:", table_name),
        ""
      )
      
      # Get row count
      row_count <- tryCatch({
        row_count_query <- paste0("SELECT COUNT(*) AS count FROM \"", table_name, "\"")
        result <- DBI::dbGetQuery(conn, row_count_query)
        result$count[1]
      }, error = function(e) {
        message("Error getting row count for table ", table_name, ": ", e$message)
        return(NA)
      })
      
      if (!is.na(row_count)) {
        md_content <- c(
          md_content,
          paste("- **Row count:** ", format(row_count, big.mark = ",")),
          ""
        )
      } else {
        md_content <- c(
          md_content,
          "- **Row count:** Unable to determine",
          ""
        )
      }
      
      # Get table schema
      schema_result <- tryCatch({
        schema_query <- paste0("SELECT * FROM \"", table_name, "\" LIMIT 0")
        DBI::dbGetQuery(conn, schema_query)
      }, error = function(e) {
        message("Error getting schema for table ", table_name, ": ", e$message)
        return(NULL)
      })
      
      if (!is.null(schema_result)) {
        column_names <- names(schema_result)
        column_types <- sapply(schema_result, function(col) {
          class_val <- class(col)[1]
          if (class_val == "integer64") class_val <- "bigint"
          return(class_val)
        })
        
        md_content <- c(
          md_content,
          "#### Columns",
          "",
          "| Column Name | Data Type |",
          "|------------|-----------|"
        )
        
        for (i in seq_along(column_names)) {
          md_content <- c(
            md_content,
            paste0("| ", column_names[i], " | ", column_types[i], " |")
          )
        }
        
        md_content <- c(md_content, "")
        
        # Include data samples if requested and if there are rows
        if (include_samples && !is.na(row_count) && row_count > 0) {
          sample_data <- tryCatch({
            sample_query <- paste0("SELECT * FROM \"", table_name, "\" LIMIT ", sample_rows)
            DBI::dbGetQuery(conn, sample_query)
          }, error = function(e) {
            message("Error getting sample data for table ", table_name, ": ", e$message)
            return(NULL)
          })
          
          if (!is.null(sample_data) && nrow(sample_data) > 0) {
            md_content <- c(
              md_content,
              "#### Sample Data",
              ""
            )
            
            # Create header row
            header_row <- paste("|", paste(column_names, collapse = " | "), "|")
            separator_row <- paste("|", paste(rep("---", length(column_names)), collapse = " | "), "|")
            
            md_content <- c(md_content, header_row, separator_row)
            
            # Add data rows
            for (i in 1:nrow(sample_data)) {
              row_values <- sapply(1:ncol(sample_data), function(j) {
                value <- sample_data[i, j]
                if (is.null(value) || is.na(value)) {
                  "NULL"
                } else if (inherits(value, "Date") || inherits(value, "POSIXct")) {
                  as.character(value)
                } else if (is.numeric(value)) {
                  format(value, scientific = FALSE, big.mark = ",")
                } else {
                  # Truncate long text values to avoid breaking markdown tables
                  text <- as.character(value)
                  if (nchar(text) > 50) {
                    paste0(substr(text, 1, 47), "...")
                  } else {
                    text
                  }
                }
              })
              
              data_row <- paste("|", paste(row_values, collapse = " | "), "|")
              md_content <- c(md_content, data_row)
            }
            
            md_content <- c(md_content, "")
          } else {
            md_content <- c(
              md_content,
              "#### Sample Data",
              "",
              "*Unable to retrieve sample data*",
              ""
            )
          }
        }
      } else {
        md_content <- c(
          md_content,
          "#### Columns",
          "",
          "*Unable to retrieve column information*",
          ""
        )
      }
      
      # Add a separator between tables
      md_content <- c(md_content, "---", "")
    }
  }
  
  # Add summary footer
  md_content <- c(
    md_content,
    "## Summary",
    "",
    paste("- **Total tables:** ", length(tables)),
    paste("- **Documentation generated:** ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    paste("- **Generated by:** summarize_database() function following MP43 (Database Documentation Principle)")
  )
  
  # Write to file
  writeLines(md_content, output_file)
  message("Database summary written to: ", output_file)
  
  # Close connection if we opened it
  if (close_conn && DBI::dbIsValid(conn)) {
    DBI::dbDisconnect(conn)
  }
  
  # Return the content invisibly
  invisible(md_content)
}

#' Convenience function to summarize the app database
#'
#' @param db_path Path to the database file (defaults to "app_data.duckdb")
#' @param output_dir Directory to write the output file to (defaults to "docs/database")
#' @return Invisibly returns the generated markdown text
#' @export
summarize_app_database <- function(db_path = "app_data.duckdb", 
                                  output_dir = "docs/database") {
  
  # Build output file path
  output_file <- file.path(output_dir, "app_data_structure.md")
  
  # Call the main function
  summarize_database(
    db_connection = db_path,
    output_file = output_file,
    include_samples = TRUE,
    sample_rows = 5,
    connection_type = "duckdb"
  )
}

#' Document all project databases
#'
#' @param app_db_path Path to the app database (defaults to "app_data.duckdb")
#' @param raw_db_path Path to the raw data database (defaults to "data/raw_data.duckdb")
#' @param output_dir Directory to write the output files to (defaults to "docs/database")
#' @return Invisibly returns TRUE if successful
#' @export
document_all_databases <- function(app_db_path = "app_data.duckdb",
                                 raw_db_path = "data/raw_data.duckdb",
                                 output_dir = "docs/database") {
  
  # Check if dependencies are available
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required for database documentation.")
  }
  
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("Package 'duckdb' is required for DuckDB database documentation.")
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Document app database if it exists
  if (file.exists(app_db_path)) {
    app_output_file <- file.path(output_dir, "app_data_structure.md")
    message("Documenting application database: ", app_db_path)
    summarize_database(
      db_connection = app_db_path,
      output_file = app_output_file,
      connection_type = "duckdb"
    )
  } else {
    warning("Application database not found at: ", app_db_path)
  }
  
  # Document raw database if it exists
  if (file.exists(raw_db_path)) {
    raw_output_file <- file.path(output_dir, "raw_data_structure.md")
    message("Documenting raw data database: ", raw_db_path)
    summarize_database(
      db_connection = raw_db_path,
      output_file = raw_output_file,
      connection_type = "duckdb"
    )
  } else {
    message("Raw data database not found at: ", raw_db_path, " (this may be normal)")
  }
  
  message("Database documentation complete. Files written to: ", output_dir)
  invisible(TRUE)
}