#' @file fn_generate_create_table_query.R
#' @use DBI
#' @use duckdb
#' @requires fn_print_query.R
#' @requires 14_sql_utils/fn_quote_identifier.R
#' @principle MP058 Database Table Creation Strategy
#' @principle R092 Universal DBI Approach
#' @principle R067 Functional Encapsulation
#' @principle P080 Integer ID Consistency
#' @principle R0112 Comment-Code Spacing Rule
#' @principle R0103 Dependency-Based Sourcing
#' @author Claude
#' @date 2025-04-15
#' @modified 2025-04-16
#' @related_to fn_initialize_app_database.R

#' Generate CREATE TABLE SQL Query with Support for All DuckDB Constraints
#'
#' Creates a SQL query to define a table schema, with support for all DuckDB constraints:
#' - PRIMARY KEY (single column or composite)
#' - NOT NULL
#' - UNIQUE
#' - CHECK (arbitrary boolean expressions)
#' - DEFAULT (literal values)
#' - REFERENCES (foreign keys with optional ON DELETE/UPDATE actions)
#' - COLLATE (collation name for text ordering)
#' 
#' This implements the fixed schema strategy from MP058, ensuring explicit table definitions
#' with proper constraints and relationships. The function can output either SQL or R code.
#'
#' @param con Database connection object (DBI-compatible)
#' @param source_table Name of the source table to extract schema from, or NULL if creating from scratch
#' @param target_table Name of the target table to create. If NULL and source_table is provided,
#'        the source_table name will be used as the target_table name.
#' @param column_defs Optional list of column definitions to use instead of extracting from source.
#'        Each element should be a list with name, type, and optional constraints:
#'        - not_null: TRUE/FALSE - adds NOT NULL constraint
#'        - default: value - adds DEFAULT constraint with provided value
#'        - check: expression - adds CHECK constraint with provided boolean expression
#'        - unique: TRUE/FALSE - adds UNIQUE constraint
#'        - collate: name - adds COLLATE constraint with provided collation name
#'        - generated_as: expression - adds GENERATED ALWAYS AS (expression) 
#'        - generated_type: "VIRTUAL" or "STORED" - storage type for generated columns (defaults to VIRTUAL)
#' @param primary_key Character vector of column names that form the primary key
#'        If multiple columns, creates a composite primary key
#' @param foreign_keys List of foreign key definitions, each with columns, ref_table, and ref_columns
#' @param indexes List of indexes to create, each with columns and optionally unique=TRUE.
#'        Index names will be generated in the format idx_{table}_{column(s)}_{table}
#'        to ensure uniqueness across different tables with similar column names.
#' @param output_format Character string specifying output format, either "sql" (default) or "r"
#'        When "r" is specified, returns R code that would generate the table
#' @param connection_name Optional character string specifying the name to use for the connection
#'        variable in the R code output. If not provided, tries to detect the connection variable name
#'        from the function call.
#' @param or_replace Logical, whether to include "OR REPLACE" in the CREATE TABLE statement (defaults to FALSE)
#' @param temp Logical, whether to create a temporary table (defaults to FALSE)
#' @param if_not_exists Logical, whether to include "IF NOT EXISTS" in the CREATE TABLE statement (defaults to FALSE)
#' @return Either formatted SQL CREATE TABLE query string (when output_format="sql")
#'         or R code to generate the table (when output_format="r")
#' @export
#'
#' @examples
#' # Generate create table query with all constraint types
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' query <- generate_create_table_query(
#'   con = con,
#'   source_table = NULL,
#'   target_table = "customers",
#'   column_defs = list(
#'     list(name = "id", type = "INTEGER", not_null = TRUE),
#'     list(name = "name", type = "VARCHAR", not_null = TRUE, 
#'          check = "length(name) > 2"),
#'     list(name = "email", type = "VARCHAR", unique = TRUE,
#'          check = "contains(email, '@')"),
#'     list(name = "status", type = "VARCHAR", default = "'active'",
#'          check = "status IN ('active', 'inactive', 'pending')"),
#'     list(name = "created_at", type = "TIMESTAMP", 
#'          default = "CURRENT_TIMESTAMP"),
#'     list(name = "country_code", type = "CHAR(2)", not_null = TRUE,
#'          collate = "NOCASE"),
#'     list(name = "credit_limit", type = "DECIMAL(10,2)", 
#'          default = "1000.00", check = "credit_limit >= 0")
#'   ),
#'   primary_key = "id",
#'   indexes = list(
#'     list(columns = "email"),
#'     list(columns = c("name", "country_code"), unique = TRUE)
#'   )
#' )
#' cat(query)
#' DBI::dbDisconnect(con)
#'
#' # Example with composite primary key and foreign keys
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' query <- generate_create_table_query(
#'   con = con,
#'   source_table = NULL,
#'   target_table = "sales_by_customer_product",
#'   column_defs = list(
#'     list(name = "customer_id", type = "INTEGER", not_null = TRUE),
#'     list(name = "product_id", type = "INTEGER", not_null = TRUE),
#'     list(name = "order_date", type = "DATE", not_null = TRUE),
#'     list(name = "quantity", type = "INTEGER", not_null = TRUE,
#'          check = "quantity > 0"),
#'     list(name = "amount", type = "DECIMAL(10,2)", not_null = TRUE,
#'          check = "amount >= 0")
#'   ),
#'   primary_key = c("customer_id", "product_id", "order_date"),
#'   foreign_keys = list(
#'     list(
#'       columns = "customer_id", 
#'       ref_table = "customers", 
#'       ref_columns = "id",
#'       on_delete = "CASCADE"
#'     ),
#'     list(
#'       columns = "product_id", 
#'       ref_table = "products", 
#'       ref_columns = "id",
#'       on_update = "CASCADE"
#'     )
#'   ),
#'   indexes = list(
#'     list(columns = "order_date"),
#'     list(columns = c("customer_id", "order_date"), unique = TRUE)
#'   )
#' )
#' cat(query)
#' DBI::dbDisconnect(con)
#'
#' # From existing table with a new name
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbExecute(con, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
#' query <- generate_create_table_query(con, "test", "test_copy")
#' cat(query)
#' DBI::dbDisconnect(con)
#'
#' # Generate SQL for an existing table (using same name)
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbExecute(con, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)")
#' query <- generate_create_table_query(con, "customers")
#' cat(query)
#' 
#' # Generate R code for an existing table 
#' r_code <- generate_create_table_query(con, "customers", output_format = "r")
#' cat(r_code)
#'
#' # Using a different connection variable name
#' my_connection <- con
#' r_code_alt <- generate_create_table_query(my_connection, "customers", output_format = "r")
#' cat("\n\n# With detected connection variable name:\n")
#' cat(r_code_alt)
#'
#' # Explicitly setting connection name
#' r_code_explicit <- generate_create_table_query(con, "customers", 
#'                                              output_format = "r",
#'                                              connection_name = "db_connection")
#' cat("\n\n# With explicit connection_name parameter:\n")
#' cat(r_code_explicit)
#'
#' DBI::dbDisconnect(con)
#'
#' VALIDATION {
#'   REQUIRE: inherits(con, "DBIConnection")
#'   REQUIRE: is.null(source_table) || (is.character(source_table) && length(source_table) == 1)
#'   REQUIRE: is.null(target_table) || (is.character(target_table) && length(target_table) == 1)
#'   REQUIRE: !is.null(source_table) || !is.null(target_table) # Either source or target must be provided
#'   REQUIRE: is.null(column_defs) || is.list(column_defs)
#'   # Check column_defs structure if provided
#'   IF: !is.null(column_defs) {
#'     REQUIRE: all(sapply(column_defs, function(x) !is.null(x$name) && is.character(x$name)))
#'     REQUIRE: all(sapply(column_defs, function(x) !is.null(x$type) && is.character(x$type)))
#'     # Check optional constraint fields if present
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$not_null) || is.logical(x$not_null)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$unique) || is.logical(x$unique)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$check) || is.character(x$check)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$default) || is.character(x$default)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$collate) || is.character(x$collate)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$generated_as) || is.character(x$generated_as)))
#'     REQUIRE: all(sapply(column_defs, function(x) is.null(x$generated_type) || 
#'                                             (is.character(x$generated_type) && 
#'                                              toupper(x$generated_type) %in% c("VIRTUAL", "STORED"))))
#'   }
#'   REQUIRE: is.null(primary_key) || is.character(primary_key)
#'   REQUIRE: is.null(foreign_keys) || is.list(foreign_keys)
#'   # Check foreign_keys structure if provided
#'   IF: !is.null(foreign_keys) {
#'     REQUIRE: all(sapply(foreign_keys, function(x) !is.null(x$columns) && 
#'                                                  !is.null(x$ref_table) && 
#'                                                  !is.null(x$ref_columns)))
#'     REQUIRE: all(sapply(foreign_keys, function(x) is.null(x$on_delete) || 
#'                                                  (is.character(x$on_delete) && 
#'                                                   x$on_delete %in% c("CASCADE", "RESTRICT", 
#'                                                                     "SET NULL", "SET DEFAULT", "NO ACTION"))))
#'     REQUIRE: all(sapply(foreign_keys, function(x) is.null(x$on_update) || 
#'                                                  (is.character(x$on_update) && 
#'                                                   x$on_update %in% c("CASCADE", "RESTRICT", 
#'                                                                     "SET NULL", "SET DEFAULT", "NO ACTION"))))
#'   }
#'   REQUIRE: is.null(indexes) || is.list(indexes)
#'   # Check indexes structure if provided
#'   IF: !is.null(indexes) {
#'     REQUIRE: all(sapply(indexes, function(x) !is.null(x$columns)))
#'     REQUIRE: all(sapply(indexes, function(x) is.null(x$unique) || is.logical(x$unique)))
#'   }
#'   REQUIRE: is.character(output_format) && output_format %in% c("sql", "r")
#'   REQUIRE: is.null(connection_name) || is.character(connection_name)
#'   REQUIRE: is.null(schema) || is.character(schema)
#'   REQUIRE: is.logical(execute)
#'   REQUIRE: is.logical(or_replace)
#'   REQUIRE: is.logical(temp)
#'   REQUIRE: is.logical(if_not_exists)
#' }
#'


generate_create_table_query <- function(con, source_table = NULL, target_table = NULL,
                                        or_replace = FALSE, temp = FALSE, if_not_exists = FALSE,
                                        schema = NULL,
                                        column_defs = NULL,
                                        primary_key = NULL, foreign_keys = NULL, indexes = NULL,
                                        output_format = "sql",
                                        connection_name = NULL,
                                        execute = FALSE) {
  # Validate inputs
  if (!inherits(con, "DBIConnection")) {
    stop("Connection must be a DBI connection object")
  }
  if (!is.null(source_table) && (!is.character(source_table) || length(source_table) != 1)) {
    stop("source_table must be NULL or a single character string")
  }
  
  # If target_table is NULL but source_table is provided, use source_table as target_table
  if (is.null(target_table) && !is.null(source_table)) {
    target_table <- source_table
    message("Using source_table as target_table: ", target_table)
  } else if (is.null(target_table)) {
    stop("target_table must be provided if source_table is NULL")
  }
  
  # Validate target_table
  if (!is.character(target_table) || length(target_table) != 1) {
    stop("target_table must be a single character string")
  }

  # Quote table name and apply schema if provided
  full_table_name <- quote_identifier(target_table)
  if (!is.null(schema)) {
    full_table_name <- paste0(quote_identifier(schema), ".", full_table_name)
  }
  
  # Validate output_format
  if (!is.character(output_format) || length(output_format) != 1 || 
      !(output_format %in% c("sql", "r"))) {
    stop("output_format must be either 'sql' or 'r'")
  }
  
  # Initialize variables
  formatted_column_defs <- character()
  primary_key_cols <- character()
  table_constraints <- character()
  index_queries <- character()
  
  # Process based on source type
  if (is.null(column_defs) && !is.null(source_table)) {
    # Get structure from existing table
    table_info <- DBI::dbGetQuery(con, paste0("PRAGMA table_info('", source_table, "');"))
    
    # Check if table exists
    if (nrow(table_info) == 0) {
      stop("Source table '", source_table, "' does not exist")
    }
    
    # Extract column names, types, and constraints
    columns <- table_info$name
    types <- table_info$type
    not_null <- table_info$notnull
    existing_primary_key <- table_info$pk
    
    # Build column definitions
    for (i in seq_along(columns)) {
      # Format column definition with constraints
      col_def <- paste0(quote_identifier(columns[i]), " ", types[i])
      
      # Add NOT NULL constraint if applicable
      if (not_null[i] == 1) {
        col_def <- paste0(col_def, " NOT NULL")
      }
      
      # Check for primary key
      if (existing_primary_key[i] > 0) {
        if (is.null(primary_key)) {
          # Only add PRIMARY KEY inline if it's not part of a composite key and no explicit primary key provided
          if (sum(existing_primary_key > 0) == 1) {
            col_def <- paste0(col_def, " PRIMARY KEY")
          }
          primary_key_cols <- c(primary_key_cols, columns[i])
        }
      }
      
      formatted_column_defs <- c(formatted_column_defs, col_def)
    }
    
    # If primary key not explicitly provided but exists in source table
    if (is.null(primary_key) && length(primary_key_cols) > 0) {
      primary_key <- primary_key_cols
    }
  } else if (!is.null(column_defs)) {
    # Use provided column definitions
    for (col in column_defs) {
      # Check if it's a generated column
      if (!is.null(col$generated_as)) {
        # Basic column definition with generated column syntax
        col_def <- paste0(quote_identifier(col$name))
        
        # Add type if specified
        if (!is.null(col$type)) {
          col_def <- paste0(col_def, " ", col$type)
        }
        
        # Add GENERATED ALWAYS AS clause
        col_def <- paste0(col_def, " GENERATED ALWAYS AS (", col$generated_as, ")")
        
        # Add storage type for generated column (VIRTUAL or STORED)
        if (!is.null(col$generated_type) && toupper(col$generated_type) == "STORED") {
          col_def <- paste0(col_def, " STORED")
        } else {
          # VIRTUAL is the default
          col_def <- paste0(col_def, " VIRTUAL")
        }
      } else {
        # Regular column (not generated)
        # Basic column definition
        col_def <- paste0(quote_identifier(col$name), " ", col$type)
        
        # Add constraints
        if (!is.null(col$not_null) && col$not_null) {
          col_def <- paste0(col_def, " NOT NULL")
        }
        
        # Add default if specified
        if (!is.null(col$default)) {
          col_def <- paste0(col_def, " DEFAULT ", col$default)
        }
        
        # Add check constraints if specified
        if (!is.null(col$check)) {
          col_def <- paste0(col_def, " CHECK (", col$check, ")")
        }
        
        # Add unique constraint if specified
        if (!is.null(col$unique) && col$unique) {
          col_def <- paste0(col_def, " UNIQUE")
        }
        
        # Add collate constraint if specified
        if (!is.null(col$collate)) {
          col_def <- paste0(col_def, " COLLATE ", col$collate)
        }
        
        # Only add PRIMARY KEY inline if it's a single-column primary key
        if (!is.null(primary_key) && length(primary_key) == 1 && col$name == primary_key) {
          col_def <- paste0(col_def, " PRIMARY KEY")
        }
      }
      
      formatted_column_defs <- c(formatted_column_defs, col_def)
    }
  } else {
    stop("Either source_table or column_defs must be provided")
  }
  
  # Add composite primary key constraint if needed
  if (!is.null(primary_key) && length(primary_key) > 1) {
    pk_quoted <- paste(sapply(primary_key, quote_identifier), collapse = ", ")
    primary_key_constraint <- paste0("PRIMARY KEY (", pk_quoted, ")")
    table_constraints <- c(table_constraints, primary_key_constraint)
  }
  
  # Add foreign key constraints
  if (!is.null(foreign_keys)) {
    for (fk in foreign_keys) {
      # Validate foreign key definition
      if (is.null(fk$columns) || is.null(fk$ref_table) || is.null(fk$ref_columns)) {
        stop("Foreign key definition must include columns, ref_table, and ref_columns")
      }
      
      # Format foreign key constraint
      fk_constraint <- paste0(
        "FOREIGN KEY ",
        paste0("(", paste(sapply(fk$columns, quote_identifier), collapse = ", "), ")"),
        " REFERENCES ", quote_identifier(fk$ref_table), " ",
        paste0("(", paste(sapply(fk$ref_columns, quote_identifier), collapse = ", "), ")"),
        if (!is.null(fk$on_delete)) paste0(" ON DELETE ", fk$on_delete) else "",
        if (!is.null(fk$on_update)) paste0(" ON UPDATE ", fk$on_update) else ""
      )
      
      table_constraints <- c(table_constraints, fk_constraint)
    }
  }
  
  # Build the full column definition string
  all_elements <- c(formatted_column_defs, table_constraints)
  column_string <- paste(all_elements, collapse = ",\n  ")
  
  # Build the CREATE TABLE statement with modifiers
  create_query <- "CREATE"
  
  # Add OR REPLACE if requested
  if (or_replace) {
    create_query <- paste0(create_query, " OR REPLACE")
  }
  
  # Add TEMP/TEMPORARY if requested
  if (temp) {
    create_query <- paste0(create_query, " TEMPORARY")
  }
  
  # Add TABLE and IF NOT EXISTS if requested
  create_query <- paste0(create_query, " TABLE")
  
  if (if_not_exists) {
    create_query <- paste0(create_query, " IF NOT EXISTS")
  }
  
  # Complete the CREATE TABLE statement with proper formatting
  create_table_query <- paste0(
    create_query, " ", full_table_name, " (\n  ",
    column_string,
    "\n);"
  )
  
  # Add indexes
  # Skip creating index on primary key columns since databases automatically create 
  # an index for primary keys - this avoids redundant indexes
  
  # Then process custom indexes
  if (!is.null(indexes)) {
    for (idx in indexes) {
      # Validate index definition
      if (is.null(idx$columns)) {
        stop("Index definition must include columns")
      }
      
      # Skip indexes that match exactly the primary key columns 
      # (as databases automatically create an index for the primary key)
      is_redundant_with_pk <- FALSE
      if (!is.null(primary_key)) {
        # Check if index columns are exactly the same as primary key columns (in any order)
        idx_cols_set <- sort(idx$columns)
        pk_cols_set <- sort(primary_key)
        
        if (length(idx_cols_set) == length(pk_cols_set) && 
            all(idx_cols_set == pk_cols_set)) {
          is_redundant_with_pk <- TRUE
          message("Skipping redundant index on primary key columns: ", 
                  paste(idx$columns, collapse = ", "))
        }
      }
      
      # Only create the index if it's not redundant with the primary key
      if (!is_redundant_with_pk) {
        # Format index columns
        idx_columns <- if (is.character(idx$columns) && length(idx$columns) > 1) {
          paste(sapply(idx$columns, quote_identifier), collapse = ", ")
        } else {
          quote_identifier(idx$columns)
        }
        
        # Create index name that includes the target table name to avoid conflicts
        # when the same column name exists in multiple tables
        idx_name <- paste0(
          "idx_", target_table, "_",
          paste(gsub("[^a-zA-Z0-9]", "_", idx$columns), collapse = "_"),
          "_", target_table
        )
        
        # Determine if unique
        unique_flag <- if (!is.null(idx$unique) && idx$unique) "UNIQUE " else ""
        
        # Create index statement
        index_query <- paste0(
          "\n\nCREATE ", unique_flag, "INDEX IF NOT EXISTS ", quote_identifier(idx_name),
          " ON ", full_table_name, "(", paste(sapply(idx$columns, quote_identifier), collapse = ", "), ");"
        )
        
        index_queries <- c(index_queries, index_query)
      }
    }
  }
  
  # Combine CREATE TABLE with any INDEX statements
  formatted_query <- paste0(create_table_query, paste(index_queries, collapse = ""))

  if (execute && output_format == "sql") {
    DBI::dbExecute(con, formatted_query)
  }
  
  # If SQL format requested, return the SQL query
  if (output_format == "sql") {
    return(formatted_query)
  }
  
  # Otherwise, generate R code format
  if (output_format == "r") {
    # Get connection variable name if specified, detect from call, or use generic "connection"
    con_name <- if (!is.null(connection_name)) {
      connection_name
    } else {
      tryCatch({
        con_name <- deparse(substitute(con))
        if (con_name == "." || con_name == "") { 
          "connection" 
        } else { 
          con_name 
        }
      }, error = function(e) {
        "connection"
      })
    }
    
    # Start building the R code
    r_code <- paste0('generate_create_table_query(\n',
                   '  con = ', con_name, ',\n',
                   '  target_table = "', target_table, '",\n',
                   '  source_table = NULL,\n')
    if (!is.null(schema)) {
      r_code <- paste0(r_code, '  schema = "', schema, '",\n')
    }
    
    # When using a source table, we need to extract the actual column definitions
    # rather than just referencing the source table again
    
    # Create column_defs from source_table if needed
    extracted_column_defs <- column_defs
    extracted_foreign_keys <- foreign_keys
    extracted_indexes <- indexes
    
    if (is.null(extracted_column_defs) && !is.null(source_table)) {
      # Get structure from existing table
      table_info <- tryCatch({
        DBI::dbGetQuery(con, paste0("PRAGMA table_info('", source_table, "');"))
      }, error = function(e) {
        # For databases that don't support PRAGMA, try to get info another way
        # Return empty dataframe for now - in production would use other DB-specific methods
        warning("Could not extract column info: ", e$message)
        return(data.frame())
      })
      
      # Try to get foreign key info if possible
      fk_info <- tryCatch({
        DBI::dbGetQuery(con, paste0("PRAGMA foreign_key_list('", source_table, "');"))
      }, error = function(e) {
        # Silently fail for databases without this capability
        return(data.frame())
      })
      
      # Try to get index info if possible
      idx_info <- tryCatch({
        DBI::dbGetQuery(con, paste0("PRAGMA index_list('", source_table, "');"))
      }, error = function(e) {
        # Silently fail for databases without this capability
        return(data.frame())
      })
      
      if (nrow(table_info) > 0) {
        # Extract column names, types, and constraints
        extracted_column_defs <- list()
        
        for (i in 1:nrow(table_info)) {
          col_def <- list(
            name = table_info$name[i],
            type = table_info$type[i]
          )
          
          # Add NOT NULL constraint if applicable
          if (table_info$notnull[i] == 1) {
            col_def$not_null <- TRUE
          }
          
          # Add to the list
          extracted_column_defs[[i]] <- col_def
          
          # Check for primary key columns
          if ("pk" %in% names(table_info) && table_info$pk[i] > 0) {
            if (is.null(primary_key)) {
              primary_key <- character(0)
            }
            primary_key <- c(primary_key, table_info$name[i])
          }
        }
      }
      
      # Process foreign keys if available
      if (nrow(fk_info) > 0 && is.null(extracted_foreign_keys)) {
        extracted_foreign_keys <- list()
        
        # Group by id to handle multi-column foreign keys
        ids <- unique(fk_info$id)
        for (id in ids) {
          fk_cols <- fk_info[fk_info$id == id, ]
          
          # Extract columns for this foreign key constraint
          fk_columns <- fk_cols$from
          ref_columns <- fk_cols$to
          ref_table <- fk_cols$table[1]  # Should be the same for all rows with same id
          
          # Create foreign key definition
          fk_def <- list(
            columns = if (length(fk_columns) == 1) fk_columns else as.character(fk_columns),
            ref_table = ref_table,
            ref_columns = if (length(ref_columns) == 1) ref_columns else as.character(ref_columns)
          )
          
          # Add optional on_delete if available
          if ("on_delete" %in% names(fk_cols)) {
            fk_def$on_delete <- fk_cols$on_delete[1]
          }
          
          # Add optional on_update if available
          if ("on_update" %in% names(fk_cols)) {
            fk_def$on_update <- fk_cols$on_update[1]
          }
          
          extracted_foreign_keys[[length(extracted_foreign_keys) + 1]] <- fk_def
        }
      }
      
      # Process indexes if available
      if (nrow(idx_info) > 0 && is.null(extracted_indexes)) {
        extracted_indexes <- list()
        
        for (i in 1:nrow(idx_info)) {
          idx_name <- idx_info$name[i]
          
          # Get index details
          idx_detail <- tryCatch({
            DBI::dbGetQuery(con, paste0("PRAGMA index_info('", idx_name, "');"))
          }, error = function(e) {
            # Silently fail
            return(data.frame())
          })
          
          if (nrow(idx_detail) > 0) {
            # Extract column names
            idx_columns <- idx_detail$name
            
            # Create index definition
            idx_def <- list(
              columns = if (length(idx_columns) == 1) idx_columns else as.character(idx_columns)
            )
            
            # Add unique flag if this is a unique index
            if ("unique" %in% names(idx_info) && idx_info$unique[i] == 1) {
              idx_def$unique <- TRUE
            }
            
            extracted_indexes[[length(extracted_indexes) + 1]] <- idx_def
          }
        }
      }
    }
    
    # Add column definitions if available
    if (!is.null(extracted_column_defs)) {
      r_code <- paste0(r_code, '  column_defs = list(\n')
      col_defs_text <- character()
      
      for (i in seq_along(extracted_column_defs)) {
        col <- extracted_column_defs[[i]]
        col_text <- paste0('    list(name = "', col$name, '", type = "', col$type, '"')
        
        # Add optional fields
        if (!is.null(col$not_null) && col$not_null) {
          col_text <- paste0(col_text, ', not_null = TRUE')
        }
        if (!is.null(col$default)) {
          col_text <- paste0(col_text, ', default = "', col$default, '"')
        }
        if (!is.null(col$check)) {
          col_text <- paste0(col_text, ', check = "', col$check, '"')
        }
        if (!is.null(col$unique) && col$unique) {
          col_text <- paste0(col_text, ', unique = TRUE')
        }
        if (!is.null(col$collate)) {
          col_text <- paste0(col_text, ', collate = "', col$collate, '"')
        }
        # Handle generated columns
        if (!is.null(col$generated_as)) {
          col_text <- paste0(col_text, ', generated_as = "', col$generated_as, '"')
          if (!is.null(col$generated_type)) {
            col_text <- paste0(col_text, ', generated_type = "', col$generated_type, '"')
          }
        }
        
        col_text <- paste0(col_text, ')')
        if (i < length(extracted_column_defs)) {
          col_text <- paste0(col_text, ',')
        }
        col_defs_text <- c(col_defs_text, col_text)
      }
      
      r_code <- paste0(r_code, paste(col_defs_text, collapse = '\n'), '\n  ),\n')
    }
    
    # Add primary key if available
    if (!is.null(primary_key)) {
      if (length(primary_key) == 1) {
        r_code <- paste0(r_code, '  primary_key = "', primary_key, '",\n')
      } else {
        r_code <- paste0(r_code, '  primary_key = c("', paste(primary_key, collapse = '", "'), '"),\n')
      }
    }
    
    # Add foreign keys if available
    if (!is.null(extracted_foreign_keys) && length(extracted_foreign_keys) > 0) {
      r_code <- paste0(r_code, '  foreign_keys = list(\n')
      fk_texts <- character()
      
      for (i in seq_along(extracted_foreign_keys)) {
        fk <- extracted_foreign_keys[[i]]
        
        # Handle columns
        cols_text <- if (length(fk$columns) > 1) {
          paste0('c("', paste(fk$columns, collapse = '", "'), '")')
        } else {
          paste0('"', fk$columns, '"')
        }
        
        # Handle ref_columns
        ref_cols_text <- if (length(fk$ref_columns) > 1) {
          paste0('c("', paste(fk$ref_columns, collapse = '", "'), '")')
        } else {
          paste0('"', fk$ref_columns, '"')
        }
        
        fk_text <- paste0('    list(columns = ', cols_text, 
                         ', ref_table = "', fk$ref_table, 
                         '", ref_columns = ', ref_cols_text)
        
        # Add optional fields
        if (!is.null(fk$on_delete)) {
          fk_text <- paste0(fk_text, ', on_delete = "', fk$on_delete, '"')
        }
        if (!is.null(fk$on_update)) {
          fk_text <- paste0(fk_text, ', on_update = "', fk$on_update, '"')
        }
        
        fk_text <- paste0(fk_text, ')')
        if (i < length(extracted_foreign_keys)) {
          fk_text <- paste0(fk_text, ',')
        }
        
        fk_texts <- c(fk_texts, fk_text)
      }
      
      r_code <- paste0(r_code, paste(fk_texts, collapse = '\n'), '\n  ),\n')
    }
    
    # Add indexes if available
    if (!is.null(extracted_indexes) && length(extracted_indexes) > 0) {
      r_code <- paste0(r_code, '  indexes = list(\n')
      idx_texts <- character()
      
      for (i in seq_along(extracted_indexes)) {
        idx <- extracted_indexes[[i]]
        
        # Handle columns
        cols_text <- if (length(idx$columns) > 1) {
          paste0('c("', paste(idx$columns, collapse = '", "'), '")')
        } else {
          paste0('"', idx$columns, '"')
        }
        
        idx_text <- paste0('    list(columns = ', cols_text)
        
        # Add unique if TRUE
        if (!is.null(idx$unique) && idx$unique) {
          idx_text <- paste0(idx_text, ', unique = TRUE')
        }
        
        idx_text <- paste0(idx_text, ')')
        if (i < length(extracted_indexes)) {
          idx_text <- paste0(idx_text, ',')
        }
        
        idx_texts <- c(idx_texts, idx_text)
      }
      
      r_code <- paste0(r_code, paste(idx_texts, collapse = '\n'), '\n  ),\n')
    }
    

    # Add the creation modifiers if they differ from defaults
    if (or_replace) {
      r_code <- paste0(r_code, '  or_replace = TRUE,\n')
    }
    
    if (temp) {
      r_code <- paste0(r_code, '  temp = TRUE,\n')
    }
    
    if (if_not_exists) {
      r_code <- paste0(r_code, '  if_not_exists = TRUE,\n')
    }

    if (execute) {
      r_code <- paste0(r_code, '  execute = TRUE,\n')
    }

    # Close the function call
    r_code <- paste0(r_code, ')')
    
    return(r_code)
  }
}
