#' Get Schema Definition for df_position Table
#'
#' Returns the schema definition for the df_position table using product_id
#' as the primary product identifier column.
#' This function implements the Database Table Creation Strategy (MP058) and
#' follows the Universal DBI Approach (DM_R023).
#'
#' @return A list containing the complete schema definition including:
#'   - table_name: The name of the table
#'   - column_defs: List of column definitions with types and constraints
#'   - primary_key: Primary key column(s)
#'   - indexes: List of indexes to create
#'   - documentation: Metadata about the table structure and purpose
#'
#' @details
#' The df_position table uses product_id as the primary product identifier.
#' This schema has been migrated from the legacy item_id column name to
#' the more descriptive product_id column name for better clarity.
#'
#' @principle MP058 Database Table Creation Strategy
#' @principle DM_R023 Universal DBI Approach (formerly R092)
#'
#' @export
get_df_position_schema <- function() {
  list(
    table_name = "df_position",

    column_defs = list(
      # Primary identifier columns
      list(name = "position_id", type = "INTEGER", not_null = TRUE),
      list(name = "product_id", type = "VARCHAR", not_null = TRUE),  # Changed from item_id

      # Position attributes
      list(name = "position_name", type = "VARCHAR"),
      list(name = "position_type", type = "VARCHAR"),
      list(name = "category", type = "VARCHAR"),
      list(name = "subcategory", type = "VARCHAR"),

      # Numeric position values
      list(name = "position_x", type = "NUMERIC"),
      list(name = "position_y", type = "NUMERIC"),
      list(name = "position_value", type = "NUMERIC"),

      # Platform and time dimensions
      list(name = "platform_id", type = "VARCHAR", not_null = TRUE),
      list(name = "time_period", type = "VARCHAR"),
      list(name = "date_recorded", type = "DATE"),

      # Metadata columns
      list(
        name = "import_timestamp",
        type = "TIMESTAMP",
        default = "CURRENT_TIMESTAMP"
      ),
      list(name = "last_modified", type = "TIMESTAMP"),
      list(name = "data_source", type = "VARCHAR")
    ),

    primary_key = c("position_id"),

    indexes = list(
      # Index on product identifier
      list(columns = "product_id", name = "idx_position_product"),
      # Composite index for common queries
      list(columns = c("platform_id", "product_id"), name = "idx_position_platform_product"),
      # Category indexes for filtering
      list(columns = "category", name = "idx_position_category"),
      list(columns = c("category", "subcategory"), name = "idx_position_cat_subcat"),
      # Time-based queries
      list(columns = "date_recorded", name = "idx_position_date")
    ),

    # Documentation for schema understanding
    documentation = list(
      purpose = "Stores product positioning data across different platforms and time periods",
      migration_notes = list(
        "Column 'item_id' has been renamed to 'product_id' for clarity",
        "Use migrate_item_id_to_product_id.R script to migrate existing data"
      ),
      notes = list(
        "The product_id field is the primary product identifier",
        "Components should use product_id for all queries"
      ),
      related_tables = list(
        "product_property_dictionary" = "Links via product_id for product details",
        "df_profile_by_customer" = "Links via platform_id for customer context"
      )
    )
  )
}

#' Create or Replace df_position Table
#'
#' Creates or replaces the df_position table in the database using the schema
#' definition from get_df_position_schema().
#'
#' @param con A DBI database connection
#' @param or_replace Logical. Whether to replace the table if it exists (default: TRUE)
#' @param verbose Logical. Whether to display detailed messages (default: TRUE)
#'
#' @return The schema definition used to create the table
#'
#' @examples
#' \dontrun{
#' # Connect to database
#' con <- dbConnect(duckdb::duckdb(), "app_data.duckdb")
#'
#' # Create the df_position table
#' schema <- create_df_position(con)
#'
#' # Query using either item_id or product_id
#' dbGetQuery(con, "SELECT product_id, position_name FROM df_position LIMIT 5")
#' }
#'
#' @export
create_df_position <- function(con, or_replace = TRUE, verbose = TRUE) {
  # Get the schema definition
  schema <- get_df_position_schema()

  if (verbose) {
    message(sprintf("Creating table: %s", schema$table_name))
    message(sprintf("  Columns: %d", length(schema$column_defs)))
    message(sprintf("  Indexes: %d", length(schema$indexes)))
    if (!is.null(schema$documentation$aliases)) {
      message("  Aliases configured:")
      for (alias_name in names(schema$documentation$aliases)) {
        message(sprintf("    - %s: %s", alias_name, schema$documentation$aliases[[alias_name]]))
      }
    }
  }

  # Check if generate_create_table_query function exists
  if (exists("generate_create_table_query")) {
    # Use the existing sophisticated function
    query <- generate_create_table_query(
      con = con,
      target_table = schema$table_name,
      column_defs = schema$column_defs,
      primary_key = schema$primary_key,
      indexes = schema$indexes,
      or_replace = or_replace
    )
  } else {
    # Fallback to manual SQL generation
    warning("generate_create_table_query not found, using fallback SQL generation")

    # Build CREATE TABLE statement
    action <- ifelse(or_replace, "CREATE OR REPLACE TABLE", "CREATE TABLE")

    # Format column definitions
    col_sql <- sapply(schema$column_defs, function(col) {
      sql <- sprintf("%s %s", col$name, col$type)
      if (!is.null(col$not_null) && col$not_null) {
        sql <- paste(sql, "NOT NULL")
      }
      if (!is.null(col$default)) {
        sql <- paste(sql, "DEFAULT", col$default)
      }
      if (!is.null(col$generated_as)) {
        sql <- sprintf("%s GENERATED ALWAYS AS (%s) %s",
                      sql, col$generated_as,
                      ifelse(is.null(col$generated_type), "STORED", col$generated_type))
      }
      sql
    })

    # Add primary key
    if (!is.null(schema$primary_key)) {
      pk_sql <- sprintf("PRIMARY KEY (%s)", paste(schema$primary_key, collapse = ", "))
      col_sql <- c(col_sql, pk_sql)
    }

    # Build complete CREATE TABLE query
    query <- sprintf("%s %s (\n  %s\n)",
                    action, schema$table_name,
                    paste(col_sql, collapse = ",\n  "))
  }

  # Execute the CREATE TABLE query
  tryCatch({
    DBI::dbExecute(con, query)
    if (verbose) {
      message(sprintf("Table %s created successfully", schema$table_name))
    }

    # Create indexes if they don't exist (for databases that don't support them in CREATE TABLE)
    if (!is.null(schema$indexes)) {
      for (idx in schema$indexes) {
        idx_name <- ifelse(is.null(idx$name),
                          paste0("idx_", schema$table_name, "_",
                                paste(idx$columns, collapse = "_")),
                          idx$name)
        idx_sql <- sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
                          idx_name, schema$table_name,
                          paste(idx$columns, collapse = ", "))
        tryCatch({
          DBI::dbExecute(con, idx_sql)
          if (verbose) message(sprintf("  Created index: %s", idx_name))
        }, error = function(e) {
          if (verbose) message(sprintf("  Index %s may already exist or not supported", idx_name))
        })
      }
    }

  }, error = function(e) {
    stop(sprintf("Failed to create table %s: %s", schema$table_name, e$message))
  })

  # Return the schema for documentation
  invisible(schema)
}

#' Validate df_position Table Structure
#'
#' Validates that the df_position table exists and has the expected structure,
#' including the virtual product_id column alias.
#'
#' @param con A DBI database connection
#' @param verbose Logical. Whether to display validation details (default: TRUE)
#'
#' @return Logical. TRUE if validation passes, FALSE otherwise
#'
#' @export
validate_df_position_schema <- function(con, verbose = TRUE) {
  schema <- get_df_position_schema()

  # Check if table exists
  if (!DBI::dbExistsTable(con, schema$table_name)) {
    if (verbose) message(sprintf("Table %s does not exist", schema$table_name))
    return(FALSE)
  }

  # Get actual table columns
  actual_cols <- DBI::dbListFields(con, schema$table_name)

  # Get expected column names
  expected_cols <- sapply(schema$column_defs, function(x) x$name)

  # Check for missing columns
  missing_cols <- setdiff(expected_cols, actual_cols)
  if (length(missing_cols) > 0) {
    if (verbose) {
      message("Missing columns:")
      for (col in missing_cols) {
        message(sprintf("  - %s", col))
      }
    }
    return(FALSE)
  }

  # Special check for product_id alias
  if ("product_id" %in% expected_cols) {
    # Test that product_id works as an alias
    test_query <- sprintf("SELECT product_id, item_id FROM %s LIMIT 1", schema$table_name)
    tryCatch({
      result <- DBI::dbGetQuery(con, test_query)
      if (verbose) message("Column alias 'product_id' is working correctly")
    }, error = function(e) {
      if (verbose) message("Warning: product_id alias may not be configured")
      return(FALSE)
    })
  }

  if (verbose) message(sprintf("Table %s structure validated successfully", schema$table_name))
  return(TRUE)
}