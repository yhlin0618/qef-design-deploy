#' NSQL Extension for Table Creation
#' 
#' This extension adds table creation and database structure documentation to NSQL.
#' It defines the CREATE syntax for specifying tables at database connections and in schemas.
#' 
#' See also:
#' - implementation_phrases.md - Contains CREATE-related implementation phrases
#' - implementation_phrase_registry.xlsx - Registry of implementation phrases
#' 
#' @export
#' @keywords NSQL extension

#' Register table creation extension with NSQL registry
#' 
#' @param registry The NSQL registry to extend
#' @return The updated registry
#' @export
register_extension <- function(registry) {
  # Add extension metadata
  registry$extensions$table_creation <- list(
    name = "Table Creation Extension",
    version = "1.0.0",
    description = "Extends NSQL with table creation syntax",
    author = "Claude",
    implements = "MP058" # Database Table Creation Strategy
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

#' Register statement patterns for table creation
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_statement_patterns <- function(registry) {
  # Add the CREATE table pattern
  registry$patterns$create_table <- list(
    syntax = "CREATE {table_name} AT {connection}[.{schema}] === {sql_definition} ===",
    description = "Create a new table at a specific database connection and schema",
    translation = function(ast, target) {
      # Parse the connection and schema
      conn_parts <- strsplit(ast$connection, "\\.")[[1]]
      connection <- conn_parts[1]
      schema <- if (length(conn_parts) > 1) conn_parts[2] else NULL
      
      # Extract the SQL definition
      sql_def <- ast$sql_definition
      
      # Generate R code
      if (target == "rsql") {
        # Basic connection reference
        conn_ref <- connection
        
        # Add schema if specified
        schema_param <- if (!is.null(schema)) {
          sprintf('schema = "%s", ', schema)
        } else {
          ""
        }
        
        # Parse the SQL to extract parameters for generate_create_table_query
        # This would require a complex SQL parser for production
        # For demonstration, we'll return a simple execute_sql call
        
        return(sprintf(
          'execute_sql(con = %s, %ssql = "%s")',
          conn_ref,
          schema_param,
          gsub('"', '\\\\"', sql_def) # Escape quotes in SQL
        ))
      } else if (target == "sql") {
        # For SQL target, just return the SQL definition
        return(sql_def)
      } else {
        return(NULL)  # Unsupported target
      }
    }
  )
  
  return(registry)
}

#' Register table creation functions
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_functions <- function(registry) {
  # Add the execute_sql function
  registry$functions$execute_sql <- list(
    name = "execute_sql",
    description = "Execute SQL statements at a database connection",
    parameters = list(
      con = list(
        type = "connection",
        description = "Database connection object"
      ),
      schema = list(
        type = "string",
        description = "Schema to use (optional)",
        required = FALSE
      ),
      sql = list(
        type = "string",
        description = "SQL statement to execute"
      )
    ),
    return_type = "boolean",
    implementation = list(
      rsql = "execute_sql"
    )
  )
  
  return(registry)
}

#' Register translation rules for table creation
#' 
#' @param registry The NSQL registry
#' @return The updated registry
register_translation_rules <- function(registry) {
  # Add translation rule for CREATE TABLE to generate_create_table_query
  registry$translation_rules$create_table_to_r <- list(
    pattern = "CREATE TABLE {table}\\s*\\((.*?)\\)",
    targets = list(
      rsql = function(match) {
        # In a real implementation, this would parse the SQL CREATE TABLE
        # statement and generate the appropriate generate_create_table_query call
        # with column_defs, primary_key, etc.
        
        # For demonstration, we'll return a simplified version
        return(sprintf(
          'generate_create_table_query(con = connection, target_table = "%s", ...)',
          match$table
        ))
      }
    )
  )
  
  return(registry)
}

#' Parse NSQL CREATE statement
#' 
#' @param nsql_statement String containing the NSQL CREATE statement
#' @return A list with parsed components
#' @export
parse_create_statement <- function(nsql_statement) {
  # Extract the intent line (CREATE table AT connection)
  intent_pattern <- "CREATE\\s+(.+?)\\s+AT\\s+(.+?)\\s*\n===\\s*"
  intent_match <- regexpr(intent_pattern, nsql_statement, perl = TRUE)
  
  if (intent_match == -1) {
    return(NULL) # Invalid format
  }
  
  intent_text <- regmatches(nsql_statement, intent_match)
  
  # Extract table name and connection
  table_pattern <- "CREATE\\s+(.+?)\\s+AT\\s+(.+?)\\s*\n==="
  table_match <- regexec(table_pattern, intent_text, perl = TRUE)
  
  if (length(table_match[[1]]) < 3) {
    return(NULL) # Couldn't extract components
  }
  
  # Get the matches
  matches <- regmatches(intent_text, table_match)[[1]]
  table_name <- matches[2]
  connection_info <- matches[3]
  
  # Parse connection and schema
  conn_parts <- strsplit(connection_info, "\\.")[[1]]
  connection <- conn_parts[1]
  schema <- if (length(conn_parts) > 1) conn_parts[2] else NULL
  
  # Extract SQL definition
  sql_pattern <- "===\\s*(.+?)\\s*===$"
  sql_match <- regexec(sql_pattern, nsql_statement, perl = TRUE, dotall = TRUE)
  
  sql_definition <- NULL
  if (length(sql_match[[1]]) >= 2) {
    sql_definition <- regmatches(nsql_statement, sql_match)[[1]][2]
  }
  
  # Return parsed components
  return(list(
    table_name = table_name,
    connection = connection,
    schema = schema,
    sql_definition = sql_definition
  ))
}

#' Generate NSQL CREATE statement
#' 
#' @param table_name Name of the table to create
#' @param connection Database connection name
#' @param schema Optional schema name
#' @param sql_definition SQL definition for the table
#' @return A properly formatted NSQL CREATE statement
#' @export
generate_create_statement <- function(table_name, connection, schema = NULL, sql_definition) {
  # Build the connection string
  conn_string <- connection
  if (!is.null(schema)) {
    conn_string <- paste0(connection, ".", schema)
  }
  
  # Build the full NSQL statement
  nsql_statement <- sprintf(
    "CREATE %s AT %s\n===\n%s\n===",
    table_name,
    conn_string,
    sql_definition
  )
  
  return(nsql_statement)
}