# Function to create or replace a database table with proper column definitions
create_summary_table <- function(db_connection,
                                 table_name,
                                 column_definitions,
                                 primary_keys=NULL, foreign_keys = NULL, indexes = NULL) {
  # Process each column definition to check if field names need quoting and replacement
  column_definitions <- sapply(column_definitions, process_column_def)
  
  # Concatenate column definitions
  column_defs <- paste(column_definitions, collapse = ",\n    ")
  
  # Concatenate primary key definition, remember to transform primary key fields too
  primary_key_def <- if (!is.null(primary_keys)) {
    paste0(",\n    PRIMARY KEY (", paste(sapply(primary_keys, function(x) quote_identifier(x)), collapse = ", "), ")")
  } else {
    ""
  }
  
  # Concatenate foreign key definitions
  foreign_key_defs <- if (!is.null(foreign_keys)) {
    paste(
      sapply(foreign_keys, function(fk) {
        paste0("FOREIGN KEY (", quote_identifier(fk$column), 
               ") REFERENCES ", fk$reference_table, "(", quote_identifier(fk$reference_column), ")")
      }),
      collapse = ",\n    "
    )
  } else {
    ""
  }
  
  # Concatenate index definitions (not included in the table, but as separate statements)
  index_defs <- if (!is.null(indexes)) {
    sapply(indexes, function(index) {
      paste0("CREATE INDEX ", index$name, " ON ", table_name, " (", paste(sapply(index$columns, quote_identifier), collapse = ", "), ");")
    })
  } else {
    NULL
  }
  
  # Dynamically generate CREATE TABLE SQL statement
  sql_query <- paste0(
    "DROP TABLE IF EXISTS ", table_name, ";\n",
    "CREATE TABLE ", table_name, " (\n",
    "    ", column_defs,
    if (foreign_key_defs != "") paste0(",\n    ", foreign_key_defs) else "",
    primary_key_def,
    "\n);"
  )
  
  # Execute DROP TABLE and CREATE TABLE statements
  dbExecute(db_connection, sql_query)
  
  # Execute CREATE INDEX statements (if there are indexes)
  if (!is.null(index_defs)) {
    for (index_query in index_defs) {
      dbExecute(db_connection, index_query)
    }
  }
  
  return(cat(sql_query, "\n", paste(index_defs, collapse = "\n")))  # Return generated SQL for inspection
}