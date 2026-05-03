# Function to generate column definitions for SQL CREATE TABLE statements
generate_column_definitions <- function(columns, types) {
  if (length(columns) != length(types)) {
    stop("The length of columns and types must be the same.")
  }
  # Replace hyphens with underscores in all column names
  columns <- sapply(columns, sanitize_identifier)
  column_definitions <- paste(columns, types)
  return(column_definitions)
}