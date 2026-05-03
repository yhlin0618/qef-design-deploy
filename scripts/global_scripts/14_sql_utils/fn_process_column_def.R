# Function to process column definition strings
process_column_def <- function(def_str) {
  # Get column name before first space
  parts <- strsplit(def_str, "\\s+", perl = TRUE)[[1]]
  col_name <- parts[1]
  # Replace hyphens with underscores, then quote the identifier if needed
  quoted_name <- quote_identifier(col_name)
  # Recombine the definition string
  paste(c(quoted_name, parts[-1]), collapse = " ")
}