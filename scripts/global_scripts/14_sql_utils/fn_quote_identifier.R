# Function to properly quote database identifiers as needed
quote_identifier <- function(identifier) {
  # First convert hyphens to underscores
  identifier <- sanitize_identifier(identifier)
  # If identifier contains non-alphanumeric characters or underscores, quote it
  if (grepl("[^A-Za-z0-9_]", identifier)) {
    return(paste0("\"", identifier, "\""))
  } else {
    return(identifier)
  }
}