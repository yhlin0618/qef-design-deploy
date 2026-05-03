# Function to replace hyphens with underscores in database identifiers
sanitize_identifier <- function(identifier) {
  gsub("-", "_", identifier)
}