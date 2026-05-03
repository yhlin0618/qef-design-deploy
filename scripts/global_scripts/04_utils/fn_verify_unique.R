#' Verify Name Uniqueness
#'
#' @description
#' Verifies that a proposed name is not already in use.
#'
#' @param new_name The name to check for uniqueness
#' @return TRUE if the name is available, FALSE if already in use
#'
verify_unique <- function(new_name) {
  # Extract the file extension (if any)
  ext <- tools::file_ext(new_name)
  
  if (ext != "") {
    # For files, simply check if the file exists
    return(!file.exists(new_name))
  } else {
    # For identifiers (like P05, MP01), extract the prefix and number
    pattern <- "^([A-Z]+)([0-9]+)$"
    if (grepl(pattern, new_name)) {
      prefix <- gsub(pattern, "\\1", new_name)
      number <- gsub(pattern, "\\2", new_name)
      
      # Look for files with the same prefix and number
      files <- list.files(pattern = paste0("^", prefix, number, "_.*\\.md$"))
      return(length(files) == 0)
    } else {
      # For other types of names, assume they're unique
      return(TRUE)
    }
  }
}
