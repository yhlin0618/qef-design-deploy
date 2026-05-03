#' Convert Email to ID
#'
#' Extracts the username portion of an email address (before @ symbol)
#' and converts it to lowercase. This function is useful for creating
#' user identifiers from email addresses.
#' 
#' Following principles:
#' - R051: Lowercase variable naming
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function, no side effects)
#'
#' @param email Character string containing an email address
#' @return Character string with the username portion in lowercase.
#'         Returns NA if input is NA or empty string if @ is not found.
#' @export
#'
#' @examples
#' email_to_id('John.Doe@example.com') # Returns 'john.doe'
#' email_to_id('admin@company.org')    # Returns 'admin'
#' email_to_id(NA)                      # Returns NA
#' 
email_to_id <- function(email) {
  # Following MP047: Functional programming - handle edge cases
  if (is.na(email) || length(email) == 0) {
    return(NA_character_)
  }
  
  # Find position of @ symbol
  at_position <- regexpr("@", email, fixed = TRUE)
  
  # If no @ found, return empty string
  if (at_position == -1) {
    return("")
  }
  
  # Extract username and convert to lowercase
  username <- substr(email, 1, at_position - 1)
  return(tolower(username))
}