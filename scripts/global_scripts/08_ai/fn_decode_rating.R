#' Decode Rating from OpenAI API Response
#'
#' Extracts numeric rating from OpenAI API response in the format "[Score, Reason]".
#' This is part of D03_08 (Process Reviews) to extract clean numeric values.
#'
#' @param rating_string Character. The rating string from OpenAI in the format "[Score, Reason]".
#'
#' @return Numeric. The extracted rating (1-5) or NA if not found or invalid.
#'
#' @examples
#' \dontrun{
#' # Single value
#' decode_rating("[4, Good quality and design]")  # Returns 4
#' decode_rating("[NaN, NaN]")                   # Returns NA
#' 
#' # With dplyr
#' ratings_df %>%
#'   mutate(numeric_rating = decode_rating(result))
#' }
#'
#' @export
decode_rating <- function(rating_string) {
  # Handle vectorized input
  if (length(rating_string) > 1) {
    return(sapply(rating_string, decode_rating))
  }
  
  # Handle NULL or NA
  if (is.null(rating_string) || is.na(rating_string)) {
    return(NA_integer_)
  }
  
  # Extract rating using regex
  # Looking for patterns like [4, reason] or [4.0, reason]
  rating_match <- regexpr("\\[\\s*([1-5](\\.0)?)(\\s*,|\\])", rating_string)
  
  if (rating_match > 0) {
    # Extract the digit
    digit_start <- rating_match + 1  # Skip the opening bracket
    digit_length <- attr(rating_match, "match.length") - 2  # Remove brackets and comma
    rating_value <- substr(rating_string, digit_start, digit_start + digit_length - 1)
    rating_value <- trimws(rating_value)
    
    # Convert to numeric
    return(as.integer(rating_value))
  }
  
  # Handle the NaN case
  if (grepl("\\[\\s*NaN", rating_string, ignore.case = TRUE)) {
    return(NA_integer_)
  }
  
  # Handle error cases or unexpected formats
  if (grepl("error|exception|unknown", rating_string, ignore.case = TRUE)) {
    return(NA_integer_)
  }
  
  # Default: return NA for any other case
  return(NA_integer_)
}