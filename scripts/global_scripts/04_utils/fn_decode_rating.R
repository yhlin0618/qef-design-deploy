#' Decode Rating String to Integer
#'
#' Converts a rating string into an integer value between 0 and 10.
#' If the string cannot be converted, returns NA_integer_.
#'
#' @param x A character vector that may contain rating values
#' @return An integer vector with values between 0 and 10, or NA_integer_ for values that cannot be converted
#' @export
#'
#' @examples
#' decode_rating("Rating: 8") # Returns 8
#' decode_rating("Score: 10/10") # Returns 10
#' decode_rating("N/A") # Returns NA_integer_
#' decode_rating("NaN") # Returns NA_integer_
#' decode_rating(NA) # Returns NA_integer_
#' decode_rating(c("Rating: 7", "Score: 10", NA, "NaN")) # Returns c(7, 10, NA, NA)
decode_rating <- function(x) {
  # 先把不合法的值標成 NA
  na_idx <- is.na(x) | grepl("NaN", x, ignore.case = TRUE)
  
  # 擷取 0–10 的整數字串
  num <- stringr::str_extract(x, "\\b(10|[0-9])\\b")
  num <- as.integer(num)
  
  # 把原本該是 NA 的位置補回 NA_integer_
  num[na_idx] <- NA_integer_
  num
}