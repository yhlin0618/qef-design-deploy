#' Replace NA Values in Factor with a Specific Level
#'
#' Adds a level to a factor and replaces NA values with that level.
#' Used to handle missing values in categorical data.
#'
#' @param f Factor. The factor to modify.
#' @param level String. The level to add and use for NA values.
#'
#' @return Factor. The modified factor with NA values replaced.
#'
#' @examples
#' # Replace NA values with "Unknown"
#' f <- factor(c("A", "B", NA, "A"))
#' result <- fct_na_value_to_level(f, "Unknown")
#'
fct_na_value_to_level <- function(f, level) {
  if (is.factor(f)) {
    levels(f) <- c(levels(f), level)
    f[is.na(f)] <- level
    return(f)
  } else {
    return(f)
  }
}