#' Remove Na Strings Function
#'
#' Brief description of what this function does
#'
#' @param params Description of parameters
#' @return Description of return value
#'
#' @examples
#' remove_na_strings()
remove_na_strings <- function(x) {
  return(x[!grepl("NA", x)])
}
