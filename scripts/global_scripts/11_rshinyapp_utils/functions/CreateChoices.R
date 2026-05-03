#' Create Choices Function
#'
#' Creates a list of unique values from a data frame column
#'
#' @param dta Data frame
#' @param variable Variable to extract values from
#'
#' @return A sorted vector of unique values
#'
CreateChoices <- function(dta, variable) {
  dta %>%
    dplyr::select({{ variable }}) %>%
    dplyr::pull() %>%
    unique() %>% 
    sort()
}