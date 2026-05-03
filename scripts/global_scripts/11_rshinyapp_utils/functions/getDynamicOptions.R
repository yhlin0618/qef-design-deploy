#' Get Dynamic Options Function
#'
#' Gets unique values from a data frame based on a filter
#'
#' @param list Filter values
#' @param dta Data frame
#' @param invariable Variable to filter on
#' @param outvariable Variable to extract values from
#'
#' @return A sorted vector of unique values
#'
getDynamicOptions <- function(list, dta, invariable, outvariable) {
  dta %>%
    dplyr::filter({{invariable}} %in% list) %>%
    dplyr::pull({{outvariable}}) %>%
    unique() %>%
    sort()
}