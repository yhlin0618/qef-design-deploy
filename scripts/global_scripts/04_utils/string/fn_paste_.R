#' Paste with underscore separator
#'
#' A wrapper around base::paste that uses underscore as the default separator
#'
#' @param ... Objects to be converted to character vectors
#' @param sep A character string to separate the terms (default: "_")
#' @param collapse An optional character string to separate the results (default: NULL)
#' @param recycle0 If FALSE, the standard recycling behavior of paste0 is maintained
#'
#' @return A character vector of the concatenated values
#'
#' @examples
#' paste_("a", "b", "c")  # Returns "a_b_c"
#' paste_(c("A", "B"), c("X", "Y"))  # Returns c("A_X", "B_Y")
paste_ <- function(..., sep = "_", collapse = NULL, recycle0 = FALSE) {
  base::paste(..., sep = sep, collapse = collapse, recycle0 = recycle0)
}