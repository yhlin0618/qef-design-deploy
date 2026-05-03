#' Recode Time TraceBack Function
#'
#' Converts a time scale to the corresponding historical time scale
#'
#' @param profile The time scale to convert
#'
#' @return The corresponding historical time scale
#'
Recode_time_TraceBack <- function(profile) {
  switch(profile,
         "m1quarter" = "m1quarter",
         "m1year" = "m1year",
         "m1month" = "m1month",
         "quarter" = "m1quarter",
         "year" = "m1year",
         "month" = "m1month",
         NA)  # If no match, return NA
}