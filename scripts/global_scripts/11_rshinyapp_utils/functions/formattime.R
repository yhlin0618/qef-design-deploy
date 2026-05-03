#' Format Time Function
#'
#' Formats a date object based on the specified time scale
#'
#' @param time_scale The date to format
#' @param case The time scale to use (year, quarter, month)
#'
#' @return A formatted string
#'
formattime <- function(time_scale, case) {
  if (case == "quarter") {
    return(gsub("\\.", ".Q", lubridate::quarter(time_scale, type = "year.quarter")))
  }
  if (case == "year") {
    return(format(time_scale, "%Y"))
  }
  if (case == "month") {
    return(format(time_scale, "%Y/%m"))
  }
}