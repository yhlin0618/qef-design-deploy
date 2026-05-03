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
