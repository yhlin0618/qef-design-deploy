#' Create a KPI Value Box
#'
#' Creates a value box for displaying a KPI metric with change indicators.
#' 
#' @param ns The namespace function
#' @param title The title of the value box
#' @param value_id The output ID for the value
#' @param diff_id The output ID for the difference indicator
#' @param perc_id The output ID for the percentage change
#'
#' @return A value_box object
#'
#' @examples
#' ns <- NS("test")
#' createKpiBox(ns, "Sales", "sales_value", "sales_diff", "sales_perc")
#' 
#' @export
createKpiBox <- function(ns, title, value_id, diff_id, perc_id) {
  value_box(
    title = title,
    value = textOutput(ns(value_id)),
    showcase = uiOutput(ns(diff_id)),
    p("成長率 = ", span(textOutput(ns(perc_id), inline = TRUE)))
  )
}