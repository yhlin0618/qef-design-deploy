#' #' Macro Trends Server Component
#' #'
#' #' @param id The ID of the module
#' #' @param data_source The reactive data source
#' #' @return The module server function
#' #' @export
#' macroTrendsServer <- function(id, data_source) {
#'   moduleServer(id, function(input, output, session) {
#'     # Create time plot
#'     output$time_plot <- renderPlot({
#'       req(data_source())
#'       df <- data_source()
#'       
#'       ggplot(df, aes(x = last_purchase, fill = segment)) +
#'         geom_histogram(bins = 30, position = "stack") +
#'         scale_fill_brewer(palette = "Set1") +
#'         theme_minimal() +
#'         labs(title = "Purchase Timing by Segment", x = "Date", y = "Count")
#'     })
#'   })
#' }