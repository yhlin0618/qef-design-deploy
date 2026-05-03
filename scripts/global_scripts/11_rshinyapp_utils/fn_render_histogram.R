# fn_render_histogram.R
# Shared histogram renderer with DP_R001 winsorization support
# Following: MP032 (DRY), DP_R001, SO_R007 (One Function One File)
#
# Usage:
#   output$hist_r <- render_histogram(data_reactive, "r_value",
#     xlab = translate("Recency (days)"), color = "#dc3545", cap_pctl = 0.99)
#
# Cap strategies (DP_R001):
#   cap_discrete - fixed upper bound for count data (e.g. F capped at 50)
#   cap_pctl     - percentile cap for continuous data (e.g. M capped at P99)
#   Neither      - no cap (for bounded data like P(alive) 0-1, CAI, scores)

render_histogram <- function(data_reactive, col, xlab, color,
                             translate_fn = translate,
                             cap_discrete = NULL, cap_pctl = NULL) {
  renderPlotly({
    df <- data_reactive()
    if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate_fn("No Data")))

    vals <- df[[col]]
    vals <- vals[!is.na(vals)]
    if (length(vals) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate_fn("No Data")))

    annotation <- NULL

    if (!is.null(cap_discrete)) {
      n_over <- sum(vals > cap_discrete)
      vals <- pmin(vals, cap_discrete)
      if (n_over > 0) {
        annotation <- list(
          text = paste0(cap_discrete, "+ (", format(n_over, big.mark = ","), " ", translate_fn("customers"), ")"),
          showarrow = FALSE, xref = "paper", yref = "paper", x = 0.98, y = 0.98,
          xanchor = "right", font = list(size = 10, color = "#666"))
      }
    } else if (!is.null(cap_pctl)) {
      cap_val <- stats::quantile(vals, cap_pctl, na.rm = TRUE)
      n_over <- sum(vals > cap_val)
      if (n_over > 0) {
        vals <- pmin(vals, cap_val)
        annotation <- list(
          text = paste0("P", round(cap_pctl * 100), ": ", format(round(cap_val, 0), big.mark = ",")),
          showarrow = FALSE, xref = "paper", yref = "paper", x = 0.98, y = 0.98,
          xanchor = "right", font = list(size = 10, color = "#666"))
      }
    }

    p <- plotly::plot_ly(x = vals, type = "histogram",
                         marker = list(color = color, line = list(color = "white", width = 1))) %>%
      plotly::layout(xaxis = list(title = xlab),
                     yaxis = list(title = translate_fn("Customer Count")),
                     margin = list(t = 10, b = 50))
    if (!is.null(annotation)) p <- p %>% plotly::layout(annotations = list(annotation))
    p
  })
}
