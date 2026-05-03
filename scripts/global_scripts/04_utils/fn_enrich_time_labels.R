#' Enrich Time Feature Labels with Hierarchical Context
#'
#' This function enriches time feature predictor labels with year, month, and
#' hierarchical context information, implementing ISSUE_115's three-tier time
#' label architecture.
#'
#' @param data Data frame containing Poisson analysis results with time features
#' @param con Database connection object for accessing order data
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz")
#'
#' @return Data frame with additional columns:
#'   - analysis_year: Integer year extracted from data
#'   - analysis_month: Integer month (1-12) extracted from predictor
#'   - time_hierarchy: Character indicating level ("year", "month", "weekday", "other")
#'   - hierarchical_label: Chinese label with full context (e.g., "2025年4月")
#'
#' @details
#' Following principles:
#'   - MP122: Statistical Interpretation Transparency
#'   - R116: Enhanced Data Access with tbl2
#'   - MP073: Interactive Visualization Preference
#'
#' Time hierarchy levels:
#'   - year: Annual trends (e.g., "2025年全年")
#'   - month: Monthly patterns (e.g., "2025年4月")
#'   - weekday: Day-of-week effects (e.g., "週一")
#'   - other: Other time features
#'
#' @examples
#' \dontrun{
#'   # Enrich time labels
#'   enriched_data <- fn_enrich_time_labels(
#'     data = poisson_time_data,
#'     con = app_data_connection,
#'     platform_id = "cbz"
#'   )
#'
#'   # Use enriched labels in display
#'   enriched_data %>%
#'     filter(time_hierarchy == "month") %>%
#'     arrange(analysis_year, analysis_month)
#' }
#'
#' @family time_analysis
#' @export
fn_enrich_time_labels <- function(data, con, platform_id = "cbz") {
  # Validate inputs
  if (!is.data.frame(data) || nrow(data) == 0) {
    warning("Empty or invalid data provided to fn_enrich_time_labels")
    return(data)
  }

  if (!"predictor" %in% names(data)) {
    warning("'predictor' column not found in data")
    return(data)
  }

  # Get current year from order data
  current_year <- tryCatch({
    table_name <- paste0("df_", platform_id, "_order_item")

    year_data <- tbl2(con, table_name) %>%
      dplyr::summarise(
        latest_year = sql("EXTRACT(YEAR FROM MAX(created_at))")
      ) %>%
      dplyr::collect()

    as.integer(year_data$latest_year)
  }, error = function(e) {
    warning("Could not extract year from database, using current year: ", e$message)
    as.integer(format(Sys.Date(), "%Y"))
  })

  # Define weekday mappings
  weekday_en_to_zh <- c(
    "monday" = "週一",
    "tuesday" = "週二",
    "wednesday" = "週三",
    "thursday" = "週四",
    "friday" = "週五",
    "saturday" = "週六",
    "sunday" = "週日",
    "Monday" = "週一",
    "Tuesday" = "週二",
    "Wednesday" = "週三",
    "Thursday" = "週四",
    "Friday" = "週五",
    "Saturday" = "週六",
    "Sunday" = "週日"
  )

  # Enrich data with hierarchical structure
  enriched <- data %>%
    dplyr::mutate(
      # Extract year
      analysis_year = dplyr::case_when(
        predictor == "year" ~ current_year,
        grepl("^month_", predictor) ~ current_year,
        TRUE ~ NA_integer_
      ),

      # Extract month number
      analysis_month = dplyr::case_when(
        grepl("^month_(\\d+)$", predictor) ~
          as.integer(stringr::str_extract(predictor, "\\d+")),
        TRUE ~ NA_integer_
      ),

      # Determine time hierarchy level
      time_hierarchy = dplyr::case_when(
        predictor == "year" ~ "year",
        grepl("^month_", predictor) ~ "month",
        tolower(predictor) %in% c("monday", "tuesday", "wednesday", "thursday",
                                   "friday", "saturday", "sunday") ~ "weekday",
        predictor == "day" ~ "day",
        TRUE ~ "other"
      ),

      # Create hierarchical label with full context
      hierarchical_label = dplyr::case_when(
        # Year level: "2025年全年"
        predictor == "year" ~ paste0(analysis_year, "年全年"),

        # Month level: "2025年4月"
        grepl("^month_", predictor) ~ paste0(analysis_year, "年", analysis_month, "月"),

        # Weekday level: "週一", "週二", etc.
        tolower(predictor) %in% names(weekday_en_to_zh) ~
          weekday_en_to_zh[tolower(predictor)],

        # Day level: "日期"
        predictor == "day" ~ "日期",

        # Other: keep original
        TRUE ~ predictor
      )
    )

  return(enriched)
}
