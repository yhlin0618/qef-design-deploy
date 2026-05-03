#' Get Active Product Lines from app_config.yaml
#'
#' Returns the subset of df_product_line that are active, excluding 'all'.
#' Source of truth for which product lines are active is app_config.yaml's
#' product_lines.active field, NOT the csv file.
#'
#' @param product_line_df Data frame of product lines (default: global df_product_line)
#' @param config App configuration list (default: global app_configs)
#' @return Data frame of active product lines (product_line_id != "all")
#'
#' @details
#' app_config.yaml setting:
#'   product_lines:
#'     active: all           # all product lines are active
#'     active: [hsg, blb]    # only these are active
#'
#' Replaces the old pattern of filtering by df_product_line$included == TRUE.
#' The 'included' column no longer exists in df_product_line.csv (#363).
#'
#' @examples
#' active_pl <- get_active_product_lines()
#' nrow(active_pl)
#'
#' @export
get_active_product_lines <- function(product_line_df = df_product_line,
                                     config = app_configs) {
  pl <- product_line_df[product_line_df$product_line_id != "all", , drop = FALSE]

  active_setting <- tryCatch(
    config$product_lines$active,
    error = function(e) "all"
  )

  if (is.null(active_setting) || identical(active_setting, "all")) {
    return(pl)
  }

  active_ids <- as.character(active_setting)
  result <- pl[pl$product_line_id %in% active_ids, , drop = FALSE]

  if (nrow(result) == 0) {
    # DM_R054 v2.1.1 / #427 F21: make the error message actionable by
    # including (a) the active setting the filter is applying,
    # (b) how many rows the input product_line_df had (to distinguish
    # "source empty" from "filter dropped everything"), and (c) the
    # available ids so the operator can spot typos quickly.
    input_n <- nrow(product_line_df)
    filter_n <- nrow(pl)  # input minus "all" row
    stop("get_active_product_lines(): no active product lines match filter.\n",
         "  input rows  (df_product_line)   : ", input_n, "\n",
         "  after drop 'all'                : ", filter_n, "\n",
         "  active setting                  : ",
         paste(active_ids, collapse = ", "), "\n",
         "  available product_line_ids      : ",
         paste(pl$product_line_id, collapse = ", "), "\n",
         "Likely cause:\n",
         if (input_n == 0) {
           "  df_product_line was loaded empty — run fn_load_product_lines() diagnostics or re-bootstrap meta_data.duckdb."
         } else if (filter_n == 0) {
           "  product_line_df only contained an 'all' row — check its source."
         } else {
           "  app_config.yaml > product_lines > active lists ids not present in df_product_line — typo or stale config."
         },
         call. = FALSE)
  }

  result
}
