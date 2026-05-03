#' Check feature availability for a platform
#'
#' Determines if a feature is enabled for a platform based on app_configs.
#'
#' @param feature_key Character. Feature key in app_configs$features.
#' @param platform_id Character. Platform identifier (e.g., "eby", "cbz", "all").
#' @param config Optional list. Overrides app_configs when provided.
#' @return Logical. TRUE when enabled or not configured.
#'
#' @export
is_feature_enabled_for_platform <- function(feature_key, platform_id, config = NULL) {
  if (is.null(feature_key) || is.null(platform_id)) return(TRUE)

  cfg <- config
  if (is.null(cfg) && exists("app_configs", envir = .GlobalEnv)) {
    cfg <- get("app_configs", envir = .GlobalEnv)
  }

  if (is.null(cfg) || is.null(cfg$features)) return(TRUE)

  feature_cfg <- cfg$features[[feature_key]]
  enabled_for <- if (!is.null(feature_cfg) && !is.null(feature_cfg$enabled_for)) {
    feature_cfg$enabled_for
  } else {
    NULL
  }

  if (is.null(enabled_for) || length(enabled_for) == 0) return(TRUE)

  enabled_for <- as.character(enabled_for)
  if ("all" %in% enabled_for) return(TRUE)

  as.character(platform_id) %in% enabled_for
}
