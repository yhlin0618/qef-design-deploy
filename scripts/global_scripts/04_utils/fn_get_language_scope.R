#' Get Language for a Given Scope
#'
#' Resolves language setting using three-layer priority:
#'   field -> category.default -> category (string) -> global default
#'
#' @param scope Character. Category name (e.g., "ui_text", "product_attributes")
#' @param field Character. Optional field name for Layer 3 override
#' @param lang_config List. The language config object from app_config.yaml
#' @return Character. Language code (e.g., "zh_TW", "en")
get_language_scope <- function(scope = NULL, field = NULL, lang_config = NULL) {
  if (is.null(lang_config)) {
    if (exists("app_configs", inherits = TRUE) && !is.null(app_configs$language)) {
      lang_config <- app_configs$language
    } else {
      return("en")
    }
  }

  # If lang_config is still a plain string (legacy), return it
  if (is.character(lang_config)) return(lang_config)

  global_default <- if (!is.null(lang_config$default)) lang_config$default else "en"

  if (is.null(scope) || is.null(lang_config$scopes)) return(global_default)

  scope_val <- lang_config$scopes[[scope]]
  if (is.null(scope_val)) return(global_default)

  # scope_val is a string -> return it
  if (is.character(scope_val)) return(scope_val)

  # scope_val is a list -> check field then default
  if (is.list(scope_val)) {
    if (!is.null(field) && !is.null(scope_val[[field]])) return(scope_val[[field]])
    if (!is.null(scope_val$default)) return(scope_val$default)
  }

  global_default
}
