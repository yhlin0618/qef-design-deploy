#' Initialize UI translation system from terminology dictionary
#'
#' Loads translations from ui_terminology.csv and initializes the
#' translation system based on app_configs$language.
#'
#' @param dictionary_path Character. Optional path to CSV dictionary.
#' @param language Character. Optional language override.
#' @return Function. Translation function (defaults to identity when unavailable).
#'
#' @export
initialize_ui_translation <- function(dictionary_path = NULL, language = NULL) {
  if (!exists("initialize_translation_system", mode = "function") ||
      !exists("get_translate_function", mode = "function")) {
    return(function(x) x)
  }

  if (is.null(dictionary_path) || !nzchar(dictionary_path)) {
    base_dir <- if (exists("APP_DIR", inherits = TRUE)) APP_DIR else getwd()
    dictionary_path <- file.path(
      base_dir,
      "scripts", "global_scripts", "30_global_data", "parameters",
      "scd_type2", "ui_terminology.csv"
    )
  }

  lang_input <- if (!is.null(language) && nzchar(language)) {
    language
  } else if (exists("get_language_scope", mode = "function")) {
    get_language_scope("ui_text")
  } else if (exists("app_configs") && !is.null(app_configs$language)) {
    lang_val <- app_configs$language
    if (is.list(lang_val)) {
      if (!is.null(lang_val$default)) lang_val$default else "en"
    } else {
      lang_val
    }
  } else {
    "en"
  }

  lang <- tolower(as.character(lang_input)[1])
  if (grepl("zh", lang)) {
    lang <- if (grepl("cn|hans", lang)) "zh_CN" else "zh_TW"
  } else if (grepl("en", lang)) {
    lang <- "en"
  } else {
    lang <- "en"
  }

  translation_dict <- NULL
  if (file.exists(dictionary_path)) {
    ui_terms <- NULL
    if (requireNamespace("readr", quietly = TRUE)) {
      ui_terms <- tryCatch(
        readr::read_csv(dictionary_path, show_col_types = FALSE),
        error = function(e) NULL
      )
    } else {
      ui_terms <- tryCatch(
        utils::read.csv(dictionary_path, stringsAsFactors = FALSE),
        error = function(e) NULL
      )
    }

    if (!is.null(ui_terms) && nrow(ui_terms) > 0) {
      names(ui_terms) <- sub("^\ufeff", "", names(ui_terms))
      if ("en_us" %in% names(ui_terms)) {
        en_vals <- trimws(as.character(ui_terms$en_us))
        en_valid <- !is.na(en_vals) & nzchar(en_vals)
        translation_dict <- list()

        if ("zh_tw" %in% names(ui_terms)) {
          zh_tw_vals <- trimws(as.character(ui_terms$zh_tw))
          zh_tw_valid <- !is.na(zh_tw_vals) & nzchar(zh_tw_vals)
          idx <- en_valid & zh_tw_valid
          if (any(idx)) {
            en_key <- en_vals[idx]
            zh_val <- zh_tw_vals[idx]
            keep <- !duplicated(en_key)
            translation_dict$zh_TW <- setNames(zh_val[keep], en_key[keep])
          }
        }

        if ("zh_cn" %in% names(ui_terms)) {
          zh_cn_vals <- trimws(as.character(ui_terms$zh_cn))
          zh_cn_valid <- !is.na(zh_cn_vals) & nzchar(zh_cn_vals)
          idx <- en_valid & zh_cn_valid
          if (any(idx)) {
            en_key <- en_vals[idx]
            zh_val <- zh_cn_vals[idx]
            keep <- !duplicated(en_key)
            translation_dict$zh_CN <- setNames(zh_val[keep], en_key[keep])
          }
        }

        if (is.null(translation_dict$zh_CN) && !is.null(translation_dict$zh_TW)) {
          translation_dict$zh_CN <- translation_dict$zh_TW
        }
      }
    }
  }

  initialize_translation_system(language = lang, translation_dict = translation_dict)
  get_translate_function()
}
