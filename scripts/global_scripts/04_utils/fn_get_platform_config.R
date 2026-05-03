#' Get platform configuration
#'
#' Loads platform configuration from app_configs (if already loaded)
#' or from app_config.yaml on disk. Auto-populates metadata from
#' df_platform.csv when available.
#'
#' @param platform_id Character. Optional platform_id (e.g., "cbz").
#' @param config Optional list. Override config object (full app config).
#' @param config_path Optional path to app_config.yaml.
#' @param warn Logical. Emit warnings when config or platform is missing.
#' @return List. Platform config entry or list of platforms; NULL when unavailable.
#'
#' @export
get_platform_config <- function(platform_id = NULL,
                                config = NULL,
                                config_path = NULL,
                                warn = TRUE) {
  read_app_config_yaml <- function(path, warn) {
    if (is.null(path)) {
      if (exists("CONFIG_PATH", inherits = TRUE)) {
        path <- CONFIG_PATH
      } else {
        path <- "app_config.yaml"
      }
    }

    if (!file.exists(path)) {
      stop("Config file not found: ", path)
    }

    if (!requireNamespace("yaml", quietly = TRUE)) {
      stop("Missing required package: yaml")
    }

    yaml::read_yaml(path, eval.expr = FALSE)
  }

  normalize_platforms <- function(platforms) {
    if (is.character(platforms)) {
      normalized <- lapply(platforms, function(platform_id) {
        list(platform_id = platform_id, status = "active", enabled = TRUE)
      })
      names(normalized) <- platforms
      return(normalized)
    }

    if (is.list(platforms) && !is.null(names(platforms)) && any(nzchar(names(platforms)))) {
      normalized <- lapply(names(platforms), function(platform_id) {
        entry <- platforms[[platform_id]]
        if (is.character(entry) && length(entry) == 1) {
          entry <- list(platform_id = entry)
        }
        if (is.list(entry)) {
          if (is.null(entry$platform_id) && !is.null(entry$code)) {
            entry$platform_id <- entry$code
          }
          entry$code <- NULL
          if (is.null(entry$platform_id)) entry$platform_id <- platform_id
          if (is.null(entry$status)) entry$status <- "active"
          if (is.null(entry$enabled)) entry$enabled <- TRUE
        }
        entry
      })
      names(normalized) <- names(platforms)
      return(normalized)
    }

    if (is.list(platforms)) {
      platform_ids <- vapply(
        platforms,
        function(entry) {
          if (is.list(entry)) {
            if (!is.null(entry$platform_id)) return(as.character(entry$platform_id))
            if (!is.null(entry$code)) return(as.character(entry$code))
          }
          ""
        },
        character(1)
      )
      if (all(nzchar(platform_ids))) {
        names(platforms) <- platform_ids
        platforms <- lapply(names(platforms), function(platform_id) {
          entry <- platforms[[platform_id]]
          if (is.null(entry$platform_id)) entry$platform_id <- platform_id
          entry$code <- NULL
          if (is.null(entry$status)) entry$status <- "active"
          if (is.null(entry$enabled)) entry$enabled <- TRUE
          entry
        })
        names(platforms) <- platform_ids
      }
    }

    platforms
  }

  load_platform_metadata <- function() {
    platform_df <- NULL
    if (exists("df_platform", envir = .GlobalEnv)) {
      platform_df <- get("df_platform", envir = .GlobalEnv)
    }

    if (is.null(platform_df)) {
      base_dir <- if (exists("GLOBAL_DIR", inherits = TRUE)) {
        GLOBAL_DIR
      } else {
        file.path("scripts", "global_scripts")
      }
      csv_path <- file.path(base_dir, "30_global_data", "parameters", "scd_type1", "df_platform.csv")
      if (file.exists(csv_path)) {
        if (requireNamespace("readr", quietly = TRUE)) {
          platform_df <- tryCatch(readr::read_csv(csv_path, show_col_types = FALSE),
                                  error = function(e) NULL)
        } else {
          platform_df <- tryCatch(utils::read.csv(csv_path, stringsAsFactors = FALSE),
                                  error = function(e) NULL)
        }
      }
    }

    if (is.null(platform_df) || nrow(platform_df) == 0) return(NULL)

    platform_df <- as.data.frame(platform_df)
    names(platform_df) <- sub("^\\ufeff", "", names(platform_df))
    if (!"platform_id" %in% names(platform_df)) return(NULL)

    platform_df
  }

  merge_platform_metadata <- function(platforms, platform_df) {
    if (is.null(platform_df) || is.null(names(platforms))) return(platforms)

    platform_df$platform_id <- tolower(as.character(platform_df$platform_id))

    for (platform_id in names(platforms)) {
      if (!nzchar(platform_id)) next
      row <- platform_df[platform_df$platform_id == platform_id, , drop = FALSE]
      if (nrow(row) == 0) next

      if (is.null(platforms[[platform_id]]$name) && "platform_name_english" %in% names(row)) {
        platforms[[platform_id]]$name <- as.character(row$platform_name_english[1])
      }
      if (is.null(platforms[[platform_id]]$product_id_coding) && "product_id_coding" %in% names(row)) {
        platforms[[platform_id]]$product_id_coding <- as.character(row$product_id_coding[1])
      }
    }

    platforms
  }

  cfg <- config
  if (is.null(cfg) && exists("app_configs", envir = .GlobalEnv)) {
    cfg <- get("app_configs", envir = .GlobalEnv)
  }

  if (is.null(cfg)) {
    cfg <- read_app_config_yaml(config_path, warn = warn)
  }

  if (is.null(cfg)) {
    stop("No app config available for platform lookup")
  }

  platforms <- cfg$platforms
  if (is.null(platforms) && !is.null(cfg$platform)) {
    platforms <- cfg$platform
  }

  if (is.null(platforms)) {
    stop("No platform section found in app config")
  }

  platforms <- normalize_platforms(platforms)
  platform_df <- load_platform_metadata()
  if (!is.null(platform_df)) {
    platforms <- merge_platform_metadata(platforms, platform_df)
  }

  if (is.null(platform_id)) {
    return(platforms)
  }

  platform_id <- as.character(platform_id)

  if (is.list(platforms)) {
    if (!is.null(names(platforms)) && platform_id %in% names(platforms)) {
      entry <- platforms[[platform_id]]
      if (is.list(entry) && is.null(entry$platform_id)) entry$platform_id <- platform_id
      return(entry)
    }

    for (entry in platforms) {
      if (is.list(entry) && !is.null(entry$platform_id) && as.character(entry$platform_id) == platform_id) {
        return(entry)
      }
    }
  }

  if (warn) {
    warning("Unknown platform_id: ", platform_id)
    stop("Unknown platform_id: ", platform_id)
  }
  return(NULL)
}
