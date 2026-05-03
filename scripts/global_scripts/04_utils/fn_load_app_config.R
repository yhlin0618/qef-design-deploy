#' loadAppConfig  -------------------------------------------------------------
#' 簡化版應用設定載入器。
#'
#' 1. 讀取 YAML 設定檔 → 回傳 list `cfg`。
#' 2. 遞迴掃描 `parameters_dir` 內所有 .csv 檔 → 讀成 `tibble` 嵌入 `cfg$parameters`。
#' 3. `attach = TRUE` 時，將 `cfg` 與每個參數表格賦值到 `.GlobalEnv`。
#'
#' 不再：
#' • 支援 readxl／rds／excel（如需可自行擴充）
#' • 動態安裝缺失套件 → 假設環境已安裝 yaml / readr / fs
#'
#' @param parameters_dir 參數目錄向量；預設 `c(GLOBAL_PARAMETER_DIR, APP_PARAMETER_DIR)`。
#' @param attach         Logical. 是否將結果附加至全域，預設 TRUE。
#' @param verbose        Logical. 是否列印載入訊息。
#'
#' @return List `cfg`，含 `$yaml` 與 `$parameters`。
#' @export
#' @importFrom yaml read_yaml
#' @importFrom readr read_csv
#' @importFrom fs dir_ls
load_app_configs <- function(parameters_dir = NULL,
                          attach = TRUE,
                          verbose = FALSE) {
  # ---- 路徑解析 -----------------------------------------------------------
  if (!exists("CONFIG_PATH", inherits = TRUE))
    stop("CONFIG_PATH constant is not defined in the environment")
  if (!file.exists(CONFIG_PATH)) stop("Config yaml not found: ", CONFIG_PATH)
  
  if (is.null(parameters_dir)) {
    parameters_dir <- c(
      get0("GLOBAL_PARAMETER_DIR", inherits = TRUE),
      get0("APP_PARAMETER_DIR",    inherits = TRUE)
    )
    parameters_dir <- parameters_dir[dir.exists(parameters_dir)]
  }
  
  # ---- 載入 YAML (含遞迴 overrides) --------------------------------------
  cfg <- yaml::read_yaml(CONFIG_PATH, eval.expr = FALSE)
  if (verbose) message("Loaded YAML: ", CONFIG_PATH)

  # 搜尋 parameters_dir 內 *.yaml / *.yml 作為覆寫檔
  yaml_overrides <- list()  # 收集檔名供 verbose 列印，不再存入 cfg
  if (length(parameters_dir)) {
    # Search for both .yaml and .yml files separately
    yaml_files <- c()
    for (dir in parameters_dir) {
      yaml_files <- c(yaml_files, 
                     fs::dir_ls(dir, recurse = TRUE, glob = "*.yaml", type = "file"),
                     fs::dir_ls(dir, recurse = TRUE, glob = "*.yml", type = "file"))
    }
    yaml_files <- yaml_files[!grepl("^~\\$", basename(yaml_files))]
    yaml_files <- yaml_files[!duplicated(basename(yaml_files), fromLast = TRUE)]

    # Helper  深度合併 list ------------------------------------------------
    deep_merge <- function(x, y) {
      for (nm in names(y)) {
        if (is.list(x[[nm]]) && is.list(y[[nm]])) {
          x[[nm]] <- Recall(x[[nm]], y[[nm]])
        } else {
          x[[nm]] <- y[[nm]]
        }
      }
      x
    }

    for (fp in yaml_files) {
      yml <- tryCatch(yaml::read_yaml(fp, eval.expr = FALSE),
                      error = function(e) { warning("Failed to read ", fp); NULL })
      if (!is.null(yml)) {
        yaml_name <- tools::file_path_sans_ext(basename(fp))
        yaml_overrides[[yaml_name]] <- yml
        cfg[[yaml_name]] <- yml  # Add directly to cfg first level
        cfg <- deep_merge(cfg, yml)  # Also merge for override behavior
        if (verbose) message("Merged YAML override: ", fp)
      }
    }
  }
  # 不再將 overrides 存入 cfg$yaml_overrides —— 已直接合併到 cfg 最外層
  
  # ---- 載入 CSV 參數 ------------------------------------------------------
  param_list <- list()
  if (length(parameters_dir)) {
    csv_files <- unlist(lapply(parameters_dir, fs::dir_ls, recurse = TRUE, glob = "*.csv", type = "file"))
    csv_files <- csv_files[!grepl("^~\\$", basename(csv_files))]  # skip temp files
    csv_files <- csv_files[!duplicated(basename(csv_files), fromLast = TRUE)]
    for (fp in csv_files) {
      nm <- tools::file_path_sans_ext(basename(fp))
      tbl <- tryCatch(readr::read_csv(fp, show_col_types = FALSE),
                      error = function(e) { warning("Failed to read ", fp); NULL })
      if (!is.null(tbl)) param_list[[nm]] <- tbl
    }

    # Also source any *.R scripts under parameters_dir ---------------------------------
    r_scripts <- unlist(lapply(parameters_dir, fs::dir_ls, recurse = TRUE, glob = "*.R", type = "file"))
    r_scripts <- r_scripts[!grepl("^~\\$", basename(r_scripts))]
    r_scripts <- r_scripts[!duplicated(basename(r_scripts), fromLast = TRUE)]
    for (fp in r_scripts) {
      if (verbose) message("Sourcing parameter script: ", fp)
      tryCatch(sys.source(fp, envir = .GlobalEnv),
               error = function(e) warning("Failed to source ", fp, ": ", e$message))
    }
 
    if (verbose) message("Loaded ", length(param_list), " parameter tables from ", length(parameters_dir), " dir(s)")
  }
  cfg$parameters <- param_list

  # ---- normalize platforms -------------------------------------------------
  normalize_platforms <- function(platforms) {
    if (is.character(platforms)) {
      entries <- lapply(platforms, function(platform_id) {
        list(platform_id = platform_id, status = "active", enabled = TRUE)
      })
      names(entries) <- platforms
      return(entries)
    }

    if (is.list(platforms) && !is.null(names(platforms)) && any(nzchar(names(platforms)))) {
      entries <- lapply(names(platforms), function(platform_id) {
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
      names(entries) <- names(platforms)
      return(entries)
    }

    if (is.list(platforms)) {
      entries <- platforms
      platform_ids <- vapply(entries, function(entry) {
        if (is.list(entry)) {
          if (!is.null(entry$platform_id)) {
            return(as.character(entry$platform_id))
          }
          if (!is.null(entry$code)) {
            return(as.character(entry$code))
          }
        }
        ""
      }, character(1))
      if (all(nzchar(platform_ids))) {
        names(entries) <- platform_ids
        entries <- lapply(names(entries), function(platform_id) {
          entry <- entries[[platform_id]]
          if (is.null(entry$platform_id)) entry$platform_id <- platform_id
          entry$code <- NULL
          if (is.null(entry$status)) entry$status <- "active"
          if (is.null(entry$enabled)) entry$enabled <- TRUE
          entry
        })
        names(entries) <- platform_ids
        return(entries)
      }
    }

    platforms
  }

  get_platform_metadata <- function(param_list) {
    platform_df <- NULL
    if ("df_platform" %in% names(param_list)) {
      platform_df <- param_list[["df_platform"]]
    } else if (exists("df_platform", envir = .GlobalEnv)) {
      platform_df <- get("df_platform", envir = .GlobalEnv)
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

  if (!is.null(cfg$platforms)) {
    platforms <- normalize_platforms(cfg$platforms)
    platform_df <- get_platform_metadata(param_list)
    if (!is.null(platform_df)) {
      platforms <- merge_platform_metadata(platforms, platform_df)
    }
    cfg$platforms <- platforms

    if (!exists("MAMBA_PLATFORM_CONFIG_AUDITED", envir = .GlobalEnv)) {
      registry_platforms <- NULL
      if (!is.null(platform_df) && "platform_id" %in% names(platform_df)) {
        registry_platforms <- unique(tolower(as.character(platform_df$platform_id)))
        registry_platforms <- registry_platforms[nzchar(registry_platforms)]
      }
      configured_platforms <- tolower(names(platforms))
      if (!is.null(registry_platforms) &&
          "amz" %in% registry_platforms &&
          !("amz" %in% configured_platforms)) {
        message("Detected platform 'amz' in df_platform but not in app_config.yaml; skipping amz.")
      }
      assign("MAMBA_PLATFORM_CONFIG_AUDITED", TRUE, envir = .GlobalEnv)
    }
  }
  
  # ---- attach to global ---------------------------------------------------
  if (isTRUE(attach)) {
    assign("app_configs", cfg, envir = .GlobalEnv)
    
    # Attach CSV parameter tables to global environment
    for (nm in names(param_list)) assign(nm, param_list[[nm]], envir = .GlobalEnv)
    
    if (verbose && length(yaml_overrides)) {
      message("YAML files merged into app_configs: ", 
              paste(names(yaml_overrides), collapse = ", "))
    }
  }
  invisible(cfg)
}
