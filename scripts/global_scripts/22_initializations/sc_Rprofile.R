## ----------------------  .Rprofile  ----------------------
## This file is named sc_Rprofile.R instead of .Rprofile to ensure Git tracking.
## To use this file, copy it to .Rprofile in your project root or working directory.
##
## Usage:
## cp sc_Rprofile.R ~/.Rprofile    # for global use
## cp sc_Rprofile.R ./.Rprofile    # for project-specific use
##

## ❶ 私有環境（存放全部狀態與常數） --------------------------
.InitEnv <- new.env(parent = baseenv())
.InitEnv$mode <- NULL # 目前 OPERATION_MODE

# ## 掛到搜尋路徑最前（名稱維持 .autoinit_env） ----------------
# if (!".autoinit_env" %in% search()) attach(.InitEnv, name = ".autoinit_env")
# search()

## ❷ 共用工具函式存進 .InitEnv -------------------------------
.InitEnv$detect_script_path <- function() {
  for (i in rev(seq_len(sys.nframe()))) {
    f <- tryCatch(sys.frame(i)$ofile, error = function(e) NULL)
    if (!is.null(f) && nzchar(f)) {
      return(normalizePath(f))
    }
  }
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
    rstudioapi::isAvailable()) {
    p <- rstudioapi::getActiveDocumentContext()$path
    if (nzchar(p)) {
      return(normalizePath(p))
    }
  }
  ca <- commandArgs(trailingOnly = FALSE)
  fi <- sub("^--file=", "", ca[grep("^--file=", ca)])
  if (length(fi) == 1) {
    return(normalizePath(fi))
  }
  ""
}

.InitEnv$get_mode <- function(path) {
  # Walk up the path ancestry to find update_scripts or global_scripts
  # This handles scripts in subdirectories like ETL/amz/ or DRV/all/
  if (nzchar(path)) {
    path_lower <- tolower(path)
    parts <- unlist(strsplit(path_lower, .Platform$file.sep, fixed = TRUE))
    if ("update_scripts" %in% parts) return("UPDATE_MODE")
    if ("global_scripts" %in% parts) return("GLOBAL_MODE")
  }
  "APP_MODE"
}

## ❸ 初始化函式（存放於 .InitEnv） ----------------------------
.InitEnv$autoinit <- function() {
  # Allow explicit override: if OPERATION_MODE was pre-set in .GlobalEnv, respect it
  if (exists("OPERATION_MODE", envir = .GlobalEnv, inherits = FALSE)) {
    .InitEnv$OPERATION_MODE <- get("OPERATION_MODE", envir = .GlobalEnv)
  } else {
    .InitEnv$OPERATION_MODE <- .InitEnv$get_mode(.InitEnv$detect_script_path())
  }
  if (identical(.InitEnv$mode, .InitEnv$OPERATION_MODE)) {
    return(invisible(NULL))
  }

  message(">> OPERATION_MODE = ", .InitEnv$OPERATION_MODE)
  .InitEnv$OPERATION_MODE <- .InitEnv$OPERATION_MODE

  if (!requireNamespace("here", quietly = TRUE)) {
    install.packages("here") # 若不存在就安裝
  }

  base <- if (exists("APP_DIR", envir = .InitEnv)) {
    .InitEnv$APP_DIR
  } else {
    # Posit Connect / subdir deploy: here::here() = git repo root, not the Shiny app folder.
    # app.R sets PROJECT_ROOT = getwd() before sourcing the union; prefer that over here().
    base <- if (nzchar(Sys.getenv("PROJECT_ROOT", ""))) {
      normalizePath(Sys.getenv("PROJECT_ROOT"), winslash = "/", mustWork = FALSE)
    } else {
      here::here()
    }
    list2env(list(
      APP_DIR = base,
      COMPANY_DIR = dirname(base),
      GLOBAL_DIR = file.path(base, "scripts", "global_scripts"),
      GLOBAL_DATA_DIR = file.path(base, "scripts", "global_scripts", "30_global_data"),
      GLOBAL_PARAMETER_DIR = file.path(base, "scripts", "global_scripts", "30_global_data", "parameters"),
      CONFIG_PATH = file.path(base, "app_config.yaml"),
      APP_DATA_DIR = file.path(base, "data", "app_data"),
      APP_PARAMETER_DIR = file.path(base, "data", "app_data", "parameters"),
      LOCAL_DATA_DIR = file.path(base, "data", "local_data")
      # app_config_path
    ), envir = .InitEnv)
    base
  }

  # ---- DEV_R057: Session-Start Toolchain Readiness (UPDATE/GLOBAL only) ----
  # APP_MODE skips this check entirely — Posit Connect / shinyapps deploy
  # environments manage their own R toolchain via manifest.json. Adding a
  # package check here would violate DM_R063 deploy bundle purity by
  # increasing startup latency on the deploy path.
  if (.InitEnv$OPERATION_MODE %in% c("UPDATE_MODE", "GLOBAL_MODE")) {
    .pkg_check_path <- file.path(.InitEnv$GLOBAL_DIR, "04_utils",
                                  "fn_check_session_packages.R")
    if (file.exists(.pkg_check_path)) {
      source(.pkg_check_path)
      # stop_on_missing_core = TRUE -> fail-fast with actionable error
      # listing the install.packages() remedy. Optional packages emit
      # warning() but do not block.
      check_session_packages(stop_on_missing_core = TRUE)
    } else {
      warning(
        "DEV_R057: fn_check_session_packages.R not found at ",
        .pkg_check_path,
        ". Toolchain readiness check skipped — install / update may surface ",
        "downstream as missing-package errors.",
        call. = FALSE
      )
    }
  }

  # ---- 讀取 db_paths.yaml (DM_R048: YAML for configuration) -----------------
  yaml_path <- file.path(.InitEnv$GLOBAL_DIR, "30_global_data", "parameters",
                         "scd_type1", "db_paths.yaml")
  if (!file.exists(yaml_path)) {
    stop("Required db configuration file not found: ", yaml_path, ". db_paths.yaml is mandatory.")
  }

  # Load YAML and construct full paths
  db_config <- yaml::read_yaml(yaml_path)
  db_path_list <- list()
  db_path_required <- list()

  # #435: Resolve a db_paths.yaml entry into (path, required).
  # Accepts two forms:
  #   - Scalar:  `<name>: path/to/file.duckdb`           (required=TRUE)
  #   - Struct:  `<name>: {path: ..., required: false}`  (optional)
  # MP154 / verify-444 B1:`required` MUST be logical TRUE/FALSE (or absent
  # = default TRUE). Malformed values (typo `flase`, quoted `"false"`,
  # numeric, NA) stop() with actionable error — silent downgrade would
  # turn required DBs into optional on YAML typos.
  .resolve_db_entry <- function(entry, name, section) {
    # verify-435 P2-2/3:guard against NA / whitespace path values.
    # Prior logic used `nzchar(entry)` which returns NA for NA_character_,
    # causing `if (NA)` crash instead of actionable error.
    if (is.character(entry) && length(entry) == 1L &&
        !is.na(entry) && nzchar(trimws(entry))) {
      return(list(path = entry, required = TRUE))
    }
    if (is.list(entry) &&
        !is.null(entry$path) &&
        is.character(entry$path) &&
        length(entry$path) == 1L &&
        !is.na(entry$path) &&
        nzchar(trimws(entry$path))) {
      required <- entry$required
      if (is.null(required)) {
        required <- TRUE
      } else if (!is.logical(required) ||
                 length(required) != 1L ||
                 is.na(required)) {
        stop(sprintf(
          paste0(
            "Invalid 'required' in db_paths.yaml %s.%s: ",
            "must be logical TRUE or FALSE (or absent = default TRUE). ",
            "Got: %s (class=%s)"),
          section, name,
          paste(deparse(required), collapse = " "),
          paste(class(required), collapse = "/")
        ), call. = FALSE)
      }
      return(list(path = entry$path, required = required))
    }
    stop(sprintf(
      paste0(
        "Invalid db_paths.yaml entry for %s.%s. ",
        "Use either '<name>: relative/path.duckdb' or ",
        "'<name>: {path: relative/path.duckdb, required: false}'."),
      section, name
    ), call. = FALSE)
  }

  .register_db_entry <- function(name, entry, section) {
    resolved <- .resolve_db_entry(entry, name, section)
    db_path_list[[name]] <<- file.path(base, resolved$path)
    db_path_required[[name]] <<- isTRUE(resolved$required)
  }

  # Process databases section (DM_R050: Mode-Specific Path Loading)
  if (!is.null(db_config$databases)) {
    if (.InitEnv$OPERATION_MODE == "APP_MODE") {
      # APP_MODE: load the databases consumed at Shiny runtime.
      #   - app_data is REQUIRED on disk (fail-fast below).
      #   - meta_data is OPTIONAL (DM_R054 v2.1.1): populate when the local
      #     DuckDB file exists (local dev / DuckDB mode), but tolerate its
      #     absence so Supabase-mode deploys on Posit Connect don't fail
      #     fast when no local meta_data.duckdb is bundled.
      if ("app_data" %in% names(db_config$databases)) {
        .register_db_entry("app_data", db_config$databases[["app_data"]],
                           "databases")
      }
      if ("meta_data" %in% names(db_config$databases)) {
        .meta_resolved <- .resolve_db_entry(
          db_config$databases[["meta_data"]], "meta_data", "databases"
        )
        .meta_path <- file.path(base, .meta_resolved$path)
        if (file.exists(.meta_path)) {
          db_path_list[["meta_data"]] <- .meta_path
          db_path_required[["meta_data"]] <- isTRUE(.meta_resolved$required)
        } else if (nzchar(Sys.getenv("AUTOINIT_DEBUG", ""))) {
          message("[autoinit-debug] APP_MODE: meta_data.duckdb absent at ",
                  .meta_path,
                  " — skipping (DM_R054 v2.1.1: Supabase mode will handle ",
                  "via dbConnectAppData).")
        }
        rm(.meta_path, .meta_resolved)
      }
    } else {
      # UPDATE_MODE/GLOBAL_MODE: 載入所有路徑
      for (name in names(db_config$databases)) {
        .register_db_entry(name, db_config$databases[[name]], "databases")
      }
    }
  }

  # Process domain section (only for UPDATE_MODE/GLOBAL_MODE)
  if (!is.null(db_config$domain) &&
      .InitEnv$OPERATION_MODE %in% c("UPDATE_MODE", "GLOBAL_MODE")) {
    for (name in names(db_config$domain)) {
      .register_db_entry(name, db_config$domain[[name]], "domain")
    }
  }

  # ---- Fail-fast precheck (autoinit-failfast-policy spec) -------------------
  # Verify all required DB files exist on disk BEFORE any downstream
  # sc_initialization_*.R is sourced. Without this guard, a missing .duckdb
  # path on Dropbox CloudStorage can cause DuckDB create + sync lock
  # contention, leading to multi-hour hangs (see issue #421).
  #
  # DM_R054 v2.1.1: APP_MODE is OUT OF SCOPE for this precheck because
  # production deploys on Posit Connect run with database.mode = supabase
  # and have NO local DuckDB files in /cloud/project/data/. Fail-fast in
  # APP_MODE would always trip on Posit Connect. Shiny startup errors in
  # APP_MODE are handled by dbConnectAppData() which routes to Supabase
  # when local DuckDB is unreachable. Dropbox × DuckDB race (issue #421)
  # only manifests in UPDATE_MODE anyway (ETL pipeline runs there).
  run_precheck <- !identical(.InitEnv$OPERATION_MODE, "APP_MODE")
  if (nzchar(Sys.getenv("AUTOINIT_DEBUG", ""))) {
    message("[autoinit-debug] OPERATION_MODE=", .InitEnv$OPERATION_MODE,
            "; precheck=", if (run_precheck) "ENABLED" else "SKIPPED (APP_MODE)")
    if (run_precheck) {
      message("[autoinit-debug] precheck running for ", length(db_path_list), " DB paths")
      for (n in names(db_path_list)) {
        message("[autoinit-debug]   ", n, " -> ", db_path_list[[n]],
                " (exists=", file.exists(db_path_list[[n]]),
                ", required=", isTRUE(db_path_required[[n]]), ")")
      }
    }
  }
  missing_dbs <- character()
  if (run_precheck) {
    for (name in names(db_path_list)) {
      # #435: Skip optional DBs (required=FALSE) — allows new-company bootstrap
      # when non-essential DBs (e.g. AI-rating scratch) haven't materialized.
      #
      # verify-435 P1-2 defensive default (MP154):if name is NOT present in
      # db_path_required (e.g. future code path populates db_path_list but
      # forgets to update db_path_required — brittle but conceivable),
      # default to **required = TRUE** (fail-safe). Previously used
      # `isTRUE(db_path_required[[name]])` which silently returned FALSE
      # on missing names → treated as optional → silent downgrade.
      required_val <- db_path_required[[name]]
      required <- is.null(required_val) || isTRUE(required_val)
      if (!required) {
        if (!file.exists(db_path_list[[name]]) &&
            nzchar(Sys.getenv("AUTOINIT_DEBUG", ""))) {
          message("[autoinit-debug] optional missing: ", name,
                  " -> ", db_path_list[[name]], " (skipped)")
        }
        next
      }
      path <- db_path_list[[name]]
      if (!file.exists(path)) {
        missing_dbs <- c(missing_dbs,
                         sprintf("  - %s: %s", name, path))
      }
    }
  }
  if (length(missing_dbs) > 0) {
    company <- basename(base)
    stop(sprintf(
      paste0(
        "ETL pipeline incomplete: %d required DB file(s) missing:\n",
        "%s\n\n",
        "Remediation: %s\n",
        "  cd %s/scripts/update_scripts\n",
        "  make run PLATFORM=<platform>              # full pipeline\n",
        "  make run PLATFORM=<platform> TARGET=<tgt> # specific layer\n\n",
        "Typical first-time bootstrap:\n",
        "  make config-full && make run PLATFORM=amz"
      ),
      length(missing_dbs),
      paste(missing_dbs, collapse = "\n"),
      if (length(missing_dbs) >= 2)
        "Multiple layers missing; run the full pipeline to rebuild."
      else
        "Run the ETL target that produces the missing file.",
      company
    ), call. = FALSE)
  }

  # ---- DuckDB lock pre-flight (#437) ----------------------------------------
  # All required DB files exist — before handing db_path_list to callers that
  # will try to open them RW, detect any foreign process already holding a
  # lock (typically a dev Shiny `app.R` session). Surface actionable info
  # (PID + full command + user) so the operator can close the right session
  # without the pipeline having to get mid-way through DRV targets and abort
  # on a cryptic rapi_startup error.
  #
  # Scope: UPDATE_MODE/GLOBAL_MODE only (APP_MODE is a reader; it's not
  # blocking anyone and skipping keeps Posit Connect startup free of lsof).
  if (run_precheck) {
    # F6 from verify-437 (fixed post-close, see commit history): the previous
    # `$base/shared/...` fallback never matches any real company layout
    # (`QEF_DESIGN/shared/` doesn't exist — only `scripts/global_scripts`
    # symlinks are used). The real repo-root fallback from a company root
    # is `$base/../shared/...` (one level up: company → l4_enterprise, then
    # into shared/). The earlier `$base/../../shared/...` patch was off by
    # one level — it resolved to `projects/ai_martech/shared` which doesn't
    # exist. Keep `$base/../shared/...` plus deeper fallback for when
    # autoinit runs from nested paths (e.g. MAMBA/deployment/mamba-enterprise).
    # Surface a warning (not debug-only) if we still can't find the utility,
    # so silent degradation is visible.
    util_candidates <- c(
      file.path(base, "scripts", "global_scripts", "04_utils",
                "fn_check_db_locks.R"),
      file.path(base, "..", "shared", "global_scripts", "04_utils",
                "fn_check_db_locks.R"),
      file.path(base, "..", "..", "shared", "global_scripts", "04_utils",
                "fn_check_db_locks.R")
    )
    util_path <- util_candidates[file.exists(util_candidates)][1]
    if (!is.na(util_path)) {
      source(util_path, local = FALSE)
      holders <- tryCatch(check_db_locks(db_path_list, exclude_self = TRUE),
                          error = function(e) {
                            message("[autoinit] lock check skipped: ", e$message)
                            list()
                          })
      if (length(holders) > 0) {
        lines <- vapply(holders, function(h) {
          sprintf("  - db=%s pid=%d user=%s\n    command: %s\n    file: %s",
                  h$db_name, h$pid, h$user, h$command, h$file)
        }, character(1))
        stop(sprintf(
          paste0(
            "Pipeline cannot start: %d DuckDB file(s) already locked by another process.\n",
            "%s\n\n",
            "Remediation:\n",
            "  - If the command above is a dev session (e.g. `R --file=app.R`, `Rscript app.R`), close it in that terminal first (Ctrl+C).\n",
            "  - If it's another `make run` or REPL session, wait for it to finish or stop it.\n",
            "  - If you must terminate: `kill -TERM <PID>` (avoid `-9` unless the process is unresponsive; never kill a session you don't own).\n",
            "  - Opening the same DB RO+RW in the same R session also trips this; disconnect first with `DBI::dbDisconnect(con, shutdown=TRUE)`."
          ),
          length(holders),
          paste(lines, collapse = "\n")
        ), call. = FALSE)
      }
    } else {
      warning(
        "[autoinit] fn_check_db_locks.R not found in any candidate path; ",
        "lock pre-flight SKIPPED. Candidates tried: ",
        paste(util_candidates, collapse = ", "),
        ". Set AUTOINIT_DEBUG=1 for path diagnostics.",
        call. = FALSE
      )
    }
  }

  # Assign to environments
  assign("db_path_list", db_path_list, envir = .InitEnv)
  assign("db_path_list", db_path_list, envir = .GlobalEnv)
  list2env(db_path_list, envir = .GlobalEnv)

  ## 把 .InitEnv 裡所有綁定複製到 .GlobalEnv (這樣常數才能被使用)
  list2env(as.list(.InitEnv, all.names = TRUE), envir = .GlobalEnv)

  ## 1️⃣ 決定應該載入哪些初始化腳本（向量）
  init_files <- switch(OPERATION_MODE,
    UPDATE_MODE = c(
      "sc_initialization_app_mode.R",
      "sc_initialization_update_mode.R"
    ), # ← 兩支都跑
    GLOBAL_MODE = "sc_initialization_update_mode.R",
    APP_MODE = "sc_initialization_app_mode.R"
  )

  ## 2️⃣ 逐一載入 -------------------------------------------------
  for (f in init_files) {
    full <- file.path(.InitEnv$GLOBAL_DIR, "22_initializations", f)
    if (file.exists(full)) {
      sys.source(full, envir = .GlobalEnv)
    } else {
      stop("Init file not found: ", full)
    }
  }

  .GlobalEnv$INITIALIZATION_COMPLETED <- TRUE

  invisible(NULL)
}

## ❹ 收尾函式（存放於 .InitEnv） ------------------------------
.InitEnv$autodeinit <- function() {
  ## 1. 關閉所有資料庫連線（函式在 .InitEnv 內）
  if (exists("dbDisconnect_all")) {
    dbDisconnect_all()
  }

  # ## 2. 從搜尋路徑移除 .InitEnv（若有 attach 過）
  # if (".autoinit_env" %in% search()) detach(".autoinit_env")

  ## 3. 刪除 .GlobalEnv 中除 .InitEnv 以外的所有物件 ----------
  objs <- ls(envir = .GlobalEnv, all.names = TRUE)
  objs <- setdiff(objs, c(".InitEnv")) # 保留私有環境
  rm(list = objs, envir = .GlobalEnv)
  gc() # 觸發垃圾回收

  ## 4. 把轉接器薄殼函式重新放回 .GlobalEnv -------------------
  assign("autoinit",
    function(...) .InitEnv$autoinit(...),
    envir = .GlobalEnv
  )
  assign("autodeinit",
    function(...) .InitEnv$autodeinit(...),
    envir = .GlobalEnv
  )

  ## 5. 清除 MODE 旗標，讓下次 autoinit() 重新啟動 ------------
  .InitEnv$mode <- NULL
  message(">> De-init completed ‒ GlobalEnv 已清空並重建薄殼")

  invisible(NULL)
}

## ❺ .GlobalEnv 轉接器（薄殼函式） ---------------------------
assign("autoinit",
  function(...) .InitEnv$autoinit(...),
  envir = .GlobalEnv
)
assign("autodeinit",
  function(...) .InitEnv$autodeinit(...),
  envir = .GlobalEnv
)

## ❻ （可選）啟動即初始化；若不想自動請註解 ------------------
# autoinit()
## -----------------------------------------------------------
