#' Resolve company product master from 6 sources with priority-based integration.
#'
#' Implements Decisions D1 (layered schema), D2 (multi-marketplace PK),
#' D3 (multi-source integration without deprecating xlsx), partial D4
#' (conflict arbitration delegated to fn_arbitrate_product_master_conflicts),
#' and Track C MP155 Phase 3 (sales + catalogue System-Sourced derivation).
#'
#' Default source_priority (Human-Decided fallback, highest first):
#'   1. Gsheet `company_product_master` tab
#'   2. Gsheet `SKU to ASIN` tab (legacy financial sheet)
#'   3. Local KEYS.xlsx
#'   4. Local SKUtoASIN number.xlsx
#'   5. Catalogue (df_amz_product_master)        — Track C
#'   6. Sales    (df_amz_sales___transformed)    — Track C
#'
#' For System-Sourced fields the priority is reversed via priority_per_class
#' (sales > catalogue > xlsx > Gsheet); see product_master_fields.yaml.
#'
#' @param app_config Parsed app_config.yaml list for the company, with
#'                   etl_sources.company_product_master / sku_mapping / keys.
#' @param rawdata_dir Base rawdata directory (for xlsx sources).
#' @param db_paths Optional named list with `transformed_data` and `raw_data`
#'                 paths (DuckDB files). If NULL, looked up from `db_path_list`
#'                 in the global environment (UPDATE_MODE convention).
#'
#' @return data.frame with columns sku, marketplace, amz_asin, product_line_id,
#'         brand, cost, profit, product_name, status, launch_date, source_origin.

MASTER_SCHEMA_COLS <- c(
  "sku", "marketplace", "amz_asin", "product_line_id", "brand",
  "cost", "profit", "product_name", "status", "launch_date"
)

resolve_company_product_master <- function(app_config, rawdata_dir, db_paths = NULL) {
  # --- Collect each source as a data.frame ---------------------------------
  sources <- list(
    gsheet_master   = try_read_gsheet_master(app_config),
    gsheet_sku_asin = try_read_gsheet_sku_asin(app_config),
    keys_xlsx       = try_read_keys_xlsx(app_config, rawdata_dir),
    sku_asin_xlsx   = try_read_sku_asin_xlsx(app_config, rawdata_dir),
    catalogue       = try_read_catalogue(app_config, db_paths),
    sales           = try_read_sales(app_config, db_paths)
  )

  resolve_company_product_master_from_sources(sources)
}

#' Resolver core (pure function, testable without external I/O).
#'
#' @param sources named list of data.frames keyed by source_origin values.
#' @return unified data.frame with source_origin column.
resolve_company_product_master_from_sources <- function(sources) {
  # --- Normalise each source to MASTER_SCHEMA_COLS + tag source_origin ----
  tagged <- list()
  for (name in names(sources)) {
    df <- sources[[name]]
    if (is.null(df) || nrow(df) == 0) next
    df_norm <- normalise_master_schema(df)
    df_norm$source_origin <- name
    tagged[[name]] <- df_norm
  }

  # --- Empty result case ---------------------------------------------------
  if (length(tagged) == 0) {
    warning(sprintf(
      "resolve_company_product_master: all sources are empty. sources were checked: %s",
      paste(names(sources), collapse = ", ")
    ), call. = FALSE)
    return(empty_master_df())
  }

  # --- Union + arbitrate ---------------------------------------------------
  all_rows <- do.call(rbind, tagged)

  # Lazy-load arbitrator if not already in scope (callers may source it
  # explicitly, but resolver can also be used standalone).
  if (!exists("arbitrate_product_master_conflicts", mode = "function")) {
    repo_root <- local({
      d <- getwd()
      while (!file.exists(file.path(d, ".spectra.yaml")) && d != "/") d <- dirname(d)
      if (d == "/") stop("Could not locate repo root for arbitrator lookup", call. = FALSE)
      d
    })
    sibling <- file.path(
      repo_root,
      "shared/global_scripts/05_etl_utils/amz/fn_arbitrate_product_master_conflicts.R"
    )
    source(sibling)
  }

  arbitrate_product_master_conflicts(all_rows)
}

# ==============================================================================
# Schema normalisation
# ==============================================================================

#' Coerce a source data.frame to master schema columns, filling missing with NA.
normalise_master_schema <- function(df) {
  # Column alias map — handles xlsx/Gsheet variations.
  alias_map <- c(
    "AMAZON SKU" = "sku",
    "amazon_sku" = "sku",
    "SKU" = "sku",
    "AMZ ASIN" = "amz_asin",
    "amz_asin" = "amz_asin",
    "ASIN" = "amz_asin",
    "asin" = "amz_asin",
    "ProductLine" = "product_line_id",
    "product_line" = "product_line_id",
    "Product Line" = "product_line_id",
    "品牌" = "brand",
    "Brand" = "brand",
    "成本" = "cost",
    "Cost" = "cost",
    "利潤" = "profit",
    "Profit" = "profit",
    "商品售價" = "product_name",
    "Name" = "product_name",
    "產品名稱" = "product_name"
  )

  # Rename known aliases to canonical names.
  for (alias in names(alias_map)) {
    canonical <- alias_map[[alias]]
    if (alias %in% names(df) && !canonical %in% names(df)) {
      names(df)[names(df) == alias] <- canonical
    }
  }

  # Ensure all schema columns exist.
  for (col in MASTER_SCHEMA_COLS) {
    if (!col %in% names(df)) {
      df[[col]] <- NA
    }
  }

  # Coerce numeric fields (cost, profit may come from Gsheet as "US$5.53")
  df$cost <- parse_numeric_safe(df$cost)
  df$profit <- parse_numeric_safe(df$profit)

  # Trim character fields
  char_cols <- setdiff(MASTER_SCHEMA_COLS, c("cost", "profit"))
  for (col in char_cols) {
    df[[col]] <- trimws(as.character(df[[col]]))
    df[[col]][!nzchar(df[[col]])] <- NA_character_
  }

  df[, MASTER_SCHEMA_COLS, drop = FALSE]
}

parse_numeric_safe <- function(x) {
  if (is.numeric(x)) return(x)
  cleaned <- gsub("[^0-9.-]", "", as.character(x))
  suppressWarnings(as.numeric(cleaned))
}

empty_master_df <- function() {
  df <- data.frame(
    sku = character(0), marketplace = character(0), amz_asin = character(0),
    product_line_id = character(0), brand = character(0),
    cost = numeric(0), profit = numeric(0),
    product_name = character(0), status = character(0), launch_date = character(0),
    source_origin = character(0),
    stringsAsFactors = FALSE
  )
  df
}

# ==============================================================================
# Source readers (guarded — return empty df on failure)
# ==============================================================================

try_read_gsheet_master <- function(app_config) {
  cfg <- app_config$platforms$amz$etl_sources$company_product_master
  if (is.null(cfg)) return(empty_master_df())

  tryCatch({
    if (!requireNamespace("googlesheets4", quietly = TRUE)) {
      warning("googlesheets4 not available; skipping gsheet_master", call. = FALSE)
      return(empty_master_df())
    }
    sheet_id <- cfg$sheet_id
    sheet_name <- cfg$sheet_name
    if (!nzchar(sheet_id) || !nzchar(sheet_name)) return(empty_master_df())

    df <- googlesheets4::read_sheet(
      googlesheets4::as_sheets_id(sheet_id),
      sheet = sheet_name,
      col_types = "c"
    )
    df <- as.data.frame(df, stringsAsFactors = FALSE)

    # Apply marketplace_default if marketplace column is missing or all NA.
    mp_default <- cfg$marketplace_default %||% "amz_us"
    if (!"marketplace" %in% names(df)) df$marketplace <- mp_default
    df$marketplace[is.na(df$marketplace) | !nzchar(as.character(df$marketplace))] <- mp_default

    df
  }, error = function(e) {
    warning(sprintf("gsheet_master read failed: %s (treating as empty)", e$message),
            call. = FALSE)
    empty_master_df()
  })
}

try_read_gsheet_sku_asin <- function(app_config) {
  # Legacy tab `SKU to ASIN` — financial data, no ASIN column per implementation
  # and no product_line_id. Treated as lowest-priority opportunistic source for
  # cost/profit where available.
  #
  # Detection: sheet_id + sheet_name come from existing product_categories or
  # (fallback) hardcoded probe.
  # For now, skip (no high-quality mapping data in this tab) but keep hook.
  empty_master_df()
}

try_read_keys_xlsx <- function(app_config, rawdata_dir) {
  cfg <- app_config$platforms$amz$etl_sources$keys
  if (is.null(cfg)) return(empty_master_df())
  if (tolower(cfg$source_type %||% "") != "excel") return(empty_master_df())

  path <- file.path(rawdata_dir, cfg$rawdata_path %||% "")
  if (!nzchar(cfg$rawdata_path) || !file.exists(path)) return(empty_master_df())

  tryCatch({
    df <- readxl::read_excel(path, col_types = "text")
    df <- as.data.frame(df, stringsAsFactors = FALSE)

    # KEYS.xlsx has no marketplace column — apply amz_us default.
    df$marketplace <- "amz_us"
    df
  }, error = function(e) {
    warning(sprintf("keys_xlsx read failed: %s (treating as empty)", e$message),
            call. = FALSE)
    empty_master_df()
  })
}

try_read_sku_asin_xlsx <- function(app_config, rawdata_dir) {
  cfg <- app_config$platforms$amz$etl_sources$sku_mapping
  if (is.null(cfg)) return(empty_master_df())
  if (tolower(cfg$source_type %||% "") != "excel") return(empty_master_df())

  path <- file.path(rawdata_dir, cfg$rawdata_path %||% "")
  if (!nzchar(cfg$rawdata_path) || !file.exists(path)) return(empty_master_df())

  tryCatch({
    # SKUtoASIN number.xlsx has title in row 1, blank in row 2, real header in row 3.
    # Task 4.2 will fix this upstream; resolver uses skip=2 to be resilient.
    df <- readxl::read_excel(path, col_types = "text", skip = 2)
    df <- as.data.frame(df, stringsAsFactors = FALSE)

    df$marketplace <- "amz_us"
    df
  }, error = function(e) {
    warning(sprintf("sku_asin_xlsx read failed: %s (treating as empty)", e$message),
            call. = FALSE)
    empty_master_df()
  })
}

`%||%` <- function(x, y) if (is.null(x) || (length(x) == 1 && is.na(x))) y else x

# ==============================================================================
# Track C — sales + catalogue source readers (MP155 Phase 3 derivation)
# ==============================================================================

#' Resolve a DB path from explicit db_paths arg or fall back to global db_path_list.
resolve_db_path <- function(name, db_paths = NULL) {
  if (!is.null(db_paths) && !is.null(db_paths[[name]])) return(db_paths[[name]])
  if (exists("db_path_list", envir = .GlobalEnv, inherits = FALSE)) {
    dpl <- get("db_path_list", envir = .GlobalEnv)
    if (!is.null(dpl[[name]])) return(dpl[[name]])
  }
  NULL
}

#' Read sales-derived (sku, marketplace, amz_asin) tuples via mode aggregation.
#'
#' Source: `df_amz_sales___transformed` in `transformed_data.duckdb`.
#' Aggregation: most-frequent ASIN per (sku, marketplace) within lookback window.
#' Lookback default 90 days; override via app_config
#' `platforms.amz.etl_sources.amz_sales_lookback_days`.
#'
#' Graceful degradation:
#'   - Missing DB / table   -> empty data.frame + message()
#'   - Missing sku/marketplace/amz_asin column -> stop() with actionable error
#'   - Missing date column (need one of order_date/purchase_date/time) -> stop()
try_read_sales <- function(app_config, db_paths = NULL, today = Sys.Date()) {
  db_path <- resolve_db_path("transformed_data", db_paths)
  if (is.null(db_path) || !file.exists(db_path)) {
    message("[try_read_sales] transformed_data.duckdb not found; sales source returning empty")
    return(empty_master_df())
  }

  if (!requireNamespace("DBI", quietly = TRUE) || !requireNamespace("duckdb", quietly = TRUE)) {
    message("[try_read_sales] DBI/duckdb not available; sales source returning empty")
    return(empty_master_df())
  }

  con <- NULL
  result <- tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)

    if (!DBI::dbExistsTable(con, "df_amz_sales___transformed")) {
      message("[try_read_sales] df_amz_sales___transformed not found in ", db_path,
              "; sales source returning empty")
      return(empty_master_df())
    }

    cols <- DBI::dbListFields(con, "df_amz_sales___transformed")
    if (!"sku" %in% cols) {
      stop(sprintf(
        "[try_read_sales] df_amz_sales___transformed missing required column 'sku' (db path: %s)",
        db_path
      ), call. = FALSE)
    }

    # ASIN column: accept canonical 'amz_asin' OR raw Amazon name 'asin'
    asin_candidates <- c("amz_asin", "asin", "ASIN")
    asin_match <- asin_candidates[asin_candidates %in% cols]
    if (length(asin_match) == 0) {
      stop(sprintf(
        "[try_read_sales] df_amz_sales___transformed missing required column 'amz_asin' (also tried aliases: %s) (db path: %s)",
        paste(setdiff(asin_candidates, "amz_asin"), collapse = ", "), db_path
      ), call. = FALSE)
    }
    asin_col <- asin_match[1]

    # Marketplace column: accept canonical 'marketplace' OR derive from
    # 'sales_channel' (Amazon's actual column, e.g. "Amazon.com" -> amz_us).
    has_marketplace <- "marketplace" %in% cols
    has_sales_channel <- "sales_channel" %in% cols
    if (!has_marketplace && !has_sales_channel) {
      stop(sprintf(
        "[try_read_sales] df_amz_sales___transformed missing required column 'marketplace' (also tried 'sales_channel' fallback) (db path: %s)",
        db_path
      ), call. = FALSE)
    }

    date_candidates <- c("order_date", "purchase_date", "time")
    matched <- date_candidates[date_candidates %in% cols]
    if (length(matched) == 0) {
      stop(sprintf(
        "[try_read_sales] df_amz_sales___transformed missing date column (need one of: %s) (db path: %s)",
        paste(date_candidates, collapse = ", "), db_path
      ), call. = FALSE)
    }
    date_col <- matched[1]

    lookback_days <- app_config$platforms$amz$etl_sources$amz_sales_lookback_days %||% 90L
    cutoff <- today - as.integer(lookback_days)

    # Pull raw rows with the columns we need (alias asin -> amz_asin in R).
    pull_cols <- c("sku", asin_col, date_col,
                   if (has_marketplace) "marketplace" else "sales_channel")
    raw <- tbl2(con, "df_amz_sales___transformed") %>%
      dplyr::filter(!is.na(.data[[date_col]]), .data[[date_col]] >= !!cutoff) %>%
      dplyr::filter(!is.na(.data[[asin_col]]), !is.na(sku)) %>%
      dplyr::select(dplyr::all_of(pull_cols)) %>%
      dplyr::collect()

    # Canonicalise asin column name
    if (asin_col != "amz_asin") {
      names(raw)[names(raw) == asin_col] <- "amz_asin"
    }
    # Derive marketplace from sales_channel when canonical missing
    if (!has_marketplace) {
      raw$marketplace <- map_sales_channel_to_marketplace(raw$sales_channel)
      raw$sales_channel <- NULL
      raw <- raw[!is.na(raw$marketplace), , drop = FALSE]  # drop Non-Amazon rows
    }

    counts <- raw %>%
      dplyr::group_by(sku, marketplace, amz_asin) %>%
      dplyr::summarise(n = dplyr::n(), .groups = "drop")

    aggregate_sales_mode(counts)
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("missing required column", msg, fixed = TRUE) ||
        grepl("missing date column", msg, fixed = TRUE)) {
      stop(e)
    }
    warning(sprintf("[try_read_sales] read failed: %s (treating as empty)", msg),
            call. = FALSE)
    empty_master_df()
  }, finally = {
    if (!is.null(con)) try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  })

  result
}

#' Pure helper: map Amazon's `sales_channel` strings (e.g. "Amazon.com",
#' "Amazon.co.uk") to canonical marketplace codes (amz_us, amz_uk, ...).
#'
#' Returns NA for non-Amazon channels (e.g. "Non-Amazon US"); callers should
#' drop NA rows so they don't pollute (sku, marketplace) aggregation.
map_sales_channel_to_marketplace <- function(sales_channel) {
  ch <- as.character(sales_channel)
  m <- rep(NA_character_, length(ch))
  m[ch == "Amazon.com"]    <- "amz_us"
  m[ch == "Amazon.ca"]     <- "amz_ca"
  m[ch == "Amazon.com.mx"] <- "amz_mx"
  m[ch == "Amazon.com.br"] <- "amz_br"
  m[ch == "Amazon.co.uk"]  <- "amz_uk"
  m[ch == "Amazon.de"]     <- "amz_de"
  m[ch == "Amazon.fr"]     <- "amz_fr"
  m[ch == "Amazon.it"]     <- "amz_it"
  m[ch == "Amazon.es"]     <- "amz_es"
  m[ch == "Amazon.nl"]     <- "amz_nl"
  m[ch == "Amazon.se"]     <- "amz_se"
  m[ch == "Amazon.pl"]     <- "amz_pl"
  m[ch == "Amazon.com.tr"] <- "amz_tr"
  m[ch == "Amazon.com.au"] <- "amz_au"
  m[ch == "Amazon.co.jp"]  <- "amz_jp"
  m[ch == "Amazon.sg"]     <- "amz_sg"
  m[ch == "Amazon.ae"]     <- "amz_ae"
  m[ch == "Amazon.in"]     <- "amz_in"
  m[ch == "Amazon.sa"]     <- "amz_sa"
  m[ch == "Amazon.eg"]     <- "amz_eg"
  # Non-Amazon* and any unmapped channel stays NA (caller drops these rows).
  m
}

#' Pure helper: pick the most-frequent ASIN per (sku, marketplace).
#' Tie-break: alphabetical sku/marketplace order, then row order on equal n.
aggregate_sales_mode <- function(counts) {
  if (is.null(counts) || nrow(counts) == 0) return(empty_master_df())
  for (col in c("sku", "marketplace", "amz_asin", "n")) {
    if (!col %in% names(counts)) {
      stop(sprintf("aggregate_sales_mode: missing column '%s'", col), call. = FALSE)
    }
  }

  counts <- as.data.frame(counts, stringsAsFactors = FALSE)
  ord <- order(counts$sku, counts$marketplace, -counts$n)
  counts <- counts[ord, , drop = FALSE]
  key <- paste(counts$sku, counts$marketplace, sep = "|")
  picked <- counts[!duplicated(key), c("sku", "marketplace", "amz_asin"), drop = FALSE]
  rownames(picked) <- NULL
  picked
}

#' Read catalogue (sku, marketplace, amz_asin) tuples from df_amz_product_master.
#'
#' Source: `df_amz_product_master` in `raw_data.duckdb`. This table is produced
#' by `amz_ETL_product_profiles_0IM.R` (unioning per-line `product_profile_<line>`
#' Gsheet tabs); the resolver only consumes, never rebuilds.
#'
#' Dedup: on (sku, marketplace) — first row kept, warning emitted on dupes.
#'
#' Graceful degradation:
#'   - Missing DB / table -> empty data.frame + message()
#'   - Missing required column -> stop() with actionable error
try_read_catalogue <- function(app_config, db_paths = NULL) {
  db_path <- resolve_db_path("raw_data", db_paths)
  if (is.null(db_path) || !file.exists(db_path)) {
    message("[try_read_catalogue] raw_data.duckdb not found; catalogue source returning empty")
    return(with_empty_pl_map(empty_master_df()))
  }

  if (!requireNamespace("DBI", quietly = TRUE) || !requireNamespace("duckdb", quietly = TRUE)) {
    message("[try_read_catalogue] DBI/duckdb not available; catalogue source returning empty")
    return(with_empty_pl_map(empty_master_df()))
  }

  con <- NULL
  result <- tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)

    if (!DBI::dbExistsTable(con, "df_amz_product_master")) {
      message("[try_read_catalogue] df_amz_product_master not found in ", db_path,
              "; catalogue source returning empty")
      return(with_empty_pl_map(empty_master_df()))
    }

    cols <- DBI::dbListFields(con, "df_amz_product_master")

    # marketplace + amz_asin are mandatory (catalogue is ASIN-keyed metadata).
    for (req in c("marketplace", "amz_asin")) {
      if (!req %in% cols) {
        stop(sprintf(
          "[try_read_catalogue] df_amz_product_master missing required column '%s' (db path: %s)",
          req, db_path
        ), call. = FALSE)
      }
    }

    # `sku` is optional in catalogue (some companies' product_profile tabs are
    # ASIN-keyed only and don't preserve SKU mapping). When absent, catalogue
    # cannot supply (sku, marketplace, amz_asin) tuples to the resolver, so
    # gracefully return empty + emit informational message. To enable
    # catalogue as a SKU<->ASIN source for these companies, enhance
    # amz_ETL_product_profiles_0IM.R to preserve the SKU column.
    if (!"sku" %in% cols) {
      message(
        "[try_read_catalogue] df_amz_product_master has no 'sku' column ",
        "(ASIN-keyed metadata only); catalogue source returning empty. ",
        "To enable catalogue as a SKU<->ASIN source, enhance ",
        "amz_ETL_product_profiles_0IM.R to preserve SKU."
      )
      return(with_empty_pl_map(empty_master_df()))
    }

    raw <- tbl2(con, "df_amz_product_master") %>%
      dplyr::select(sku, marketplace, amz_asin) %>%
      dplyr::collect()

    dedup_result <- dedup_catalogue(raw)

    # amz-mapping-gap-detection (Issue #471):
    # Project (sku, product_line_id) into attr so detect_anomalies() can
    # compute no_product_line gaps without a second DB read. Empty when
    # product_line_id column absent (catalogue only carries SKU<->ASIN).
    pl_map <- if ("product_line_id" %in% cols) {
      tbl2(con, "df_amz_product_master") %>%
        dplyr::select(sku, product_line_id) %>%
        dplyr::filter(!is.na(sku), !is.na(product_line_id), nzchar(sku), nzchar(product_line_id)) %>%
        dplyr::distinct() %>%
        dplyr::collect() %>%
        as.data.frame(stringsAsFactors = FALSE)
    } else {
      empty_pl_map_df()
    }
    attr(dedup_result, "sku_product_line_map") <- pl_map
    dedup_result
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("missing required column", msg, fixed = TRUE)) {
      stop(e)
    }
    warning(sprintf("[try_read_catalogue] read failed: %s (treating as empty)", msg),
            call. = FALSE)
    with_empty_pl_map(empty_master_df())
  }, finally = {
    if (!is.null(con)) try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  })

  result
}

# Helpers for amz-mapping-gap-detection (Issue #471) sku_product_line_map attr.
empty_pl_map_df <- function() {
  data.frame(sku = character(0), product_line_id = character(0),
             stringsAsFactors = FALSE)
}

with_empty_pl_map <- function(df) {
  attr(df, "sku_product_line_map") <- empty_pl_map_df()
  df
}

#' Pure helper: dedup catalogue rows by (sku, marketplace), warn on dupes.
dedup_catalogue <- function(raw) {
  if (is.null(raw) || nrow(raw) == 0) return(empty_master_df())

  raw <- as.data.frame(raw, stringsAsFactors = FALSE)
  for (col in c("sku", "marketplace", "amz_asin")) {
    if (!col %in% names(raw)) {
      stop(sprintf("dedup_catalogue: missing column '%s'", col), call. = FALSE)
    }
  }

  key <- paste(raw$sku, raw$marketplace, sep = "|")
  dup_keys <- unique(key[duplicated(key)])

  if (length(dup_keys) > 0) {
    examples <- head(dup_keys, 5)
    warning(sprintf(
      "[try_read_catalogue] %d (sku, marketplace) duplicate(s) in df_amz_product_master; keeping first row. Examples: %s",
      length(dup_keys), paste(examples, collapse = "; ")
    ), call. = FALSE)
  }

  picked <- raw[!duplicated(key), c("sku", "marketplace", "amz_asin"), drop = FALSE]
  rownames(picked) <- NULL
  picked
}
