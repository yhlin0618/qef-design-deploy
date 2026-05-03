#' Detect anomalies, missing-master, and drift records for Status Gsheet.
#'
#' Implements the "anomaly detection" capability of MP155 Status Surface
#' (qef-gsheet-three-surface-redesign Decision 4 / 7).
#'
#' Produces three data.frames consumed by fn_write_status_gsheet:
#'
#'   - anomalies:      conflicts emitted during arbitration (per-field
#'                     OVERRIDE), plus System-Suggested values routed to queue.
#'   - missing_master: SKUs present in sales / activity sources but absent
#'                     from the resolved master.
#'   - drift:          recent value changes for fields with field_class set
#'                     to System-Sourced (snapshot diff vs prior run).
#'
#' Every output row carries a `suggested_action` column whose text routes the
#' business to the Master Gsheet (Action-as-Input pattern - Status Gsheet has
#' no buttons; the business mutates Master to resolve anomalies).
#'
#' @param resolved_master data.frame returned by
#'        fn_resolve_company_product_master + arbitrate_product_master_conflicts.
#'        MAY carry attr(, "system_suggested_queue") from the arbitrator.
#' @param sales_skus character vector of SKUs observed in sales / activity
#'        sources. Used to compute missing-master. NULL skips that output.
#' @param prior_master Optional data.frame snapshot of the previous run's
#'        resolved master (for drift detection). NULL skips drift output.
#' @param drift_fields Optional character vector of fields to track for drift.
#'        Defaults to all fields with field_class=System-Sourced when present.
#' @param config_path Optional override path to product_master_fields.yaml.
#'
#' @return Named list with elements `anomalies`, `missing_master`, `drift` -
#'         each a data.frame (possibly with 0 rows) with a `suggested_action`
#'         column. Any NULL inputs produce a NULL element in the result list.
#'
#' @export

detect_anomalies <- function(resolved_master,
                             sales_skus = NULL,
                             prior_master = NULL,
                             sales_sku_asin_pairs = NULL,
                             drift_fields = NULL,
                             config_path = NULL) {

  # Track C: merge anomalies from System-Suggested queue + System-Sourced
  # override log into a single anomalies output.
  anomalies <- rbind(
    build_anomalies_from_queue(resolved_master),
    build_anomalies_from_override_log(resolved_master)
  )
  rownames(anomalies) <- NULL

  # amz-mapping-gap-detection (Issue #471): mapping_gaps element only
  # populated when sales_sku_asin_pairs supplied. Backwards compat:
  # callers without the new parameter see no mapping_gaps in output.
  mapping_gaps <- if (!is.null(sales_sku_asin_pairs)) {
    sku_pl_map <- attr(resolved_master, "sku_product_line_map")
    if (is.null(sku_pl_map)) {
      sku_pl_map <- data.frame(sku = character(0),
                               product_line_id = character(0),
                               stringsAsFactors = FALSE)
    }
    build_mapping_gaps(resolved_master, sales_sku_asin_pairs, sku_pl_map)
  } else NULL

  out <- list(
    anomalies = anomalies,
    missing_master = if (!is.null(sales_skus))
      build_missing_master(resolved_master, sales_skus) else NULL,
    mapping_gaps = mapping_gaps,
    drift = if (!is.null(prior_master))
      build_drift(resolved_master, prior_master, drift_fields, config_path) else NULL
  )

  out
}

#' Detect mapping gaps between sales transactions and the resolved master.
#'
#' Implements amz-mapping-gap-detection (Issue #471):
#'   - no_master_row:  sales (sku, amz_asin) tuple has no row in master
#'   - no_product_line: master row exists but product_line_id is NA
#'   - sku_asin_mismatch: phase 2, requires #469 to land
#'
#' @param resolved_master data.frame from resolve_company_product_master.
#'        MAY carry attr "sku_product_line_map".
#' @param sales_pairs data.frame with columns (sku, amz_asin, marketplace).
#'        sku or amz_asin may be NA per row but at least one must be non-NA.
#'        Optional column `sales_volume` (numeric) drives cap ordering when
#'        gap count exceeds `cap`; missing column treated as 0 for ordering.
#' @param sku_pl_map data.frame with columns (sku, product_line_id) for
#'        catalogue-validated SKUs. Empty -> no_product_line check skipped.
#' @param cap integer; maximum gap rows to retain. When the unbounded count
#'        exceeds cap, the top `cap` rows by sales_volume desc are kept, and
#'        one summary row with gap_type="...and N more" is appended. Default
#'        500 (Status Gsheet write performance budget; see Decision 4 / R1).
#'
#' @return data.frame(sku, amz_asin, marketplace, gap_type, suggested_action).
#'         0 rows when no gaps. gap_type in {"no_master_row","no_product_line"}
#'         plus literal "...and N more" for the summary row when capped.
build_mapping_gaps <- function(resolved_master, sales_pairs, sku_pl_map,
                               cap = 500L) {
  empty <- empty_mapping_gaps_df()
  if (is.null(sales_pairs) || nrow(sales_pairs) == 0) return(empty)

  # Coerce + normalise inputs
  sp <- as.data.frame(sales_pairs, stringsAsFactors = FALSE)
  for (col in c("sku", "amz_asin", "marketplace")) {
    if (!col %in% names(sp)) {
      stop(sprintf("build_mapping_gaps: sales_pairs missing column '%s'", col),
           call. = FALSE)
    }
  }
  sp$sku <- as.character(sp$sku)
  sp$amz_asin <- as.character(sp$amz_asin)
  sp$marketplace <- as.character(sp$marketplace)
  sp$sales_volume <- if ("sales_volume" %in% names(sp))
    suppressWarnings(as.numeric(sp$sales_volume)) else 0

  master_skus <- if ("sku" %in% names(resolved_master))
    as.character(resolved_master$sku) else character(0)
  master_asins <- if ("amz_asin" %in% names(resolved_master))
    as.character(resolved_master$amz_asin) else character(0)

  # ---------------- no_master_row ----------------
  has_sku <- !is.na(sp$sku) & nzchar(sp$sku)
  has_asin <- !is.na(sp$amz_asin) & nzchar(sp$amz_asin)

  # SKU side: SKU non-NA, not in master_skus
  sku_missing <- has_sku & !(sp$sku %in% master_skus)
  # ASIN side: SKU NA but ASIN non-NA, ASIN not in master_asins
  asin_only_missing <- !has_sku & has_asin & !(sp$amz_asin %in% master_asins)

  parts <- list()

  if (any(sku_missing)) {
    rows <- sp[sku_missing, , drop = FALSE]
    base_actions <- sprintf(
      "sku=%s 出現在銷售但 master 沒對應 row;請在 KEYS.xlsx + Master Gsheet 新增此 SKU 的 product_line_id / brand 等。",
      rows$sku
    )
    # #472: when sku is ASIN-shaped, this is the Amazon auto-fallback case
    # (seller has no merchant SKU; platform fills sku with the ASIN). The
    # SKU is immutable for active listings, so source-side fix is infeasible.
    # Pipeline normalize already addresses it via amz_ETL_sales_2TR.R Step 2.5.
    is_asin_shaped <- grepl("^B0[A-Z0-9]{8}$", rows$sku)
    fallback_note <- paste0(
      " | Amazon auto-fallback (immutable for active listings) — ",
      "addressed by amz_ETL_sales_2TR.R Step 2.5 backfill_asin_from_sku, not source."
    )
    suggested <- ifelse(is_asin_shaped,
                        paste0(base_actions, fallback_note),
                        base_actions)
    parts[[length(parts) + 1]] <- data.frame(
      sku = rows$sku,
      amz_asin = rows$amz_asin,
      marketplace = rows$marketplace,
      gap_type = "no_master_row",
      suggested_action = suggested,
      sales_volume = rows$sales_volume,
      stringsAsFactors = FALSE
    )
  }

  if (any(asin_only_missing)) {
    rows <- sp[asin_only_missing, , drop = FALSE]
    parts[[length(parts) + 1]] <- data.frame(
      sku = rep(NA_character_, nrow(rows)),
      amz_asin = rows$amz_asin,
      marketplace = rows$marketplace,
      gap_type = "no_master_row",
      suggested_action = sprintf(
        "amz_asin=%s 出現在銷售但 master 沒對應 row;請到 Amazon listing 確認對應 SKU 並加進 KEYS.xlsx。",
        rows$amz_asin
      ),
      sales_volume = rows$sales_volume,
      stringsAsFactors = FALSE
    )
  }

  # ---------------- no_product_line ----------------
  # Catalogue empty -> skip with informational message (Spec Requirement:
  # Catalogue empty graceful degradation in mapping gap detection).
  if (is.null(sku_pl_map) || nrow(sku_pl_map) == 0) {
    message(
      "[build_mapping_gaps] catalogue empty, skipping no_product_line ",
      "gap detection until #469 resolved (df_amz_product_master needs SKU column)."
    )
  } else {
    # For SKUs that ARE in master (so no_master_row didn't trigger), check
    # whether master.product_line_id is NA AND catalogue can validate.
    sku_in_master <- has_sku & !sku_missing
    if (any(sku_in_master) && "product_line_id" %in% names(resolved_master)) {
      candidates <- sp[sku_in_master, , drop = FALSE]
      # Look up product_line_id for these candidate SKUs in master
      master_pl <- resolved_master[, c("sku", "product_line_id"), drop = FALSE]
      master_pl$sku <- as.character(master_pl$sku)
      # Use first match per SKU (master may have multi-marketplace rows)
      first_pl <- master_pl[!duplicated(master_pl$sku), , drop = FALSE]
      lookup <- setNames(as.character(first_pl$product_line_id), first_pl$sku)

      cand_pl <- lookup[candidates$sku]
      pl_is_na <- is.na(cand_pl) | !nzchar(cand_pl)

      if (any(pl_is_na)) {
        rows <- candidates[pl_is_na, , drop = FALSE]
        parts[[length(parts) + 1]] <- data.frame(
          sku = rows$sku,
          amz_asin = rows$amz_asin,
          marketplace = rows$marketplace,
          gap_type = "no_product_line",
          suggested_action = sprintf(
            "sku=%s 在 master 有 row 但 product_line_id 是 NA;請到 coding sheet 對應 product_line tab 加入此 SKU 對應的 ASIN。",
            rows$sku
          ),
          sales_volume = rows$sales_volume,
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (length(parts) == 0) return(empty)
  result <- do.call(rbind, parts)
  rownames(result) <- NULL

  # ---------------- cap (Decision 4 / Risk R1) ----------------
  if (!is.null(cap) && is.finite(cap) && nrow(result) > cap) {
    n_total <- nrow(result)
    n_more <- n_total - cap
    # Sort by sales_volume desc; NA last so real volumes win
    ord <- order(-result$sales_volume, na.last = TRUE)
    result <- result[ord, , drop = FALSE]
    capped <- result[seq_len(cap), , drop = FALSE]
    summary_row <- data.frame(
      sku = NA_character_,
      amz_asin = NA_character_,
      marketplace = NA_character_,
      gap_type = sprintf("...and %d more", n_more),
      suggested_action = sprintf(
        "Status Gsheet 為 top %d 筆 (依 sales_volume 排序);完整 %d 筆請執行 audit script 或下載 CSV (詳見 onboarding doc).",
        cap, n_total
      ),
      sales_volume = NA_real_,
      stringsAsFactors = FALSE
    )
    result <- rbind(capped, summary_row)
    rownames(result) <- NULL
  }

  # Drop internal sales_volume column to match documented output schema
  result$sales_volume <- NULL
  result
}

empty_mapping_gaps_df <- function() {
  data.frame(
    sku = character(0), amz_asin = character(0), marketplace = character(0),
    gap_type = character(0), suggested_action = character(0),
    stringsAsFactors = FALSE
  )
}

# ==============================================================================
# Internal builders
# ==============================================================================

build_anomalies_from_queue <- function(resolved_master) {
  queue <- attr(resolved_master, "system_suggested_queue")
  if (is.null(queue) || nrow(queue) == 0) {
    return(empty_anomalies_df())
  }
  data.frame(
    sku = queue$sku,
    marketplace = queue$marketplace,
    field = queue$field,
    suggested_value = queue$value,
    source_origin = queue$source_origin,
    review_action = queue$review_action,
    suggested_action = vapply(seq_len(nrow(queue)), function(i) {
      sprintf(
        "Master Gsheet 補上 sku=%s marketplace=%s 的 %s(%s 建議值: %s);請在 Master Gsheet 對應 row 補上正確值,讓系統不再需要建議。",
        queue$sku[i], queue$marketplace[i], queue$field[i],
        queue$source_origin[i], queue$value[i]
      )
    }, character(1)),
    stringsAsFactors = FALSE
  )
}

#' Track C: emit anomaly rows from `system_sourced_override_log` attribute.
#'
#' Each entry in the log records an event where a System-Sourced source
#' (sales / catalogue) overrode the default-priority winner. The anomaly row
#' uses `review_action = "system_sourced_override"` and a suggested_action
#' that names BOTH the system value and the manual value, instructing the
#' business to either update KEYS.xlsx or annotate the SKU as trial in
#' Master Gsheet.
build_anomalies_from_override_log <- function(resolved_master) {
  log <- attr(resolved_master, "system_sourced_override_log")
  if (is.null(log) || nrow(log) == 0) {
    return(empty_anomalies_df())
  }
  data.frame(
    sku = log$sku,
    marketplace = log$marketplace,
    field = log$field,
    suggested_value = log$system_sourced_value,
    source_origin = log$system_sourced_source,
    review_action = "system_sourced_override",
    suggested_action = vapply(seq_len(nrow(log)), function(i) {
      sprintf(
        "System-Sourced 衝突: %s 來源觀察到 %s='%s',但 KEYS.xlsx 記錄為 '%s'。請更新 KEYS.xlsx 改成 '%s' (若 sales 是對的) 或在 Master Gsheet 對應 row 標 status='trial' (若 KEYS 是對的、sales 為臨時值)。",
        log$system_sourced_source[i],
        log$field[i],
        log$system_sourced_value[i],
        log$default_value[i],
        log$system_sourced_value[i]
      )
    }, character(1)),
    stringsAsFactors = FALSE
  )
}

build_missing_master <- function(resolved_master, sales_skus) {
  master_skus <- if ("sku" %in% names(resolved_master)) unique(resolved_master$sku)
                 else character(0)
  missing <- setdiff(sales_skus, master_skus)
  if (length(missing) == 0) return(empty_missing_df())
  data.frame(
    sku = missing,
    suggested_action = sprintf(
      "sku=%s 出現在銷售資料但 Master Gsheet 沒對應 row;請在 Master Gsheet 新增此 SKU 的 product_line_id / brand 等 Human-Decided 欄位。",
      missing
    ),
    stringsAsFactors = FALSE
  )
}

build_drift <- function(resolved_master, prior_master, drift_fields, config_path) {
  fields <- drift_fields %||% load_system_sourced_fields(config_path)
  if (length(fields) == 0) return(empty_drift_df())

  # Inner join on sku + marketplace where available
  key_cols <- intersect(c("sku", "marketplace"),
                        intersect(names(resolved_master), names(prior_master)))
  if (length(key_cols) == 0) return(empty_drift_df())

  joined <- merge(resolved_master, prior_master,
                  by = key_cols, suffixes = c(".now", ".prior"),
                  all = FALSE)

  drift_chunks <- list()
  for (f in fields) {
    fn <- paste0(f, ".now")
    fp <- paste0(f, ".prior")
    if (!fn %in% names(joined) || !fp %in% names(joined)) next
    diff_idx <- which(mapply(is_different, joined[[fn]], joined[[fp]]))
    if (length(diff_idx) == 0) next
    drift_chunks[[length(drift_chunks) + 1]] <- data.frame(
      sku = joined$sku[diff_idx],
      marketplace = joined$marketplace[diff_idx],
      field = f,
      prior_value = as.character(joined[[fp]][diff_idx]),
      current_value = as.character(joined[[fn]][diff_idx]),
      suggested_action = sprintf(
        "System-Sourced 欄位 %s 在 sku=%s marketplace=%s 從 '%s' 變成 '%s';如果這是 sales 端的合理變化,無需動作;如果業務認為不對,請查 Master Gsheet 對應 row。",
        f, joined$sku[diff_idx], joined$marketplace[diff_idx],
        joined[[fp]][diff_idx], joined[[fn]][diff_idx]
      ),
      stringsAsFactors = FALSE
    )
  }

  if (length(drift_chunks) == 0) return(empty_drift_df())
  do.call(rbind, drift_chunks)
}

# ==============================================================================
# Helpers
# ==============================================================================

empty_anomalies_df <- function() {
  data.frame(
    sku = character(0), marketplace = character(0), field = character(0),
    suggested_value = character(0), source_origin = character(0),
    review_action = character(0), suggested_action = character(0),
    stringsAsFactors = FALSE
  )
}

empty_missing_df <- function() {
  data.frame(sku = character(0), suggested_action = character(0),
             stringsAsFactors = FALSE)
}

empty_drift_df <- function() {
  data.frame(
    sku = character(0), marketplace = character(0), field = character(0),
    prior_value = character(0), current_value = character(0),
    suggested_action = character(0),
    stringsAsFactors = FALSE
  )
}

load_system_sourced_fields <- function(config_path) {
  if (is.null(config_path)) {
    repo_root <- find_repo_root_for_anomalies()
    config_path <- file.path(
      repo_root,
      "shared/global_scripts/30_global_data/parameters/scd_type1/product_master_fields.yaml"
    )
  }
  if (!file.exists(config_path)) return(character(0))
  cfg <- yaml::read_yaml(config_path)
  fc <- cfg$company_product_master$field_class %||% list()
  names(fc)[vapply(fc, function(x) identical(x, "System-Sourced"), logical(1))]
}

is_different <- function(a, b) {
  if (is.na(a) && is.na(b)) return(FALSE)
  if (is.na(a) || is.na(b)) return(TRUE)
  !identical(a, b)
}

find_repo_root_for_anomalies <- function() {
  d <- getwd()
  while (!file.exists(file.path(d, ".spectra.yaml")) && d != "/") d <- dirname(d)
  if (d == "/") stop("Could not locate repo root (.spectra.yaml not found)")
  d
}

`%||%` <- function(x, y) if (is.null(x)) y else x
