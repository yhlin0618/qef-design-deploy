#' Arbitrate conflicts in company product master data from multiple sources.
#'
#' Implements Decision D4 (Hybrid conflict arbitration) of
#' qef-product-master-redesign + Decision 1 (Per-field priority via field_class)
#' of qef-gsheet-three-surface-redesign (MP155 application).
#'
#' Two orthogonal classification dimensions:
#'   (A) critical / non_critical:
#'       critical fields     -> fail ETL with stop() when conflicting (unless
#'                              PRODUCT_MASTER_STRICT_CONFLICT env var is "false")
#'       non-critical fields -> resolve by priority, log OVERRIDE
#'
#'   (B) field_class (MP155 Three Categories, optional):
#'       System-Sourced     -> resolve via priority_per_class.System-Sourced
#'                             (typically reverses default - file/system over manual)
#'       Human-Decided      -> default behavior (uses source_priority)
#'       System-Suggested   -> EXCLUDED from master output; routed to
#'                             attr(result, "system_suggested_queue") for
#'                             downstream review (fn_detect_anomalies)
#'       (field absent)     -> treated as Human-Decided (backwards-compat)
#'
#' Config: shared/global_scripts/30_global_data/parameters/scd_type1/product_master_fields.yaml
#'
#' @param rows data.frame with columns including sku, marketplace, source_origin,
#'             and the critical/non-critical fields listed in product_master_fields.yaml.
#'             Multiple rows for the same (sku, marketplace) are expected and
#'             represent per-source candidates.
#' @param config_path Optional override path to product_master_fields.yaml.
#'
#' @return data.frame with exactly one row per (sku, marketplace) pair,
#'         retaining the `source_origin` column identifying which source
#'         provided the authoritative row.
#'         When any field has field_class=System-Suggested in the config, the
#'         result also carries attr(result, "system_suggested_queue") - a
#'         data.frame of (sku, marketplace, field, value, source_origin,
#'         review_action) rows that did NOT make it into master and require
#'         human review.
#'
#' @export

arbitrate_product_master_conflicts <- function(rows,
                                               config_path = NULL) {
  if (nrow(rows) == 0) return(rows)

  # --- Load field classification config ------------------------------------
  if (is.null(config_path)) {
    repo_root <- find_repo_root()
    config_path <- file.path(
      repo_root,
      "shared/global_scripts/30_global_data/parameters/scd_type1/product_master_fields.yaml"
    )
  }

  cfg <- yaml::read_yaml(config_path)
  critical_fields <- cfg$company_product_master$critical %||% character(0)
  non_critical_fields <- cfg$company_product_master$non_critical %||% character(0)
  priority_order <- cfg$source_priority %||% c(
    "gsheet_master", "gsheet_sku_asin", "keys_xlsx", "sku_asin_xlsx"
  )

  # v2: per-field MP155 classification (optional; backwards-compat when absent)
  field_class_map <- cfg$company_product_master$field_class %||% list()
  priority_per_class <- cfg$company_product_master$priority_per_class %||% list()
  suggested_review_action <- priority_per_class$`System-Suggested`$review_action %||%
    "route_to_status_gsheet"

  strict_mode <- resolve_strict_mode(cfg$env_var_name %||% "PRODUCT_MASTER_STRICT_CONFLICT")

  # --- Identify System-Suggested fields (excluded from master output) ------
  suggested_fields <- names(field_class_map)[
    vapply(field_class_map, function(x) identical(x, "System-Suggested"), logical(1))
  ]

  # --- Partition rows by (sku, marketplace) --------------------------------
  rows$.key <- paste(rows$sku, rows$marketplace, sep = "\x1f")  # unit separator
  keys <- unique(rows$.key)

  per_key_results <- lapply(keys, function(k) {
    group <- rows[rows$.key == k, , drop = FALSE]
    arbitrate_single_key(group, critical_fields, non_critical_fields,
                         priority_order, strict_mode,
                         field_class_map, priority_per_class,
                         suggested_fields, suggested_review_action)
  })

  master_rows <- do.call(rbind, lapply(per_key_results, `[[`, "winner"))
  master_rows$.key <- NULL

  # Drop System-Suggested columns from master output (route to queue instead)
  for (f in suggested_fields) {
    if (f %in% names(master_rows)) master_rows[[f]] <- NULL
  }

  # Aggregate System-Suggested queue across all keys
  queue_chunks <- lapply(per_key_results, `[[`, "suggested_queue")
  queue_chunks <- queue_chunks[!vapply(queue_chunks, is.null, logical(1))]
  if (length(queue_chunks) > 0) {
    queue_df <- do.call(rbind, queue_chunks)
    attr(master_rows, "system_suggested_queue") <- queue_df
  }

  # Track C: aggregate System-Sourced override log across all keys
  override_chunks <- lapply(per_key_results, `[[`, "override_log")
  override_chunks <- override_chunks[!vapply(override_chunks, is.null, logical(1))]
  if (length(override_chunks) > 0) {
    override_df <- do.call(rbind, override_chunks)
    attr(master_rows, "system_sourced_override_log") <- override_df
  }

  master_rows
}

# ==============================================================================
# Internal helpers
# ==============================================================================

arbitrate_single_key <- function(group, critical_fields, non_critical_fields,
                                 priority_order, strict_mode,
                                 field_class_map = list(),
                                 priority_per_class = list(),
                                 suggested_fields = character(0),
                                 suggested_review_action = "route_to_status_gsheet") {
  # Default ranking by global source_priority (used for Human-Decided fields)
  default_rank <- function(g) {
    r <- match(g$source_origin, priority_order)
    g[order(r, na.last = TRUE), , drop = FALSE]
  }

  # Track C: collect System-Sourced override events for downstream surfacing
  override_log <- NULL

  if (nrow(group) == 1) {
    # Even singletons may have System-Suggested fields routed to queue
    suggested_queue <- collect_suggested_queue(group, suggested_fields,
                                               suggested_review_action)
    return(list(winner = group, suggested_queue = suggested_queue,
                override_log = override_log))
  }

  group <- default_rank(group)

  # --- Critical field conflict check (unchanged from v1) -------------------
  for (field in critical_fields) {
    if (!field %in% names(group)) next
    values <- group[[field]]
    distinct_non_na <- unique(values[!is.na(values) & nzchar(as.character(values))])
    if (length(distinct_non_na) > 1) {
      msg <- format_conflict_message(group, field)
      if (strict_mode) {
        stop(msg, call. = FALSE)
      } else {
        warning(msg, call. = FALSE, immediate. = TRUE)
      }
    }
  }

  # --- Default winner (highest priority by global source_priority) ---------
  winner <- group[1, , drop = FALSE]

  # --- v2: per-field priority override for fields with field_class --------
  # For each field where field_class is set, recompute the winner value
  # using priority_per_class[[field_class]] instead of the default ranking.
  reclassified_fields <- intersect(names(field_class_map),
                                   union(critical_fields, non_critical_fields))
  for (field in reclassified_fields) {
    if (!field %in% names(group)) next
    cls <- field_class_map[[field]]
    if (identical(cls, "Human-Decided")) next  # default ranking already used
    if (identical(cls, "System-Suggested")) next  # routed to queue, not master

    if (identical(cls, "System-Sourced")) {
      class_priority <- priority_per_class$`System-Sourced`
      if (is.null(class_priority) || length(class_priority) == 0) next
      reranked <- group[order(match(group$source_origin, class_priority),
                              na.last = TRUE), , drop = FALSE]
      # Skip System-Sourced rows whose value is NA (gap-filling fallback)
      reranked_nonNA <- reranked[!is.na(reranked[[field]]) &
                                 nzchar(as.character(reranked[[field]])), ,
                                 drop = FALSE]
      if (nrow(reranked_nonNA) == 0) next
      class_winner_val <- reranked_nonNA[[field]][1]
      class_winner_src <- reranked_nonNA$source_origin[1]
      if (is_different(winner[[field]], class_winner_val)) {
        # System-Sourced overrides default winner for this field only
        message(format_class_override_message(winner, reranked_nonNA[1, ],
                                              field, "System-Sourced"))
        # Record the override event for downstream Status Gsheet surfacing
        override_log <- rbind(override_log, data.frame(
          sku = winner$sku,
          marketplace = winner$marketplace,
          field = field,
          default_value = as.character(winner[[field]]),
          default_source = winner$source_origin,
          system_sourced_value = as.character(class_winner_val),
          system_sourced_source = class_winner_src,
          stringsAsFactors = FALSE
        ))
        winner[[field]] <- class_winner_val
        # Note: source_origin stays as the default winner's source_origin
        # since it represents the row's primary source. Per-field overrides
        # are recorded via OVERRIDE log for traceability.
      }
    }
  }

  # --- Non-critical: priority override (unchanged for non-classified) -----
  for (field in non_critical_fields) {
    if (!field %in% names(group)) next
    if (field %in% reclassified_fields) next  # handled above
    values <- group[[field]]
    winner_val <- winner[[field]]
    alt_rows <- group[-1, , drop = FALSE]
    for (i in seq_len(nrow(alt_rows))) {
      alt_val <- alt_rows[[field]][i]
      if (is_different(winner_val, alt_val)) {
        message(format_override_message(winner, alt_rows[i, ], field))
      }
    }
  }

  # --- v2: collect System-Suggested values for review queue ---------------
  suggested_queue <- collect_suggested_queue(group, suggested_fields,
                                             suggested_review_action)

  list(winner = winner, suggested_queue = suggested_queue,
       override_log = override_log)
}

#' Collect System-Suggested field values into a review queue data.frame.
#'
#' For each (sku, marketplace) group + each suggested field, emit one row per
#' source that has a non-NA value. The downstream consumer (fn_detect_anomalies)
#' decides how to surface these in Status Gsheet.
collect_suggested_queue <- function(group, suggested_fields, review_action) {
  if (length(suggested_fields) == 0) return(NULL)
  chunks <- list()
  for (field in suggested_fields) {
    if (!field %in% names(group)) next
    for (i in seq_len(nrow(group))) {
      val <- group[[field]][i]
      if (is.na(val) || !nzchar(as.character(val))) next
      chunks[[length(chunks) + 1]] <- data.frame(
        sku = group$sku[i],
        marketplace = group$marketplace[i],
        field = field,
        value = as.character(val),
        source_origin = group$source_origin[i],
        review_action = review_action,
        stringsAsFactors = FALSE
      )
    }
  }
  if (length(chunks) == 0) return(NULL)
  do.call(rbind, chunks)
}

is_different <- function(a, b) {
  if (is.na(a) && is.na(b)) return(FALSE)
  if (is.na(a) || is.na(b)) return(TRUE)
  !identical(a, b)
}

format_conflict_message <- function(group, field) {
  key <- sprintf("sku=%s marketplace=%s", group$sku[1], group$marketplace[1])
  source_vals <- sprintf("%s=%s", group$source_origin, group[[field]])
  paste0("CONFLICT: ", key, " field=", field, " ",
         paste(source_vals, collapse = " "))
}

format_override_message <- function(winner, loser, field) {
  sprintf(
    "OVERRIDE: sku=%s marketplace=%s field=%s %s=%s %s=%s chose=%s",
    winner$sku, winner$marketplace, field,
    winner$source_origin, winner[[field]],
    loser$source_origin, loser[[field]],
    winner$source_origin
  )
}

format_class_override_message <- function(winner, class_winner_row, field, cls) {
  sprintf(
    "OVERRIDE[%s]: sku=%s marketplace=%s field=%s %s=%s %s=%s chose=%s (per field_class)",
    cls,
    winner$sku, winner$marketplace, field,
    winner$source_origin, winner[[field]],
    class_winner_row$source_origin, class_winner_row[[field]],
    class_winner_row$source_origin
  )
}

resolve_strict_mode <- function(env_var) {
  val <- Sys.getenv(env_var, unset = NA_character_)
  if (is.na(val) || val == "") return(TRUE)  # Default: strict
  tolower(val) %in% c("true", "t", "1", "yes")
}

find_repo_root <- function() {
  d <- getwd()
  while (!file.exists(file.path(d, ".spectra.yaml")) && d != "/") d <- dirname(d)
  if (d == "/") stop("Could not locate repo root (.spectra.yaml not found)")
  d
}

`%||%` <- function(x, y) if (is.null(x)) y else x
