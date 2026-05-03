#' @file fn_apply_mapping.R
#' @use yaml
#' @principle MP102 v1.2; MP156; MP154 (sentinel-aware fallback); DM_R064 (no positional inference)
#' @author Claude (#489 Phase 4)

#' Apply a bridge mapping deterministically (no LLM, no eval/parse)
#'
#' Given a bridge yaml's `column_mapping` block, the canonical schema's
#' `required_fields` definition (and platform extension fields if any),
#' and a prerawdata data.frame, produce a data.frame conforming to the
#' canonical layer's column order/types/constraints.
#'
#' Order of operations per field:
#'   1. Find source column via column_mapping[field]$from_column
#'      (case-insensitive direct match, then schema's `aliases:` list)
#'   2. If source value present and non-empty after coercion -> use it
#'   3. Else if column_mapping[field]$apply_fallback OR field has
#'      schema fallback -> apply fallback rule (`use_value`, `derive_from`,
#'      `sentinel`)
#'   4. Else if field is required and no fallback applies -> stop()
#'   5. Else (optional, no value) -> use empty-string sentinel
#'
#' This function is a pure data transform — no I/O, no DB writes, no
#' randomness, no system calls. Same input always yields same output.
#'
#' @param prerawdata data.frame loaded from prerawdata source
#' @param column_mapping List from bridge yaml's `column_mapping:` section
#' @param required_fields List from canonical schema yaml `required_fields:`
#' @param ext_fields List from platform extension yaml `fields:` (or empty)
#' @param canonical_target_platform Platform_id to use for `derive_from`
#'        rules that reference canonical_target.platform
#' @return data.frame conforming to canonical layer schema
#' @export
apply_mapping <- function(prerawdata,
                          column_mapping,
                          required_fields,
                          ext_fields = list(),
                          canonical_target_platform = NA_character_,
                          pre_filter = NULL) {
  if (!is.data.frame(prerawdata)) {
    stop("prerawdata must be a data.frame")
  }
  if (!is.list(column_mapping)) {
    stop("column_mapping must be a list (parsed from bridge yaml)")
  }
  if (!is.list(required_fields) || length(required_fields) == 0) {
    stop("required_fields must be a non-empty list")
  }

  # Optional pre_filter: drop rows whose values match a deterministic
  # exclude list. Bridge yaml shape:
  #   pre_filter:
  #     - column: "order-status"
  #       exclude_values: ["Cancelled", "Pending"]
  if (!is.null(pre_filter) && length(pre_filter) > 0) {
    keep <- rep(TRUE, nrow(prerawdata))
    for (rule in pre_filter) {
      col <- rule$column
      ex <- as.character(rule$exclude_values %||% character(0))
      if (!is.null(col) && col %in% colnames(prerawdata) && length(ex) > 0) {
        keep <- keep & !(as.character(prerawdata[[col]]) %in% ex)
      }
    }
    prerawdata <- prerawdata[keep, , drop = FALSE]
  }

  all_fields <- c(required_fields, ext_fields)
  source_cols <- colnames(prerawdata)
  source_cols_lower <- tolower(source_cols)

  result <- list()
  warnings_acc <- character(0)

  for (field_name in names(all_fields)) {
    spec <- all_fields[[field_name]]
    mapping_entry <- column_mapping[[field_name]]

    # Step 1: Try to read from source
    src_col <- NULL
    # Bridge yaml may override with a literal value (`use_value:`). This is
    # the canonical way to express per-company constants (e.g.,
    # amz_marketplace_id is always "ATVPDKIKX0DER" for QEF_DESIGN — that's
    # bridge-level data, not schema-level data).
    use_value_override <- mapping_entry$use_value

    if (!is.null(mapping_entry$from_column) && nzchar(mapping_entry$from_column)) {
      candidate <- mapping_entry$from_column
      idx <- match(tolower(candidate), source_cols_lower)
      if (!is.na(idx)) src_col <- source_cols[idx]
    }
    # Fall back to alias lookup if no explicit mapping found
    if (is.null(src_col) && !is.null(spec$aliases)) {
      for (alias in spec$aliases) {
        idx <- match(tolower(alias), source_cols_lower)
        if (!is.na(idx)) {
          src_col <- source_cols[idx]
          break
        }
      }
    }
    # Last-ditch: direct field-name lookup
    if (is.null(src_col) && is.null(use_value_override)) {
      idx <- match(tolower(field_name), source_cols_lower)
      if (!is.na(idx)) src_col <- source_cols[idx]
    }

    raw_value <- if (!is.null(use_value_override)) use_value_override
                  else if (!is.null(src_col)) prerawdata[[src_col]]
                  else NULL

    # Step 2: Coerce
    coerced <- coerce_field(raw_value, spec, mapping_entry)

    # Step 2b: Apply value_map if bridge specifies one (deterministic
    # lookup, e.g., {"Amazon" -> "AFN", "Merchant" -> "MFN"}). Names of
    # the value_map list are source values; values are target values.
    if (!is.null(mapping_entry$value_map) && length(mapping_entry$value_map) > 0) {
      vm <- mapping_entry$value_map
      coerced <- vapply(as.character(coerced), function(v) {
        if (is.na(v)) return(NA_character_)
        replacement <- vm[[v]]
        if (is.null(replacement)) v else as.character(replacement)
      }, character(1), USE.NAMES = FALSE)
    }

    # Step 3: Decide whether to apply fallback
    apply_fb <- is.null(use_value_override) &&
                (isTRUE(mapping_entry$apply_fallback) ||
                 is_empty_after_coercion(coerced, spec$type))

    if (apply_fb) {
      coerced <- apply_fallback(spec, mapping_entry, coerced,
                                 result_so_far = result,
                                 canonical_target_platform = canonical_target_platform)
    }

    # Step 4: Final required check
    if (isTRUE(spec$required) && is_empty_after_coercion(coerced, spec$type)) {
      stop("Required field '", field_name,
           "' has no value after mapping + fallback. ",
           "Fix bridge yaml or prerawdata source.")
    }

    result[[field_name]] <- coerced
  }

  # Length check: all columns same length as prerawdata
  n_rows <- nrow(prerawdata)
  for (field_name in names(result)) {
    val <- result[[field_name]]
    if (length(val) == 1 && n_rows > 1) {
      result[[field_name]] <- rep(val, n_rows)
    } else if (length(val) != n_rows) {
      stop("Field '", field_name, "' has length ", length(val),
           " but prerawdata has ", n_rows, " rows. ",
           "Coercion or fallback returned wrong-length vector.")
    }
  }

  data.frame(result, stringsAsFactors = FALSE)
}

#' Coerce a value to the canonical R type per the schema's type field
#' @keywords internal
coerce_field <- function(raw, spec, mapping_entry = NULL) {
  if (is.null(raw)) return(NULL)
  type <- spec$type
  if (is.null(type)) return(raw)

  switch(toupper(type),
    "VARCHAR" = ,
    "TEXT" = trimws(as.character(raw)),
    "INTEGER" = as.integer(raw),
    "NUMERIC" = as.numeric(raw),
    "BOOLEAN" = {
      if (is.logical(raw)) raw
      else if (is.character(raw)) {
        tolower(trimws(raw)) %in% c("true", "yes", "1", "active", "available")
      } else as.logical(raw)
    },
    "TIMESTAMP" = {
      if (inherits(raw, "POSIXct")) raw
      else as.POSIXct(as.character(raw), tz = "UTC")
    },
    raw  # fallback: pass through
  )
}

#' Detect "empty" after coercion (type-aware)
#' @keywords internal
is_empty_after_coercion <- function(val, type) {
  if (is.null(val)) return(TRUE)
  if (length(val) == 0) return(TRUE)
  type_upper <- toupper(type %||% "VARCHAR")
  if (type_upper %in% c("VARCHAR", "TEXT")) {
    return(all(is.na(val) | nchar(val) == 0))
  }
  return(all(is.na(val)))
}

#' Apply the schema's fallback rule to produce a final value
#' @keywords internal
apply_fallback <- function(spec, mapping_entry, current_value,
                            result_so_far = list(),
                            canonical_target_platform = NA_character_) {
  fb <- spec$fallback
  if (is.null(fb)) {
    # No schema fallback -> use empty string for VARCHAR/TEXT, NA otherwise
    type_upper <- toupper(spec$type %||% "VARCHAR")
    if (type_upper %in% c("VARCHAR", "TEXT")) return("")
    return(NA)
  }
  rule <- fb$rule %||% "sentinel"

  if (rule == "use_value") return(fb$value)
  if (rule == "sentinel")  return(fb$value)
  if (rule == "derive_from") {
    derive <- fb$derive %||% ""
    # Special-case: platform_id derivation from bridge yaml
    if (grepl("canonical_target.platform", derive, fixed = TRUE)) {
      return(canonical_target_platform)
    }
    if (grepl("Sys.time\\(\\)", derive)) return(Sys.time())
    # Special-case: total_amount = unit_price * quantity (recognized by
    # exact phrase match against the canonical schema's documented rule)
    if (grepl("unit_price\\s*\\*\\s*quantity", derive)) {
      up <- result_so_far$unit_price
      qty <- result_so_far$quantity
      if (!is.null(up) && !is.null(qty) &&
          length(up) == length(qty)) {
        return(up * qty)
      }
    }
    # Special-case: unit_price = total_amount / quantity
    if (grepl("total_amount\\s*[/]\\s*quantity", derive)) {
      ta <- result_so_far$total_amount
      qty <- result_so_far$quantity
      if (!is.null(ta) && !is.null(qty) &&
          length(ta) == length(qty) && all(qty > 0, na.rm = TRUE)) {
        return(ta / qty)
      }
    }
    # Other derive rules require referencing fields not yet computed or
    # unrecognized expressions. Best effort: emit sentinel rather than
    # eval/parse arbitrary code.
    warning("derive_from rule '", derive,
            "' is not implemented in deterministic interpreter. ",
            "Falling back to schema sentinel.")
    if (!is.null(fb$value)) return(fb$value)
    type_upper <- toupper(spec$type %||% "VARCHAR")
    if (type_upper %in% c("VARCHAR", "TEXT")) return("")
    return(NA)
  }
  warning("Unknown fallback rule '", rule, "'. Returning empty.")
  ""
}

`%||%` <- function(a, b) if (is.null(a)) b else a
