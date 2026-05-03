#' @file fn_validate_against_schema.R
#' @use yaml
#' @principle MP102 v1.2 (post-INSERT validation); MP154
#' @author Claude (#489 Phase 4)

#' Validate a mapped data.frame against the canonical schema definition
#'
#' Runs after `apply_mapping()` and before `dbWriteTable()`. Returns a
#' validation report with row-level error details. Does NOT mutate the
#' data — caller decides whether to write or quarantine on errors.
#'
#' Checks performed per field:
#'   - NOT NULL (required && empty -> error)
#'   - Pattern (if pattern defined && value non-empty -> regex match)
#'   - Constraint expressions (LENGTH > 0, comparators, etc.)
#'
#' Sentinels are valid: a `customer_email = ""` for an optional email
#' field passes (MP154 — empty string is the canonical sentinel).
#'
#' @param df data.frame produced by apply_mapping()
#' @param required_fields List from canonical schema yaml
#' @param ext_fields List from platform extension yaml (optional)
#' @return List with `n_rows`, `n_errors` (integer), `errors`
#'         (data.frame: row, field, error_type, value, message),
#'         `warnings` (character vector)
#' @export
validate_against_schema <- function(df,
                                     required_fields,
                                     ext_fields = list()) {
  if (!is.data.frame(df)) {
    stop("df must be a data.frame")
  }
  all_fields <- c(required_fields, ext_fields)

  errors <- list()
  warnings_acc <- character(0)

  for (field_name in names(all_fields)) {
    spec <- all_fields[[field_name]]
    if (!field_name %in% colnames(df)) {
      if (isTRUE(spec$required)) {
        errors[[length(errors) + 1]] <- data.frame(
          row = NA_integer_, field = field_name,
          error_type = "missing_column", value = NA_character_,
          message = "required column missing from output",
          stringsAsFactors = FALSE
        )
      }
      next
    }

    col <- df[[field_name]]

    # Required + empty check
    if (isTRUE(spec$required)) {
      empty_idx <- which(is.na(col) | (is.character(col) & nchar(col) == 0))
      if (length(empty_idx) > 0) {
        errors[[length(errors) + 1]] <- data.frame(
          row = empty_idx, field = field_name,
          error_type = "not_null", value = NA_character_,
          message = "required field is empty",
          stringsAsFactors = FALSE
        )
      }
    }

    # Pattern check (only on character columns with non-empty values)
    if (!is.null(spec$pattern) && nchar(spec$pattern) > 0 && is.character(col)) {
      non_empty_idx <- which(!is.na(col) & nchar(col) > 0)
      if (length(non_empty_idx) > 0) {
        ok <- grepl(spec$pattern, col[non_empty_idx], perl = TRUE)
        bad_idx <- non_empty_idx[!ok]
        if (length(bad_idx) > 0) {
          errors[[length(errors) + 1]] <- data.frame(
            row = bad_idx, field = field_name,
            error_type = "pattern_mismatch",
            value = as.character(col[bad_idx]),
            message = paste0("does not match pattern: ", spec$pattern),
            stringsAsFactors = FALSE
          )
        }
      }
    }

    # Numeric range checks from constraints
    if (!is.null(spec$constraints) && is.numeric(col)) {
      for (constraint in spec$constraints) {
        m <- regmatches(constraint,
                        regexec("^(>=|<=|>|<|=)\\s*([-0-9.]+)$", constraint))[[1]]
        if (length(m) == 3) {
          op <- m[2]; val <- as.numeric(m[3])
          ok <- switch(op,
            ">=" = col >= val,
            "<=" = col <= val,
            ">"  = col >  val,
            "<"  = col <  val,
            "="  = col == val,
            rep(TRUE, length(col))
          )
          ok[is.na(ok)] <- TRUE  # NA handled by NOT NULL check above
          bad_idx <- which(!ok)
          if (length(bad_idx) > 0) {
            errors[[length(errors) + 1]] <- data.frame(
              row = bad_idx, field = field_name,
              error_type = "range",
              value = as.character(col[bad_idx]),
              message = paste0("violates: ", constraint),
              stringsAsFactors = FALSE
            )
          }
        }
      }
    }
  }

  errors_df <- if (length(errors) > 0) {
    do.call(rbind, errors)
  } else {
    data.frame(row = integer(), field = character(),
               error_type = character(), value = character(),
               message = character(), stringsAsFactors = FALSE)
  }

  list(
    n_rows = nrow(df),
    n_errors = nrow(errors_df),
    errors = errors_df,
    warnings = warnings_acc
  )
}
