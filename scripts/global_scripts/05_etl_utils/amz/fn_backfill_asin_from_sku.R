# ==============================================================================
# fn_backfill_asin_from_sku.R
#
# Backfill empty `asin` from ASIN-shaped `sku` (#472).
#
# Amazon Seller Central auto-fallback: when a seller has no merchant SKU for
# an active listing, the platform populates the `sku` column with the ASIN.
# Because the SKU is immutable for active listings with sales history (changing
# it requires rebuilding the listing and losing review/sales history), we
# accept the auto-fallback and merely fill the empty `asin` column.
#
# This is non-destructive (MP154-compliant): we never overwrite a non-empty
# `asin`, and we never modify `sku`. We only fill rows where:
#   - sku matches `^B0[A-Z0-9]{8}$` (Amazon ASIN shape)
#   - asin is NA or empty string
#
# Cross-company impact: companies without amz_mx (or amz_us) listings that
# trigger this fallback will see no rows match the predicate -> no-op.
#
# Tested by: 98_test/etl/test_amz_472_asin_normalization.R
# ==============================================================================

#' Backfill `asin` from ASIN-shaped `sku`
#'
#' @param dt A data.frame with at least `sku` and `asin` columns. Other columns
#'   are preserved untouched. Missing column(s) -> returns dt unchanged.
#' @param verbose Logical. If TRUE, emits one `message()` reporting the
#'   backfill count (suppressed when count == 0). Default FALSE.
#'
#' @return The data.frame with `asin` populated for matching rows. `sku` is
#'   never modified. Row count and column order are preserved.
#'
#' @export
backfill_asin_from_sku <- function(dt, verbose = FALSE) {
  # Defensive: missing columns or empty -> no-op
  if (!all(c("sku", "asin") %in% names(dt))) return(dt)
  if (nrow(dt) == 0L) return(dt)

  # #472 verify finding (Logic P2): factor columns silently coerce to NA on
  # indexed assignment with non-level character values, violating MP154.
  # Cast to character defensively so the assignment below is well-typed.
  if (is.factor(dt$sku))  dt$sku  <- as.character(dt$sku)
  if (is.factor(dt$asin)) dt$asin <- as.character(dt$asin)

  asin_pattern <- "^B0[A-Z0-9]{8}$"
  needs_backfill <- !is.na(dt$sku) &
                    grepl(asin_pattern, dt$sku) &
                    (is.na(dt$asin) | !nzchar(dt$asin))

  n_backfill <- sum(needs_backfill)
  if (n_backfill > 0L) {
    dt$asin[needs_backfill] <- dt$sku[needs_backfill]
    if (isTRUE(verbose)) {
      message(sprintf("Backfilled asin from ASIN-shaped sku for %d row(s)",
                      n_backfill))
    }
  }

  dt
}
