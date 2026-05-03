#' @title BTYD-Based Customer Alive Probability
#'
#' @description
#' Replaces the logistic nrec model with BG/NBD P(alive) from CLVTools.
#' Designed to run AFTER analysis_dna() — overwrites nrec_prob with
#' BTYD-derived churn probability (1 - P(alive)).
#'
#' Supports Two-Pass Empirical Bayes: when CLVTools estimation fails (degenerate
#' parameters), caller can provide fallback_params from successful slices.
#'
#' @param data_by_customer data.table from analysis_dna()$data_by_customer.
#'        Must have: customer_id, r_value (recency in days), f_value (times),
#'        total_spent or total_amount.
#' @param df_sales_by_customer_by_date Transaction-level data with
#'        customer_id, payment_time (or min_time_by_date, min_time).
#' @param time_unit Character. Time unit for CLVTools: "week" (default) or "day".
#' @param prediction_periods Numeric. Forecast horizon in time_unit units (default: 12).
#' @param fallback_params Named numeric vector c(r, alpha, a, b). When CLVTools
#'        estimation fails or produces degenerate parameters, use these as
#'        reference parameters for the BG/NBD closed-form P(alive) computation.
#'        Typically: median parameters from successfully estimated slices
#'        (Empirical Bayes borrowing). If NULL (default), failed estimation
#'        returns data unchanged (backward compatible).
#' @param verbose Logical. Print progress messages (default: TRUE).
#'
#' @return Updated data_by_customer (data.frame) with:
#'   - nrec_prob: overwritten with (1 - P(alive)) from BG/NBD
#'   - p_alive: new column, P(alive) from BG/NBD (0~1)
#'   - btyd_expected_transactions: expected future transactions (CET)
#'   - nrec: set to NULL (binary indicator no longer needed)
#'
#' Additionally, two attributes are set on the return value:
#'   - attr(,"btyd_params"): named numeric c(r, alpha, a, b) used for computation
#'   - attr(,"btyd_status"): "estimated" | "fallback" | "degenerate" | "failed"
#'
#' @details
#' Uses CLVTools BG/NBD model. Sufficient statistics per customer:
#'   x = repeat purchases (times - 1)
#'   t.x = recency (time of last purchase since first purchase)
#'   T.cal = observation period (time_now - first_purchase)
#'
#' P(alive) closed-form (Fader, Hardie & Lee 2005):
#'   For x >= 1:
#'     P(alive | x, t_x, T) = 1 / (1 + (a/(b+x-1)) * ((alpha+T)/(alpha+t_x))^(r+x))
#'   For x = 0:
#'     P(alive | T) = 1 / (1 + (a/b) * (((alpha+T)/alpha)^r - 1))
#'
#' @principle UX_P002 (accurate metrics), MP064 (derivation logic)
#' @principle MP029 (no fake data — uses real transaction history)
#' @principle SO_R007 (one function one file)

# --- Helper: BG/NBD P(alive) closed-form computation ---
# No CLVTools dependency. Pure R using BG/NBD sufficient statistics.
# Used for: (1) fallback when CLVTools fails, (2) all-customer computation
btyd_palive_closed_form <- function(transactions, params, time_unit,
                                     prediction_periods, verbose = TRUE) {
  r_par <- params["r"]; alpha_par <- params["alpha"]
  a_par <- params["a"]; b_par <- params["b"]

  observation_end <- max(transactions$date, na.rm = TRUE)
  time_divisor <- switch(time_unit, "week" = 7, "day" = 1, 7)

  # Compute sufficient statistics per customer
  cust_stats <- transactions[, .(
    first_date = min(date),
    last_date = max(date),
    N = .N
  ), by = customer_id]
  cust_stats[, `:=`(
    x = N - 1L,
    t_x = as.numeric(last_date - first_date) / time_divisor,
    T_cal = as.numeric(observation_end - first_date) / time_divisor
  )]

  # P(alive) for x >= 1 (Fader, Hardie & Lee 2005, eq. 7)
  cust_stats[x >= 1 & T_cal > 0, p_alive := {
    1 / (1 + (a_par / (b_par + x - 1)) *
           ((alpha_par + T_cal) / (alpha_par + t_x))^(r_par + x))
  }]

  # P(alive) for x = 0, T_cal > 0
  cust_stats[x == 0 & T_cal > 0, p_alive := {
    delta <- ((alpha_par + T_cal) / alpha_par)^r_par - 1
    1 / (1 + (a_par / b_par) * delta)
  }]

  # T_cal = 0 (bought on observation_end): P(alive) = 1
  cust_stats[T_cal == 0 | T_cal < 0, p_alive := 1.0]

  # CET approximation: P(alive) * base_rate * periods
  cust_stats[, btyd_expected_transactions := p_alive * (r_par / alpha_par) * prediction_periods]

  if (verbose) {
    pa_vals <- cust_stats$p_alive[!is.na(cust_stats$p_alive)]
    if (length(pa_vals) > 0) {
      message(sprintf("[BTYD] Closed-form P(alive): n=%d, min=%.4f, median=%.4f, mean=%.4f, max=%.4f",
                      length(pa_vals), min(pa_vals), median(pa_vals), mean(pa_vals), max(pa_vals)))
    }
  }

  return(cust_stats[, .(customer_id, p_alive, btyd_expected_transactions)])
}


analysis_btyd <- function(data_by_customer,
                          df_sales_by_customer_by_date,
                          time_unit = "week",
                          prediction_periods = 12,
                          fallback_params = NULL,
                          verbose = TRUE) {

  start_time <- Sys.time()
  if (verbose) message("[BTYD] Starting P(alive) computation...")

  # --- Return helper: attach metadata before early return ---
  return_unchanged <- function(status) {
    attr(data_by_customer, "btyd_params") <- NULL
    attr(data_by_customer, "btyd_status") <- status
    return(data_by_customer)
  }

  # --- Guard: CLVTools availability (only needed when no fallback_params) ---
  if (is.null(fallback_params) && !requireNamespace("CLVTools", quietly = TRUE)) {
    warning("[BTYD] CLVTools package not installed. Keeping original nrec_prob.")
    return(return_unchanged("failed"))
  }

  # --- Guard: minimum data requirements ---
  if (is.null(data_by_customer) || nrow(data_by_customer) == 0) {
    warning("[BTYD] data_by_customer is empty. Returning unchanged.")
    return(return_unchanged("failed"))
  }
  if (is.null(df_sales_by_customer_by_date) || nrow(df_sales_by_customer_by_date) == 0) {
    warning("[BTYD] Transaction data is empty. Keeping original nrec_prob.")
    return(return_unchanged("failed"))
  }

  # Convert to data.table if needed
  if (!data.table::is.data.table(data_by_customer)) {
    data_by_customer <- data.table::as.data.table(data_by_customer)
  }
  dt_transactions <- data.table::as.data.table(df_sales_by_customer_by_date)

  # --- Step 1: Detect time and price fields ---
  time_field <- NULL
  for (tf in c("payment_time", "min_time_by_date", "min_time")) {
    if (tf %in% names(dt_transactions)) {
      time_field <- tf
      break
    }
  }
  if (is.null(time_field)) {
    warning("[BTYD] No time field found (payment_time/min_time_by_date/min_time). Keeping original nrec_prob.")
    return(return_unchanged("failed"))
  }

  price_field <- NULL
  for (pf in c("total_spent", "sum_spent_by_date", "total_amount", "sum_sales_by_customer")) {
    if (pf %in% names(dt_transactions)) {
      price_field <- pf
      break
    }
  }
  if (is.null(price_field)) {
    if (verbose) message("[BTYD] No price field found, using placeholder price = 1 (P(alive) is unaffected)")
    dt_transactions[, btyd_price := 1]
    price_field <- "btyd_price"
  }

  if (verbose) message(sprintf("[BTYD] Using time field: %s, price field: %s", time_field, price_field))

  # --- Step 2: Prepare transaction data ---
  transactions <- data.table::data.table(
    customer_id = dt_transactions[["customer_id"]],
    date = as.Date(dt_transactions[[time_field]]),
    price = as.numeric(dt_transactions[[price_field]])
  )

  transactions <- transactions[!is.na(date) & !is.na(customer_id)]
  transactions[is.na(price), price := 0]
  transactions[price <= 0, price := 0.01]

  n_customers <- data.table::uniqueN(transactions$customer_id)
  if (n_customers < 2) {
    warning("[BTYD] Fewer than 2 customers in transaction data. Keeping original nrec_prob.")
    return(return_unchanged("failed"))
  }

  n_repeat <- transactions[, .N, by = customer_id][N > 1, .N]
  if (n_repeat < 2 && is.null(fallback_params)) {
    warning("[BTYD] Fewer than 2 customers with repeat purchases. Keeping original nrec_prob.")
    return(return_unchanged("failed"))
  }

  if (verbose) message(sprintf("[BTYD] %d customers, %d with repeat purchases, %d transactions",
                               n_customers, n_repeat, nrow(transactions)))

  # --- Steps 3-4: CLVTools estimation (or skip to fallback) ---
  use_fallback <- FALSE
  btyd_params <- NULL

  if (n_repeat < 2 && !is.null(fallback_params)) {
    # Too few repeat buyers for MLE — skip CLVTools entirely
    use_fallback <- TRUE
    btyd_params <- fallback_params
    if (verbose) message("[BTYD] Too few repeat buyers for MLE. Using fallback params directly.")

  } else {
    # Step 3: Build CLVTools data object
    clv_data <- tryCatch({
      CLVTools::clvdata(
        data.transactions = transactions,
        date.format = "ymd",
        time.unit = time_unit,
        estimation.split = NULL,
        name.id = "customer_id",
        name.date = "date",
        name.price = "price"
      )
    }, error = function(e) {
      warning(sprintf("[BTYD] clvdata() failed: %s", e$message))
      NULL
    })

    if (is.null(clv_data)) {
      if (!is.null(fallback_params)) {
        use_fallback <- TRUE
        btyd_params <- fallback_params
        if (verbose) message("[BTYD] CLVTools clvdata() failed. Using fallback params.")
      } else {
        return(return_unchanged("failed"))
      }
    }

    if (!use_fallback) {
      # Step 4: Estimate BG/NBD model
      bgf <- tryCatch({
        CLVTools::bgnbd(clv.data = clv_data, verbose = verbose,
                        start.params.model = c(r = 0.5, alpha = 10, a = 0.5, b = 5))
      }, error = function(e) {
        if (verbose) message(sprintf("[BTYD] BG/NBD with start params failed: %s. Trying default...", e$message))
        tryCatch({
          CLVTools::bgnbd(clv.data = clv_data, verbose = verbose)
        }, error = function(e2) {
          warning(sprintf("[BTYD] BG/NBD estimation failed: %s", e2$message))
          NULL
        })
      })

      if (is.null(bgf)) {
        if (!is.null(fallback_params)) {
          use_fallback <- TRUE
          btyd_params <- fallback_params
          if (verbose) message("[BTYD] CLVTools estimation failed. Using fallback params.")
        } else {
          return(return_unchanged("failed"))
        }
      }
    }

    if (!use_fallback) {
      params <- coef(bgf)
      if (verbose) {
        message(sprintf("[BTYD] BG/NBD parameters: r=%.4f, alpha=%.4f, a=%.4f, b=%.4f",
                        params["r"], params["alpha"], params["a"], params["b"]))
      }

      # Sanity check: detect degenerate parameters (MLE at boundary)
      params_degenerate <- (params["a"] < 1e-3) || (params["r"] > 1e4) ||
                           (params["b"] < 1e-3) || (params["a"] / params["b"] < 1e-4)
      if (params_degenerate) {
        if (!is.null(fallback_params)) {
          use_fallback <- TRUE
          btyd_params <- fallback_params
          if (verbose) {
            message(sprintf("[BTYD] Parameters degenerate (a=%.6f, b=%.6f). Using fallback params.",
                            params["a"], params["b"]))
          }
        } else {
          warning(sprintf("[BTYD] Parameters degenerate (a=%.6f, b=%.6f, r=%.4f, alpha=%.4f). Keeping original nrec_prob.",
                          params["a"], params["b"], params["r"], params["alpha"]))
          return(return_unchanged("degenerate"))
        }
      } else {
        btyd_params <- params
      }
    }
  }

  # --- Step 5: Compute pred_dt ---
  if (use_fallback) {
    # Fallback path: compute P(alive) directly from BG/NBD closed-form
    if (verbose) {
      message(sprintf("[BTYD] Fallback params: r=%.4f, alpha=%.4f, a=%.4f, b=%.4f",
                      btyd_params["r"], btyd_params["alpha"],
                      btyd_params["a"], btyd_params["b"]))
    }
    pred_dt <- btyd_palive_closed_form(transactions, btyd_params, time_unit,
                                        prediction_periods, verbose)

    # Ensure customer_id type matches data_by_customer
    target_class <- class(data_by_customer$customer_id)[1]
    if (target_class %in% c("integer", "numeric") && is.character(pred_dt$customer_id)) {
      pred_dt[, customer_id := as.numeric(customer_id)]
    } else if (target_class == "character" && !is.character(pred_dt$customer_id)) {
      pred_dt[, customer_id := as.character(customer_id)]
    }
    btyd_status <- "fallback"

  } else {
    # Normal path: CLVTools predict + x=0 fix
    pred <- tryCatch({
      suppressWarnings(
        predict(bgf, prediction.end = prediction_periods, verbose = FALSE)
      )
    }, error = function(e) {
      warning(sprintf("[BTYD] predict() failed: %s", e$message))
      NULL
    })

    if (is.null(pred)) {
      if (!is.null(fallback_params)) {
        # Last resort fallback after successful estimation but failed predict
        if (verbose) message("[BTYD] predict() failed. Falling back to closed-form.")
        pred_dt <- btyd_palive_closed_form(transactions, btyd_params, time_unit,
                                            prediction_periods, verbose)
        target_class <- class(data_by_customer$customer_id)[1]
        if (target_class %in% c("integer", "numeric") && is.character(pred_dt$customer_id)) {
          pred_dt[, customer_id := as.numeric(customer_id)]
        } else if (target_class == "character" && !is.character(pred_dt$customer_id)) {
          pred_dt[, customer_id := as.character(customer_id)]
        }
        btyd_status <- "fallback"
      } else {
        return(return_unchanged("failed"))
      }
    } else {
      # Build pred_dt from CLVTools output
      id_col <- if ("Id" %in% names(pred)) "Id" else names(pred)[1]
      pred_dt <- data.table::data.table(
        customer_id = pred[[id_col]],
        p_alive = pred[["PAlive"]],
        btyd_expected_transactions = pred[["CET"]]
      )

      # Ensure customer_id type matches
      target_class <- class(data_by_customer$customer_id)[1]
      if (target_class %in% c("integer", "numeric") && is.character(pred_dt$customer_id)) {
        pred_dt[, customer_id := as.numeric(customer_id)]
      } else if (target_class == "character" && !is.character(pred_dt$customer_id)) {
        pred_dt[, customer_id := as.character(customer_id)]
      }

      # --- Step 5b: Fix P(alive) for single-purchase customers (x=0) ---
      # CLVTools returns P(alive) = 1.0 exactly for x=0 customers.
      # Recompute using BG/NBD closed-form formula.
      r_par <- btyd_params["r"]; alpha_par <- btyd_params["alpha"]
      a_par <- btyd_params["a"]; b_par <- btyd_params["b"]

      observation_end <- max(transactions$date, na.rm = TRUE)
      cust_first_date <- transactions[, .(first_date = min(date)), by = customer_id]
      cust_n_txn <- transactions[, .N, by = customer_id]
      cust_stats <- merge(cust_first_date, cust_n_txn, by = "customer_id")
      cust_stats[, x := N - 1L]
      cust_stats[, T_days := as.numeric(observation_end - first_date)]
      time_divisor <- switch(time_unit, "week" = 7, "day" = 1, 7)
      cust_stats[, T_cal := T_days / time_divisor]

      single_purchase_ids <- cust_stats[x == 0]$customer_id
      n_single <- length(single_purchase_ids)

      if (n_single > 0 && verbose) {
        message(sprintf("[BTYD] Recomputing P(alive) for %d single-purchase customers (x=0)", n_single))
      }

      pred_dt <- merge(pred_dt, cust_stats[, .(customer_id, x, T_cal)],
                       by = "customer_id", all.x = TRUE)

      pred_dt[x == 0 & !is.na(T_cal) & T_cal > 0, `:=`(
        p_alive = {
          delta <- ((alpha_par + T_cal) / alpha_par)^r_par - 1
          1 / (1 + (a_par / b_par) * delta)
        },
        btyd_expected_transactions = {
          delta <- ((alpha_par + T_cal) / alpha_par)^r_par - 1
          pa <- 1 / (1 + (a_par / b_par) * delta)
          pa * (r_par / alpha_par) * prediction_periods
        }
      )]

      pred_dt[x == 0 & !is.na(T_cal) & T_cal == 0, p_alive := 1.0]
      pred_dt[, c("x", "T_cal") := NULL]

      if (n_single > 0 && verbose) {
        fixed_vals <- pred_dt[customer_id %in% single_purchase_ids & !is.na(p_alive)]$p_alive
        if (length(fixed_vals) > 0) {
          message(sprintf("[BTYD] x=0 P(alive) after fix: min=%.4f, median=%.4f, max=%.4f",
                          min(fixed_vals), median(fixed_vals), max(fixed_vals)))
        }
      }

      btyd_status <- "estimated"
    }
  }

  # --- Step 6: Merge back and overwrite nrec_prob ---
  # Remove old btyd columns if they exist (idempotent re-run)
  for (col in c("p_alive", "btyd_expected_transactions")) {
    if (col %in% names(data_by_customer)) {
      data_by_customer[[col]] <- NULL
    }
  }

  # Convert to plain data.frame before merge to avoid data.table reference issues.
  # data.table::set() and := silently fail on merge() results due to
  # .internal.selfref invalidation. Using data.frame + base R $<- is reliable.
  data_by_customer <- as.data.frame(data_by_customer)
  pred_df <- as.data.frame(pred_dt)

  data_by_customer <- merge(
    data_by_customer, pred_df,
    by = "customer_id", all.x = TRUE
  )

  # Overwrite nrec_prob using base R assignment (100% reliable, no reference issues)
  has_palive <- !is.na(data_by_customer[["p_alive"]])
  n_overwrite <- sum(has_palive)
  if (n_overwrite > 0) {
    data_by_customer[["nrec_prob"]][has_palive] <- 1 - data_by_customer[["p_alive"]][has_palive]
  }

  if (verbose && n_overwrite > 0) {
    message(sprintf("[BTYD] Overwrote nrec_prob for %d customers (verified: min=%.6f, max=%.6f)",
                    n_overwrite,
                    min(data_by_customer[["nrec_prob"]][has_palive]),
                    max(data_by_customer[["nrec_prob"]][has_palive])))
  }

  # For customers not in BTYD results (edge case), keep original nrec_prob
  n_na <- sum(!has_palive)
  if (n_na > 0 && verbose) {
    message(sprintf("[BTYD] %d customers had no BTYD result (keeping original nrec_prob)", n_na))
  }

  # Remove nrec binary column (no longer meaningful with continuous P(alive))
  if ("nrec" %in% names(data_by_customer)) {
    data_by_customer[["nrec"]] <- NULL
  }

  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  if (verbose) {
    p_alive_vals <- data_by_customer$p_alive[!is.na(data_by_customer$p_alive)]
    message(sprintf("[BTYD] P(alive) computed for %d customers in %s seconds (status: %s)",
                    length(p_alive_vals), elapsed, btyd_status))
    if (length(p_alive_vals) > 0) {
      message(sprintf("[BTYD] P(alive) distribution: min=%.4f, median=%.4f, mean=%.4f, max=%.4f",
                      min(p_alive_vals), median(p_alive_vals), mean(p_alive_vals), max(p_alive_vals)))
      message(sprintf("[BTYD] High churn risk (nrec_prob > 0.7): %d customers (%.1f%%)",
                      sum(data_by_customer$nrec_prob > 0.7, na.rm = TRUE),
                      100 * mean(data_by_customer$nrec_prob > 0.7, na.rm = TRUE)))
    }
  }

  # Attach metadata for two-pass pipeline coordination
  attr(data_by_customer, "btyd_params") <- btyd_params
  attr(data_by_customer, "btyd_status") <- btyd_status

  return(data_by_customer)
}
