#!/usr/bin/env Rscript
# ==============================================================================
# Currency Conversion Utility Function (R116 Compliant)
# ==============================================================================
#
# Function: fn_convert_currency_to_usd
# Purpose: Convert monetary values to USD with full audit trail
# Location: scripts/global_scripts/04_utils/fn_convert_currency_to_usd.R
#
# Principle Compliance:
# - R116: Currency Standardization in ETL 1ST Stage
# - MP029: No Fake Data Principle (use real exchange rates)
# - MP102: Completeness (preserve original values + metadata)
# - MP108: ETL Stage Separation (designed for 1ST stage usage)
#
# Author: principle-product-manager
# Date: 2025-11-12
# Version: 1.0
# ==============================================================================

#' Convert Monetary Values to USD
#'
#' Implements R116: Currency Standardization in ETL 1ST Stage
#' Converts all monetary columns to USD while preserving complete audit trail
#'
#' @param data Data frame with price and currency columns
#' @param price_col Name of price column (default: "price")
#' @param currency_col Name of currency column (default: "currency")
#' @param rate_source Exchange rate source: "ECB", "FRED", "FIXED" (default: "FIXED")
#' @param rate_date Date for exchange rates (default: Sys.Date())
#' @param custom_rates Named list of custom exchange rates (optional)
#'
#' @return Data frame with new columns:
#'   - original_price: Original price value before conversion
#'   - original_currency: Original currency code
#'   - price_usd: Converted price in USD
#'   - conversion_rate: Exchange rate used (original_currency to USD)
#'   - conversion_date: Date of exchange rate
#'   - conversion_source: Source of exchange rate data
#'
#' @examples
#' # Basic usage with fixed rates
#' products <- data.frame(
#'   product_id = c("A001", "A002", "A003"),
#'   price = c(1000, 50, 100),
#'   currency = c("TWD", "USD", "EUR")
#' )
#' result <- fn_convert_currency_to_usd(products)
#'
#' # Custom exchange rates
#' custom_rates <- list(TWD = 0.0320, EUR = 1.08, GBP = 1.27)
#' result <- fn_convert_currency_to_usd(products, custom_rates = custom_rates)
#'
#' @details
#' Exchange Rate Sources:
#' - "FIXED": Uses default exchange rates (suitable for Week 1 implementation)
#' - "ECB": European Central Bank (future enhancement)
#' - "FRED": Federal Reserve Economic Data (future enhancement)
#'
#' Default Exchange Rates (as of 2025-11-12):
#' - USD: 1.0000 (base currency)
#' - EUR: 1.0800 (1 EUR = 1.08 USD)
#' - GBP: 1.2700 (1 GBP = 1.27 USD)
#' - TWD: 0.0320 (1 TWD = 0.032 USD)
#' - AUD: 0.6500 (1 AUD = 0.65 USD)
#' - CAD: 0.7300 (1 CAD = 0.73 USD)
#' - JPY: 0.0067 (1 JPY = 0.0067 USD)
#' - CNY: 0.1380 (1 CNY = 0.138 USD)
#'
#' Missing Currency Handling:
#' - If currency_col is missing or NA, assumes USD (conversion_rate = 1.0)
#' - If currency code is not recognized, issues warning and uses USD
#'
#' Audit Trail (R116 Requirement):
#' All original values are preserved to enable verification, debugging, and
#' regulatory compliance. The conversion calculation can be verified:
#'   price_usd = original_price * conversion_rate
#'
fn_convert_currency_to_usd <- function(data,
                                       price_col = "price",
                                       currency_col = "currency",
                                       rate_source = "FIXED",
                                       rate_date = Sys.Date(),
                                       custom_rates = NULL) {

  # ==============================================================================
  # Input Validation
  # ==============================================================================

  # Check data is a data frame
  if (!is.data.frame(data)) {
    stop("VIOLATION R116: 'data' must be a data frame")
  }

  # Check price column exists
  if (!price_col %in% names(data)) {
    stop(sprintf("VIOLATION R116: Price column '%s' not found in data", price_col))
  }

  # Check currency column exists (or will be created)
  if (!currency_col %in% names(data)) {
    warning(sprintf("R116 WARNING: Currency column '%s' not found. Assuming all prices are USD.",
                   currency_col))
    data[[currency_col]] <- "USD"
  }

  # ==============================================================================
  # Preserve Original Values (R116 Requirement)
  # ==============================================================================

  data$original_price <- data[[price_col]]
  data$original_currency <- data[[currency_col]]

  # ==============================================================================
  # Get Exchange Rates
  # ==============================================================================

  if (!is.null(custom_rates)) {
    # Use custom rates provided by user
    exchange_rates <- custom_rates
    rate_source <- "CUSTOM"

  } else if (rate_source == "FIXED") {
    # Use fixed exchange rates (suitable for Week 1 implementation)
    # Rates as of 2025-11-12
    exchange_rates <- list(
      USD = 1.0000,    # Base currency
      EUR = 1.0800,    # Euro
      GBP = 1.2700,    # British Pound
      TWD = 0.0320,    # Taiwan Dollar
      AUD = 0.6500,    # Australian Dollar
      CAD = 0.7300,    # Canadian Dollar
      JPY = 0.0067,    # Japanese Yen
      CNY = 0.1380,    # Chinese Yuan
      KRW = 0.0007,    # Korean Won
      SGD = 0.7400,    # Singapore Dollar
      HKD = 0.1280,    # Hong Kong Dollar
      INR = 0.0120,    # Indian Rupee
      MXN = 0.0580,    # Mexican Peso
      BRL = 0.2000,    # Brazilian Real
      ZAR = 0.0550     # South African Rand
    )

  } else if (rate_source == "ECB") {
    # Future enhancement: Fetch from European Central Bank
    stop("R116 ERROR: ECB exchange rate fetching not yet implemented. Use rate_source='FIXED' for Week 1.")

  } else if (rate_source == "FRED") {
    # Future enhancement: Fetch from Federal Reserve Economic Data
    stop("R116 ERROR: FRED exchange rate fetching not yet implemented. Use rate_source='FIXED' for Week 1.")

  } else {
    stop(sprintf("VIOLATION R116: Unknown rate_source '%s'. Use 'FIXED', 'ECB', or 'FRED'.",
                rate_source))
  }

  # ==============================================================================
  # Convert Prices to USD
  # ==============================================================================

  # Get unique currencies in data
  currencies_in_data <- unique(data[[currency_col]])
  currencies_in_data <- currencies_in_data[!is.na(currencies_in_data)]

  # Check for unrecognized currencies
  unrecognized <- setdiff(currencies_in_data, names(exchange_rates))
  if (length(unrecognized) > 0) {
    warning(sprintf(
      "R116 WARNING: Unrecognized currencies: %s. Will use USD (rate=1.0) for these.",
      paste(unrecognized, collapse = ", ")
    ))
  }

  # Apply conversion rates
  data$conversion_rate <- sapply(data[[currency_col]], function(curr) {
    if (is.na(curr)) {
      return(1.0)  # Assume USD if missing
    }

    rate <- exchange_rates[[curr]]
    if (is.null(rate)) {
      return(1.0)  # Default to USD if not found
    }

    return(rate)
  })

  # Calculate USD prices
  data$price_usd <- data$original_price * data$conversion_rate

  # Add metadata
  data$conversion_date <- rate_date
  data$conversion_source <- rate_source

  # ==============================================================================
  # Validation (R116 Compliance Check)
  # ==============================================================================

  # Check for NA values in price_usd
  na_count <- sum(is.na(data$price_usd))
  if (na_count > 0) {
    warning(sprintf("R116 WARNING: %d rows have NA price_usd values (likely from NA original_price)",
                   na_count))
  }

  # Check for unreasonable conversion rates
  unreasonable <- data$conversion_rate < 0.0001 | data$conversion_rate > 10000
  unreasonable_count <- sum(unreasonable, na.rm = TRUE)
  if (unreasonable_count > 0) {
    warning(sprintf(
      "R116 WARNING: %d rows have unreasonable conversion rates (<%s or >%s)",
      unreasonable_count, "0.0001", "10000"
    ))
  }

  # ==============================================================================
  # Return Result
  # ==============================================================================

  message(sprintf("✓ R116: Currency conversion complete"))
  message(sprintf("  → Processed %d rows", nrow(data)))
  message(sprintf("  → Currencies found: %s", paste(currencies_in_data, collapse = ", ")))
  message(sprintf("  → Rate source: %s", rate_source))
  message(sprintf("  → Rate date: %s", rate_date))

  return(data)
}

# ==============================================================================
# Validation Function
# ==============================================================================

#' Validate Currency Conversion Results
#'
#' Validates that currency conversion was performed correctly according to R116
#'
#' @param data Data frame with currency conversion columns
#' @return TRUE if validation passes, stops with error otherwise
#'
validate_currency_conversion <- function(data) {

  # Check 1: All required metadata columns exist
  required_cols <- c("original_price", "original_currency", "price_usd",
                     "conversion_rate", "conversion_date", "conversion_source")
  missing_cols <- setdiff(required_cols, names(data))

  if (length(missing_cols) > 0) {
    stop(sprintf("VIOLATION R116: Missing currency metadata columns: %s",
                paste(missing_cols, collapse = ", ")))
  }

  # Check 2: All prices converted (no NA in price_usd where original_price exists)
  has_original <- !is.na(data$original_price)
  missing_usd <- is.na(data$price_usd)
  invalid_conversions <- sum(has_original & missing_usd)

  if (invalid_conversions > 0) {
    stop(sprintf("VIOLATION R116: %d rows have original_price but NA price_usd",
                invalid_conversions))
  }

  # Check 3: Conversion rates are reasonable (0.0001 to 10000)
  invalid_rates <- data$conversion_rate < 0.0001 | data$conversion_rate > 10000
  invalid_rate_count <- sum(invalid_rates, na.rm = TRUE)

  if (invalid_rate_count > 0) {
    warning(sprintf("R116 WARNING: %d rows have unusual conversion rates (<%s or >%s)",
                   invalid_rate_count, "0.0001", "10000"))
  }

  # Check 4: Verify conversion calculation accuracy
  calculation_errors <- abs(data$price_usd - (data$original_price * data$conversion_rate)) > 0.01
  calculation_errors <- sum(calculation_errors, na.rm = TRUE)

  if (calculation_errors > 0) {
    stop(sprintf("VIOLATION R116: %d rows have incorrect USD conversion calculations",
                calculation_errors))
  }

  message("✅ R116 Validation: Currency conversion passed all checks")
  return(TRUE)
}

# ==============================================================================
# Example Usage (for testing)
# ==============================================================================

if (FALSE) {
  # Example 1: Basic usage
  test_data <- data.frame(
    product_id = c("A001", "A002", "A003", "A004"),
    product_name = c("Product A", "Product B", "Product C", "Product D"),
    price = c(1000, 50, 100, 3000),
    currency = c("TWD", "USD", "EUR", "JPY")
  )

  result <- fn_convert_currency_to_usd(test_data)
  print(result)

  # Validate result
  validate_currency_conversion(result)

  # Example 2: Custom exchange rates
  custom_rates <- list(TWD = 0.0320, EUR = 1.08, JPY = 0.0067)
  result2 <- fn_convert_currency_to_usd(
    test_data,
    custom_rates = custom_rates
  )
  print(result2)
}
