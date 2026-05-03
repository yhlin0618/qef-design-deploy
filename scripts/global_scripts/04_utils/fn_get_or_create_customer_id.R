#' @title Get or Create Unified Customer ID
#' @description Returns unified INTEGER customer_id for given email addresses.
#'              Creates new entries in lookup table for unknown emails.
#'              Enables cross-platform customer identification.
#' @param emails Character vector. Email addresses to look up.
#' @param platform Character. Platform ID (e.g., "cbz", "eby", "amz").
#' @param con DBI connection. Connection to transformed_data.duckdb.
#' @param lookup_table Character. Name of lookup table. Default "df_customer_id_lookup".
#' @return Integer vector. Unified customer IDs (same length as emails).
#' @principle DM_P003, DM_P006, MP064
#' @author Claude
#' @date 2025-12-29

get_or_create_customer_ids <- function(emails,
                                        platform,
                                        con,
                                        lookup_table = "df_customer_id_lookup") {
  # Validate inputs
  if (!inherits(con, "DBIConnection")) {
    stop("con must be a valid DBI connection")
  }
  if (length(emails) == 0) {
    return(integer(0))
  }

  # Normalize emails: lowercase, trimmed
  normalized_emails <- tolower(trimws(emails))

  # Handle NA/empty emails - return NA for these
  valid_mask <- !is.na(normalized_emails) & nzchar(normalized_emails)
  valid_emails <- unique(normalized_emails[valid_mask])

  if (length(valid_emails) == 0) {
    return(rep(NA_integer_, length(emails)))
  }

  # Create lookup table if it doesn't exist
  if (!DBI::dbExistsTable(con, lookup_table)) {
    message(sprintf("[get_or_create_customer_ids] Creating lookup table: %s", lookup_table))
    DBI::dbExecute(con, sprintf("
      CREATE TABLE %s (
        customer_id INTEGER PRIMARY KEY,
        customer_email VARCHAR UNIQUE,
        email_hash VARCHAR(64),
        first_seen_platform VARCHAR(10),
        first_seen_date DATE
      )
    ", lookup_table))
  }

  # Batch lookup existing emails
  # Use parameterized query to prevent SQL injection
  # DuckDB supports unnest for batch lookups
  temp_table <- sprintf("temp_email_lookup_%s", format(Sys.time(), "%Y%m%d%H%M%S"))

  # Create temporary table with emails to look up
  temp_df <- data.frame(
    customer_email = valid_emails,
    stringsAsFactors = FALSE
  )
  DBI::dbWriteTable(con, temp_table, temp_df, temporary = TRUE, overwrite = TRUE)

  # Find existing mappings
  existing <- DBI::dbGetQuery(con, sprintf("
    SELECT l.customer_email, l.customer_id
    FROM %s l
    INNER JOIN %s t ON l.customer_email = t.customer_email
  ", lookup_table, temp_table))

  # Find new emails that need IDs
  existing_emails <- existing$customer_email
  new_emails <- setdiff(valid_emails, existing_emails)

  # Create new entries for unknown emails
  if (length(new_emails) > 0) {
    # Get the next available customer_id
    max_id_result <- DBI::dbGetQuery(con, sprintf(
      "SELECT COALESCE(MAX(customer_id), 0) as max_id FROM %s", lookup_table
    ))
    next_id <- max_id_result$max_id + 1L

    # Prepare new entries
    new_entries <- data.frame(
      customer_id = seq(from = next_id, length.out = length(new_emails)),
      customer_email = new_emails,
      email_hash = sapply(new_emails, function(e) digest::digest(e, algo = "sha256")),
      first_seen_platform = platform,
      first_seen_date = Sys.Date(),
      stringsAsFactors = FALSE
    )

    # Insert new entries
    DBI::dbWriteTable(con, lookup_table, new_entries, append = TRUE)

    message(sprintf("[get_or_create_customer_ids] Created %d new customer IDs for platform '%s'",
                    length(new_emails), platform))

    # Add new mappings to existing
    existing <- rbind(existing, new_entries[, c("customer_email", "customer_id")])
  }

  # Clean up temp table
  tryCatch(
    DBI::dbRemoveTable(con, temp_table),
    error = function(e) NULL
  )

  # Build result vector matching original email order
  email_to_id <- setNames(existing$customer_id, existing$customer_email)
  result <- email_to_id[normalized_emails]

  # NA for invalid emails stays NA
  result[!valid_mask] <- NA_integer_

  as.integer(result)
}


#' @title Get Customer Email from ID
#' @description Reverse lookup: get email from unified customer_id.
#' @param customer_ids Integer vector. Customer IDs to look up.
#' @param con DBI connection. Connection to transformed_data.duckdb.
#' @param lookup_table Character. Name of lookup table. Default "df_customer_id_lookup".
#' @return Character vector. Email addresses (same length as customer_ids).
get_customer_emails <- function(customer_ids,
                                 con,
                                 lookup_table = "df_customer_id_lookup") {
  if (!inherits(con, "DBIConnection")) {
    stop("con must be a valid DBI connection")
  }
  if (length(customer_ids) == 0) {
    return(character(0))
  }
  if (!DBI::dbExistsTable(con, lookup_table)) {
    warning(sprintf("Lookup table %s does not exist", lookup_table))
    return(rep(NA_character_, length(customer_ids)))
  }

  valid_mask <- !is.na(customer_ids)
  valid_ids <- unique(customer_ids[valid_mask])

  if (length(valid_ids) == 0) {
    return(rep(NA_character_, length(customer_ids)))
  }

  # Query existing mappings
  id_list <- paste(valid_ids, collapse = ",")
  existing <- DBI::dbGetQuery(con, sprintf("
    SELECT customer_id, customer_email
    FROM %s
    WHERE customer_id IN (%s)
  ", lookup_table, id_list))

  # Build result vector
  id_to_email <- setNames(existing$customer_email, as.character(existing$customer_id))
  result <- id_to_email[as.character(customer_ids)]
  result[!valid_mask] <- NA_character_

  as.character(result)
}


#' @title Get Customer Lookup Stats
#' @description Returns statistics about the customer lookup table.
#' @param con DBI connection. Connection to transformed_data.duckdb.
#' @param lookup_table Character. Name of lookup table. Default "df_customer_id_lookup".
#' @return List with stats.
get_customer_lookup_stats <- function(con,
                                       lookup_table = "df_customer_id_lookup") {
  if (!DBI::dbExistsTable(con, lookup_table)) {
    return(list(
      exists = FALSE,
      total_customers = 0,
      platforms = character(0)
    ))
  }

  stats <- DBI::dbGetQuery(con, sprintf("
    SELECT
      COUNT(*) as total_customers,
      COUNT(DISTINCT first_seen_platform) as platform_count,
      MIN(first_seen_date) as earliest_date,
      MAX(first_seen_date) as latest_date
    FROM %s
  ", lookup_table))

  platforms <- DBI::dbGetQuery(con, sprintf("
    SELECT first_seen_platform, COUNT(*) as count
    FROM %s
    GROUP BY first_seen_platform
    ORDER BY count DESC
  ", lookup_table))

  list(
    exists = TRUE,
    total_customers = stats$total_customers,
    platform_count = stats$platform_count,
    earliest_date = stats$earliest_date,
    latest_date = stats$latest_date,
    by_platform = platforms
  )
}
