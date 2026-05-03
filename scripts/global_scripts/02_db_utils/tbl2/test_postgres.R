# Integration test: tbl2() against real PostgreSQL backend.
#
# Validates cross-driver behavior after the tbl2.DBIConnection pure-passthrough
# refactor (issue #369). The original MAMBA production failure was caused by
# `?` positional placeholders — which DuckDB accepts but PostgreSQL rejects.
# This test proves that tbl2() + dplyr verbs produce PostgreSQL-compatible
# SQL via dbplyr, with no driver-specific string construction.
#
# How to run:
#   Ensure PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (and optionally
#   PGSSLMODE) are set in the environment, then:
#     Rscript -e "testthat::test_file('scripts/global_scripts/02_db_utils/tbl2/test_postgres.R')"
#
# CI note: this test SKIPs when credentials are missing — it does not fail.

library(testthat)
library(DBI)
library(dplyr)
library(dbplyr)

# Source the refactored tbl2 function
source("scripts/global_scripts/02_db_utils/tbl2/fn_tbl2.R")

# --- helpers ---------------------------------------------------------------

pg_credentials_available <- function() {
  all(nzchar(c(
    Sys.getenv("PGHOST"),
    Sys.getenv("PGUSER"),
    Sys.getenv("PGPASSWORD"),
    Sys.getenv("PGDATABASE")
  )))
}

pg_connect <- function() {
  skip_if_not_installed("RPostgres")
  skip_if_not(pg_credentials_available(),
              "PostgreSQL credentials not set (PGHOST/PGUSER/PGPASSWORD/PGDATABASE)")

  tryCatch({
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = Sys.getenv("PGHOST"),
      port     = as.integer(Sys.getenv("PGPORT", "5432")),
      user     = Sys.getenv("PGUSER"),
      password = Sys.getenv("PGPASSWORD"),
      dbname   = Sys.getenv("PGDATABASE"),
      sslmode  = Sys.getenv("PGSSLMODE", "require")
    )
  }, error = function(e) {
    skip(paste("PostgreSQL connection failed:", e$message))
  })
}

# Create a uniquely-named fixture table, write it, and arrange for cleanup.
with_fixture <- function(pg_con, data, prefix = "tbl2_test_fixture") {
  temp_name <- paste0(prefix, "_", format(Sys.time(), "%Y%m%d_%H%M%S_"),
                      sample.int(1e6, 1))
  DBI::dbWriteTable(pg_con, temp_name, data, temporary = FALSE)
  withr::defer(
    try(DBI::dbExecute(pg_con, paste0('DROP TABLE IF EXISTS "', temp_name, '"')),
        silent = TRUE),
    envir = parent.frame()
  )
  temp_name
}

# --- tests -----------------------------------------------------------------

test_that("tbl2() simple read returns all rows on PostgreSQL", {
  pg_con <- pg_connect()
  on.exit(DBI::dbDisconnect(pg_con), add = TRUE)

  fixture <- data.frame(
    id = 1:5,
    name = c("Alice", "Bob", "Charlie", "David", "Eve"),
    platform_id = c("amz", "cbz", "amz", "eby", "amz"),
    value = c(10, 20, 30, 40, 50),
    stringsAsFactors = FALSE
  )
  tbl_name <- with_fixture(pg_con, fixture)

  result <- tbl2(pg_con, tbl_name) %>% collect()

  expect_equal(nrow(result), 5)
  expect_true(all(c("id", "name", "platform_id", "value") %in% names(result)))
})

test_that("tbl2() with single-predicate filter translates to PostgreSQL", {
  pg_con <- pg_connect()
  on.exit(DBI::dbDisconnect(pg_con), add = TRUE)

  fixture <- data.frame(
    id = 1:5,
    platform_id = c("amz", "cbz", "amz", "eby", "amz"),
    value = c(10, 20, 30, 40, 50),
    stringsAsFactors = FALSE
  )
  tbl_name <- with_fixture(pg_con, fixture)

  result <- tbl2(pg_con, tbl_name) %>%
    filter(platform_id == "amz") %>%
    collect()

  expect_equal(nrow(result), 3)
  expect_true(all(result$platform_id == "amz"))
})

test_that("tbl2() with multi-predicate filter works (the MAMBA #365 case)", {
  # The original failure: DBI::dbGetQuery(con, "... WHERE x = ? AND y = ?", params = list(...))
  # exploded on PostgreSQL because it does not accept `?` positional placeholders.
  # Via dbplyr, the same filter compiles to `WHERE x = $1 AND y = $2` automatically.
  pg_con <- pg_connect()
  on.exit(DBI::dbDisconnect(pg_con), add = TRUE)

  fixture <- data.frame(
    platform_id = c("amz", "cbz", "amz", "eby", "amz"),
    product_line_id = c("jew", "jew", "sop", "jew", "sop"),
    value = c(10, 20, 30, 40, 50),
    stringsAsFactors = FALSE
  )
  tbl_name <- with_fixture(pg_con, fixture)

  result <- tbl2(pg_con, tbl_name) %>%
    filter(platform_id == "amz", product_line_id == "sop") %>%
    collect()

  expect_equal(nrow(result), 2)
  expect_true(all(result$platform_id == "amz"))
  expect_true(all(result$product_line_id == "sop"))
})

test_that("tbl2() with group_by + summarise works on PostgreSQL", {
  pg_con <- pg_connect()
  on.exit(DBI::dbDisconnect(pg_con), add = TRUE)

  fixture <- data.frame(
    platform_id = c("amz", "cbz", "amz", "eby", "amz"),
    value = c(10, 20, 30, 40, 50),
    stringsAsFactors = FALSE
  )
  tbl_name <- with_fixture(pg_con, fixture)

  result <- tbl2(pg_con, tbl_name) %>%
    group_by(platform_id) %>%
    summarise(total = sum(value, na.rm = TRUE), n = dplyr::n(),
              .groups = "drop") %>%
    collect() %>%
    arrange(platform_id)

  expect_equal(nrow(result), 3)
  expect_equal(result$total[result$platform_id == "amz"], 90)
  expect_equal(result$n[result$platform_id == "amz"], 3)
})

test_that("tbl2() with dbplyr::in_schema works on PostgreSQL public schema", {
  pg_con <- pg_connect()
  on.exit(DBI::dbDisconnect(pg_con), add = TRUE)

  fixture <- data.frame(id = 1:3, val = c("a", "b", "c"),
                        stringsAsFactors = FALSE)
  tbl_name <- with_fixture(pg_con, fixture)

  # in_schema() is the canonical way to reference schema.table in dplyr/dbplyr.
  # PostgreSQL default schema is "public".
  result <- tbl2(pg_con, in_schema("public", tbl_name)) %>% collect()

  expect_equal(nrow(result), 3)
  expect_equal(result$val, c("a", "b", "c"))
})
