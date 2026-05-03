# Create df_profile_by_customer table (DuckDB)
# PK: (platform_id, customer_id)
# Uses generate_create_table_query (MP058, R092)

if (!exists("generate_create_table_query", mode = "function")) {
  gen_path <- file.path("scripts", "global_scripts", "01_db", "generate_create_table_query", "fn_generate_create_table_query.R")
  if (file.exists(gen_path)) source(gen_path) else stop("generate_create_table_query.R not found")
}

create_df_profile_by_customer_table <- function(con, or_replace = TRUE, verbose = FALSE) {
  stopifnot(inherits(con, "DBIConnection"))

  query <- generate_create_table_query(
    con = con,
    target_table = "df_profile_by_customer",
    column_defs = list(
      list(name = "customer_id", type = "INTEGER", not_null = TRUE),
      list(name = "platform_id", type = "VARCHAR", not_null = TRUE),
      list(name = "buyer_name", type = "VARCHAR"),
      list(name = "email", type = "VARCHAR"),
      list(
        name = "display_name",
        type = "VARCHAR",
        generated_as = "buyer_name || ' (' || coalesce(email,'') || ')'",
        generated_type = "VIRTUAL"
      ),
      list(name = "derivation_timestamp", type = "TIMESTAMP"),
      list(name = "derivation_batch_id", type = "VARCHAR"),
      list(name = "derivation_script", type = "VARCHAR")
    ),
    primary_key = c("platform_id", "customer_id"),
    indexes = list(list(columns = "platform_id")),
    or_replace = or_replace
  )

  if (verbose) {
    message("[DDL] df_profile_by_customer")
    cat(query, "\n")
  }

  DBI::dbExecute(con, query)
  invisible(TRUE)
}
