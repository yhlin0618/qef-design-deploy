# Create df_dna_by_customer table (DuckDB)
# PK: (platform_id, customer_id, product_line_id_filter)
# Uses generate_create_table_query (MP058, R092)

if (!exists("generate_create_table_query", mode = "function")) {
  gen_path <- file.path("scripts", "global_scripts", "01_db", "generate_create_table_query", "fn_generate_create_table_query.R")
  if (file.exists(gen_path)) source(gen_path) else stop("generate_create_table_query.R not found")
}

create_df_dna_by_customer_table <- function(con, or_replace = TRUE, verbose = FALSE) {
  stopifnot(inherits(con, "DBIConnection"))

  num <- function(name, not_null = FALSE) list(name = name, type = "DOUBLE", not_null = not_null)
  int <- function(name, not_null = FALSE) list(name = name, type = "INTEGER", not_null = not_null)
  txt <- function(name, not_null = FALSE) list(name = name, type = "VARCHAR", not_null = not_null)
  ts  <- function(name) list(name = name, type = "TIMESTAMP")

  query <- generate_create_table_query(
    con = con,
    target_table = "df_dna_by_customer",
    column_defs = list(
      int("customer_id", TRUE),
      txt("platform_id", TRUE),
      txt("product_line_id_filter"),
      num("ipt"), num("total_spent"), num("times"),
      num("zipcode"), num("state"), num("lat"), num("lng"),
      num("ipt_mean"), num("m_value"), num("ni"),
      num("sigma_hnorm_mle"), num("sigma_hnorm_bcmle"),
      num("m_ecdf"), txt("m_label"),
      ts("date"), num("sum_spent_by_date"), num("count_transactions_by_date"),
      ts("min_time_by_date"), ts("payment_time"),
      num("ni_ipt"), num("difftime"), num("nes_ratio"),
      txt("nes_status"), num("f_value"), num("f_ecdf"), txt("f_label"),
      num("r_value"), num("r_ecdf"), txt("r_label"),
      num("mle"), num("wmle"),
      num("cai_value"), num("cai_ecdf"), txt("cai_label"),
      num("pcv"), num("total_sum"), num("clv"),
      ts("time_first"), num("nt"), num("time_first_to_now"),
      num("e0t"), num("ge"), num("ie"), num("be"),
      num("cri"), num("cri_ecdf"), num("be2"),
      num("nrec_prob"), txt("nrec"), num("p_alive"), num("btyd_expected_transactions"), num("nes_value"),
      num("cai"), num("dna_m_score"), num("dna_f_score"), num("dna_r_score"),
      txt("dna_segment"),
      ts("derivation_timestamp"), txt("derivation_batch_id"), txt("derivation_script")
    ),
    primary_key = c("platform_id", "customer_id", "product_line_id_filter"),
    indexes = list(
      list(columns = "platform_id"),
      list(columns = c("platform_id", "product_line_id_filter"))
    ),
    or_replace = or_replace
  )

  if (verbose) {
    message("[DDL] df_dna_by_customer")
    cat(query, "\n")
  }

  DBI::dbExecute(con, query)
  invisible(TRUE)
}
