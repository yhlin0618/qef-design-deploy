#' @file example_create_table_query.R
#' @use DBI
#' @use RSQLite
#' @requires 01_db/generate_create_table/fn_generate_create_table_query.R
#' @requires 02_db_utils/fn_print_query.R
#' 
#' @title Example Script for Generate Create Table Query Function
#' @description Demonstrates how to use the generate_create_table_query function
#' with various options, particularly focusing on composite keys and constraints.
#'

# Source the function
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))


conn <- dbConnect_from_list("app_data")
r_code <- generate_create_table_query(con=conn, source_table="df_dna_by_customer", output_format="r")
cat(r_code)

# sqltest<- generate_create_table_query(
#   con = app_data,
#   or_replace = TRUE,
#   target_table = "df_profile_by_customer",
#   source_table = NULL,
#   column_defs = list(
#     list(name = "customer_id", type = "INTEGER"),
#     list(name = "buyer_name", type = "VARCHAR"),
#     list(name = "email", type = "VARCHAR"),
#     list(name = "platform_id", type = "INTEGER", not_null = TRUE), 
#     list(name = "display_name", type = "VARCHAR", not_null = TRUE,
#          generated_as = "buyer_name || ' (' || email || ')'")
#   ),
#   primary_key = c("customer_id","platform_id"),
#   indexes = list(
#     list(columns = "platform_id")
#   )
# )

sqltest<- generate_create_table_query(
  con = conn,
  target_table = "df_dna_by_customer",
  source_table = NULL,
  column_defs = list(
    list(name = "customer_id", type = "INTEGER", not_null = TRUE),
    list(name = "ipt", type = "DOUBLE"),
    list(name = "total_spent", type = "DOUBLE"),
    list(name = "times", type = "INTEGER"),
    list(name = "zipcode", type = "BOOLEAN"),
    list(name = "state", type = "BOOLEAN"),
    list(name = "lat", type = "BOOLEAN"),
    list(name = "lng", type = "BOOLEAN"),
    list(name = "ipt_mean", type = "DOUBLE"),
    list(name = "m_value", type = "DOUBLE"),
    list(name = "sigma_hnorm_mle", type = "DOUBLE"),
    list(name = "sigma_hnorm_bcmle", type = "DOUBLE"),
    list(name = "m_ecdf", type = "DOUBLE"),
    list(name = "m_label", type = "ENUM('Low Value', 'Medium Value', 'High Value')"),
    list(name = "ni", type = "INTEGER"),
    list(name = "date", type = "TIMESTAMP"),
    list(name = "sum_spent_by_date", type = "DOUBLE"),
    list(name = "count_transactions_by_date", type = "INTEGER"),
    list(name = "min_time_by_date", type = "TIMESTAMP"),
    list(name = "ni_2", type = "INTEGER"),
    list(name = "min_time", type = "TIMESTAMP"),
    list(name = "payment_time", type = "TIMESTAMP"),
    list(name = "difftime", type = "INTERVAL"),
    list(name = "nes_ratio", type = "DOUBLE"),
    list(name = "nes_status", type = "ENUM('N', 'E0', 'S1', 'S2', 'S3')", default="'N'"),
    list(name = "f_value", type = "INTEGER"),
    list(name = "f_ecdf", type = "DOUBLE"),
    list(name = "f_label", type = "ENUM('Low Frequency', 'Medium Frequency', 'High Frequency')"),
    list(name = "r_value", type = "DOUBLE"),
    list(name = "r_ecdf", type = "DOUBLE"),
    list(name = "r_label", type = "ENUM('Long Inactive', 'Medium Inactive', 'Recent Buyer')"),
    list(name = "mle", type = "DOUBLE"),
    list(name = "wmle", type = "DOUBLE"),
    list(name = "cai", type = "DOUBLE"),
    list(name = "cai_ecdf", type = "DOUBLE"),
    list(name = "cai_label", type = "ENUM('Gradually Inactive', 'Stable', 'Increasingly Active')"),
    list(name = "pcv", type = "DOUBLE"),
    list(name = "total_sum", type = "DOUBLE"),
    list(name = "clv", type = "DOUBLE"),
    list(name = "time_first", type = "TIMESTAMP"),
    list(name = "nt", type = "DOUBLE"),
    list(name = "time_first_to_now", type = "DOUBLE"),
    list(name = "e0t", type = "DOUBLE"),
    list(name = "ge", type = "DOUBLE"),
    list(name = "ie", type = "DOUBLE"),
    list(name = "be", type = "DOUBLE"),
    list(name = "cri", type = "DOUBLE"),
    list(name = "cri_ecdf", type = "DOUBLE"),
    list(name = "be2", type = "DOUBLE"),
    list(name = "nrec", type = "ENUM('nrec', 'rec')"),
    list(name = "nrec_prob", type = "DOUBLE"),
    list(name = "platform_id", type = "INTEGER", not_null = TRUE)
  ),
)

print_query(sqltest, "CREATE TABLE df_dna_by_customer")

# Close connection
dbDisconnect(conn)

cat("\nExamples completed successfully.\n")

