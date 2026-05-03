#!/usr/bin/env Rscript
# seed_qef_company_product_master.R
#
# One-off helper: suggest SKU → product_line_id mapping for QEF_DESIGN
# based on sales ASIN overlap with df_amz_product_master (catalogue) + brand.
#
# Output: data/app_data/parameters/scd_type1/qef_company_master_seed_suggestions.csv
#
# This does NOT write to Gsheet or any DB. It produces a CSV for the business
# team to review, correct, and paste into the Gsheet `company_product_master` tab.
#
# Part of qef-product-master-redesign spectra change, task 6.5.1.

suppressMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
})

# Simple working-dir check — must run from QEF_DESIGN project root.
if (!file.exists("app_config.yaml")) {
  stop("Must run from QEF_DESIGN project root (app_config.yaml not found in cwd)")
}

raw_path <- "data/local_data/raw_data.duckdb"
if (!file.exists(raw_path)) {
  stop("raw_data.duckdb not found. Run ETL 0IM first.")
}

con <- dbConnect(duckdb(), raw_path, read_only = TRUE)
on.exit(dbDisconnect(con))

# ---- Collect sales ASINs (self-orders) -------------------------------------
if (!"df_amazon_sales" %in% dbListTables(con)) {
  stop("df_amazon_sales not found. Run amz_ETL_sales_0IM first.")
}
sales_asins <- dbGetQuery(con, "
  SELECT DISTINCT
    asin,
    sku,
    MAX(product_name) AS product_name,
    COUNT(*) AS n_orders
  FROM df_amazon_sales
  WHERE asin IS NOT NULL
  GROUP BY asin, sku
  ORDER BY n_orders DESC
")
message(sprintf("sales ASINs (self-orders): %d unique", nrow(sales_asins)))

# ---- Load catalogue for product_line + brand hints ------------------------
if (!"df_amz_product_master" %in% dbListTables(con)) {
  message("df_amz_product_master not found — product_line suggestions will be NA.")
  catalogue <- data.frame(
    amz_asin = character(0),
    product_line_id = character(0),
    brand = character(0),
    stringsAsFactors = FALSE
  )
} else {
  catalogue <- dbGetQuery(con, "
    SELECT DISTINCT amz_asin, product_line_id, brand
    FROM df_amz_product_master
  ")
  message(sprintf("catalogue rows: %d", nrow(catalogue)))
}

# ---- Join sales to catalogue by ASIN --------------------------------------
suggestions <- sales_asins %>%
  left_join(catalogue, by = c("asin" = "amz_asin")) %>%
  mutate(
    marketplace = "amz_us",  # D2 default; adjust in Gsheet if multi-marketplace
    suggested_product_line_id = product_line_id,
    suggested_brand = brand,
    confidence = case_when(
      !is.na(product_line_id) ~ "high (catalogue match)",
      TRUE ~ "low (no catalogue match — fill manually)"
    )
  ) %>%
  select(
    sku, marketplace, amz_asin = asin,
    suggested_product_line_id, suggested_brand,
    product_name, n_orders, confidence
  ) %>%
  arrange(desc(n_orders))

# ---- Write CSV -------------------------------------------------------------
out_dir <- file.path("data", "app_data", "parameters", "scd_type1")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "qef_company_master_seed_suggestions.csv")

# Write with UTF-8 BOM for Excel compatibility (DEV_R051)
con_file <- file(out_path, "wb")
writeBin(charToRaw("\xef\xbb\xbf"), con_file)
close(con_file)
utils::write.table(suggestions, out_path,
                   row.names = FALSE, sep = ",", quote = TRUE,
                   append = TRUE, fileEncoding = "UTF-8")

message(strrep("=", 60))
message(sprintf("Seed suggestions written: %s", out_path))
message(sprintf("Total SKU/ASIN pairs: %d", nrow(suggestions)))
message(sprintf("High confidence (catalogue match): %d",
                sum(suggestions$confidence == "high (catalogue match)")))
message(sprintf("Low confidence (manual fill needed): %d",
                sum(grepl("^low", suggestions$confidence))))
message(strrep("=", 60))
message("")
message("Next steps (business team):")
message("  1. Open the CSV in Excel / Google Sheets")
message("  2. For 'low confidence' rows, fill suggested_product_line_id + suggested_brand manually")
message("  3. Remove any competitor ASINs that shouldn't be in the own master")
message("  4. Paste the curated rows (sku, marketplace, amz_asin, product_line_id, brand)")
message("     into the `company_product_master` tab of the shared Google Sheet")
message("  5. Re-run ETL: amz_ETL_company_product_master_0IM.R")
