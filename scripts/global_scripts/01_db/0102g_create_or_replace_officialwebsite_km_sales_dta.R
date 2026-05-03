create_or_replace_officialwebsite_km_sales <- function(con) {
  # 定義表格名稱
  table_name <- "officialwebsite_sales_dta"
  
  # 定義建立表格的 SQL 語句（包含刪除舊有表格）
  create_table_sql <- "
    DROP TABLE IF EXISTS officialwebsite_sales_dta;
    CREATE TABLE officialwebsite_sales_dta (
      receipt_name VARCHAR NOT NULL,
      email VARCHAR,
      financial_status VARCHAR,
      paid_at VARCHAR,
      fulfillment_status VARCHAR,
      fulfilled_at VARCHAR,
      accepts_marketing BOOLEAN,
      currency CHAR(3),
      subtotal DOUBLE,
      shipping DOUBLE,
      taxes DOUBLE,
      total DOUBLE,
      discount_code VARCHAR,
      discount_amount DOUBLE,
      shipping_method VARCHAR,
      created_at VARCHAR,
      lineproduct_quantity UINTEGER,
      lineproduct_name VARCHAR,
      lineproduct_price DOUBLE,
      lineproduct_compare_at_price DOUBLE,
      lineproduct_sku VARCHAR,
      lineproduct_requires_shipping BOOLEAN,
      lineproduct_taxable BOOLEAN,
      lineproduct_fulfillment_status VARCHAR,
      billing_name VARCHAR,
      billing_street VARCHAR,
      billing_address1 VARCHAR,
      billing_address2 VARCHAR,
      billing_company VARCHAR,
      billing_city VARCHAR,
      billing_zip VARCHAR,
      billing_province CHAR(2),
      billing_country CHAR(2),
      billing_phone VARCHAR,
      shipping_name VARCHAR,
      shipping_street VARCHAR,
      shipping_address1 VARCHAR,
      shipping_address2 VARCHAR,
      shipping_company VARCHAR,
      shipping_city VARCHAR,
      shipping_zip VARCHAR,
      shipping_province VARCHAR,
      shipping_country VARCHAR,
      shipping_phone VARCHAR,
      notes VARCHAR,
      note_attributes VARCHAR,
      cancelled_at VARCHAR,
      payment_method VARCHAR,
      payment_reference VARCHAR,
      refunded_amount DOUBLE,
      vendor VARCHAR,
      transaction_id CHAR,
      tags VARCHAR,
      risk_level VARCHAR,
      source VARCHAR,
      lineproduct_discount DOUBLE,
      tax1_name VARCHAR,
      tax1_value DOUBLE,
      tax2_name VARCHAR,
      tax2_value DOUBLE,
      tax3_name VARCHAR,
      tax3_value DOUBLE,
      tax4_name VARCHAR,
      tax4_value DOUBLE,
      tax5_name VARCHAR,
      tax5_value DOUBLE,
      phone VARCHAR,
      receipt_number VARCHAR,
      duties VARCHAR,
      billing_province_name VARCHAR,
      shipping_province_name VARCHAR,
      payment_id VARCHAR,
      payment_terms_name VARCHAR,
      next_payment_due_at VARCHAR,
      payment_references VARCHAR
    );
  "
  
  # 由於無額外索引需求，索引語句設為空向量
  indexes <- character(0)
  
  # 調用預先定義好的 setup_table 函數來執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出表格結構以驗證
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}
