#' Validate ETL+DRV Pipeline Compliance
#' 
#' Generic validation framework for MAMBA ETL+DRV implementations
#' Implements principle compliance checks per MP029, MP108, MP109, MP064, MP102
#' 
#' @param domain Domain name (e.g., "precision", "cbz", "eby")
#' @param etl_stages Vector of ETL stages to validate (c("0IM", "1ST", "2TR"))
#' @param drv_files Vector of DRV file basenames (without path/extension)
#' @param custom_checks Optional list of domain-specific validation functions
#' @return List of validation results with compliance flags
#' 
#' @examples
#' # Validate precision marketing pipeline
#' results <- fn_validate_etl_drv_template(
#'   domain = "precision",
#'   etl_stages = c("0IM", "1ST", "2TR"),
#'   drv_files = c("feature_preparation", "time_series", "poisson_analysis")
#' )
#' 
#' @export
fn_validate_etl_drv_template <- function(domain,
                                         etl_stages = c("0IM", "1ST", "2TR"),
                                         drv_files,
                                         custom_checks = list()) {
  
  library(dplyr)
  library(DBI)
  library(duckdb)
  
  message(sprintf("=== Validating %s ETL+DRV Pipeline ===", toupper(domain)))
  message(sprintf("Timestamp: %s\n", Sys.time()))
  
  validation_results <- list()
  
  # ============================================================
  # MP108 Validation: ETL Stage Separation
  # ============================================================
  
  message("\n[MP108] Validating ETL stage separation...")
  
  for (stage in etl_stages) {
    etl_file <- sprintf("scripts/update_scripts/ETL/%s/%s_ETL_product_profiles_%s.R", 
                       domain, domain, stage)
    
    if (!file.exists(etl_file)) {
      validation_results[[sprintf("mp108_%s_exists", stage)]] <- list(
        principle = "MP108",
        check = sprintf("ETL %s file exists", stage),
        compliant = FALSE,
        error = sprintf("File not found: %s", etl_file)
      )
      next
    }
    
    message(sprintf("  ✓ Found ETL %s: %s", stage, etl_file))
    
    file_content <- readLines(etl_file)
    
    # Stage-specific checks
    if (stage == "0IM") {
      # 0IM should ONLY import, no transformations
      has_transformation <- any(grepl("mutate(?!.*data_source)|transform|convert|standardize", 
                                     file_content, ignore.case = TRUE, perl = TRUE))
      
      validation_results[[sprintf("mp108_0im_no_transform")]] <- list(
        principle = "MP108",
        check = "0IM stage has no transformations",
        compliant = !has_transformation,
        detail = if(has_transformation) "Found transformation in 0IM stage (violation)" else "Clean import only"
      )
      
      message(sprintf("    %s 0IM no transformations: %s", 
                     if(!has_transformation) "✓" else "✗",
                     if(!has_transformation) "PASS" else "FAIL"))
    }
    
    if (stage == "1ST") {
      # 1ST should have R116 currency conversion
      has_currency_conversion <- any(grepl("fn_convert_currency|convert.*currency|currency.*usd", 
                                          file_content, ignore.case = TRUE))
      
      validation_results[[sprintf("r116_currency_conversion")]] <- list(
        principle = "R116",
        check = "1ST stage has currency conversion",
        compliant = has_currency_conversion,
        detail = if(has_currency_conversion) "R116 currency standardization present" else "Missing R116 implementation"
      )
      
      message(sprintf("    %s R116 currency conversion: %s", 
                     if(has_currency_conversion) "✓" else "✗",
                     if(has_currency_conversion) "PASS" else "FAIL"))
    }
  }
  
  # ============================================================
  # MP109 Validation: DRV Layer Separation
  # ============================================================
  
  message("\n[MP109] Validating DRV layer separation...")
  
  for (drv_file in drv_files) {
    drv_path <- sprintf("scripts/update_scripts/DRV/%s/%s_DRV_%s.R", 
                       domain, domain, df_file)
    
    if (!file.exists(drv_path)) {
      validation_results[[sprintf("mp109_%s_exists", drv_file)]] <- list(
        principle = "MP109",
        check = sprintf("DRV file %s exists", df_file),
        compliant = FALSE,
        error = sprintf("File not found: %s", drv_path)
      )
      next
    }
    
    message(sprintf("  ✓ Found DRV: %s", df_file))
    
    file_content <- readLines(drv_path)
    
    # DRV should NOT import external data
    has_external_import <- any(grepl("read_sheet|read_csv.*http|GET\\(|POST\\(", 
                                    file_content, ignore.case = TRUE))
    
    validation_results[[sprintf("mp109_%s_no_external", drv_file)]] <- list(
      principle = "MP109",
      check = sprintf("DRV %s has no external import", df_file),
      compliant = !has_external_import,
      detail = if(has_external_import) "DRV importing external data (violation)" else "Reads from DuckDB only"
    )
    
    message(sprintf("    %s No external imports: %s", 
                   if(!has_external_import) "✓" else "✗",
                   if(!has_external_import) "PASS" else "FAIL"))
    
    # DRV should read from transformed_data.duckdb
    reads_transformed <- any(grepl("transformed_data\\.duckdb", file_content))
    
    validation_results[[sprintf("mp109_%s_reads_transformed", drv_file)]] <- list(
      principle = "MP109",
      check = sprintf("DRV %s reads from transformed_data", df_file),
      compliant = reads_transformed,
      detail = if(reads_transformed) "Correctly reads ETL output" else "Not reading from transformed_data"
    )
    
    message(sprintf("    %s Reads transformed_data: %s", 
                   if(reads_transformed) "✓" else "✗",
                   if(reads_transformed) "PASS" else "FAIL"))
  }
  
  # ============================================================
  # MP029 Validation: No Fake Data
  # ============================================================
  
  message("\n[MP029] Validating no fake data...")
  
  # Check for R117 compliance in time series files
  if ("time_series" %in% drv_files) {
    con <- dbConnect(duckdb::duckdb(), "data/processed_data.duckdb", read_only = TRUE)
    
    if (dbExistsTable(con, sprintf("df_%s_time_series", domain))) {
      time_series_data <- dbReadTable(con, sprintf("df_%s_time_series", domain))
      
      # R117: data_source column exists
      has_data_source <- "data_source" %in% names(time_series_data)
      
      validation_results[["r117_data_source_exists"]] <- list(
        principle = "R117",
        check = "Time series has data_source column (R117)",
        compliant = has_data_source,
        detail = if(has_data_source) "R117 transparency markers present" else "Missing data_source column"
      )
      
      message(sprintf("  %s R117 data_source exists: %s", 
                     if(has_data_source) "✓" else "✗",
                     if(has_data_source) "PASS" else "FAIL"))
      
      if (has_data_source) {
        # Check values are valid
        valid_sources <- all(time_series_data$data_source %in% c("REAL", "FILLED", NA))
        fill_rate <- mean(time_series_data$data_source == "FILLED", na.rm = TRUE)
        
        validation_results[["r117_valid_markers"]] <- list(
          principle = "R117",
          check = "data_source values are valid",
          compliant = valid_sources && fill_rate < 0.80,
          detail = sprintf("Fill rate: %.1f%% (target: <80%%)", fill_rate * 100)
        )
        
        message(sprintf("  %s R117 valid markers (fill rate %.1f%%): %s", 
                       if(valid_sources && fill_rate < 0.80) "✓" else "✗",
                       fill_rate * 100,
                       if(valid_sources && fill_rate < 0.80) "PASS" else "FAIL"))
      }
    } else {
      message(sprintf("  ⚠ Table df_%s_time_series not found in processed_data.duckdb", domain))
    }
    
    dbDisconnect(con, shutdown = TRUE)
  }
  
  # Check for R118 compliance in Poisson analysis
  if ("poisson_analysis" %in% drv_files || "poisson_features" %in% drv_files) {
    con <- dbConnect(duckdb::duckdb(), "data/processed_data.duckdb", read_only = TRUE)
    
    if (dbExistsTable(con, sprintf("df_%s_poisson_analysis", domain))) {
      poisson_data <- dbReadTable(con, sprintf("df_%s_poisson_analysis", domain))
      
      # R118: p_value and significance_flag columns exist
      has_p_value <- "p_value" %in% names(poisson_data)
      has_sig_flag <- "significance_flag" %in% names(poisson_data)
      
      validation_results[["r118_significance_present"]] <- list(
        principle = "R118",
        check = "Poisson analysis has p_value and significance_flag (R118)",
        compliant = has_p_value && has_sig_flag,
        detail = if(has_p_value && has_sig_flag) "R118 statistical significance documentation complete" else "Missing R118 fields"
      )
      
      message(sprintf("  %s R118 significance fields: %s", 
                     if(has_p_value && has_sig_flag) "✓" else "✗",
                     if(has_p_value && has_sig_flag) "PASS" else "FAIL"))
      
      # Check for variable range metadata (MP029 - no guessing)
      range_fields <- c("predictor_min", "predictor_max", "predictor_range")
      has_range_metadata <- all(range_fields %in% names(poisson_data))
      
      validation_results[["mp029_no_range_guessing"]] <- list(
        principle = "MP029",
        check = "Variable ranges calculated from data (not guessed)",
        compliant = has_range_metadata,
        detail = if(has_range_metadata) "Actual ranges stored (MP029 compliance)" else "Missing range metadata"
      )
      
      message(sprintf("  %s MP029 range metadata: %s", 
                     if(has_range_metadata) "✓" else "✗",
                     if(has_range_metadata) "PASS" else "FAIL"))
    } else {
      message(sprintf("  ⚠ Table df_%s_poisson_analysis not found in processed_data.duckdb", domain))
    }
    
    dbDisconnect(con, shutdown = TRUE)
  }
  
  # ============================================================
  # MP064 Validation: ETL-DRV Separation
  # ============================================================
  
  message("\n[MP064] Validating ETL-DRV separation...")
  
  # Check ETL files don't have aggregations
  for (stage in etl_stages) {
    etl_file <- sprintf("scripts/update_scripts/ETL/%s/%s_ETL_product_profiles_%s.R", 
                       domain, domain, stage)
    
    if (file.exists(etl_file)) {
      file_content <- readLines(etl_file)
      has_aggregation <- any(grepl("group_by.*summarize|aggregate\\(", file_content))
      
      validation_results[[sprintf("mp064_etl_%s_no_agg", stage)]] <- list(
        principle = "MP064",
        check = sprintf("ETL %s has no cross-record aggregation", stage),
        compliant = !has_aggregation,
        detail = if(has_aggregation) "ETL doing aggregation (should be in DRV)" else "No aggregation in ETL"
      )
      
      message(sprintf("  %s ETL %s no aggregations: %s", 
                     if(!has_aggregation) "✓" else "✗",
                     stage,
                     if(!has_aggregation) "PASS" else "FAIL"))
    }
  }
  
  # ============================================================
  # MP102 Validation: Completeness & Standardization
  # ============================================================
  
  message("\n[MP102] Validating completeness and standardization...")
  
  # Check metadata files exist
  expected_metadata <- c(
    "metadata/variable_name_transformations.csv",
    "metadata/dummy_encoding_metadata.csv",
    "metadata/time_series_filling_stats.csv",
    "metadata/country_extraction_metadata.csv"
  )
  
  for (meta_file in expected_metadata) {
    exists <- file.exists(meta_file)
    validation_results[[sprintf("mp102_%s", basename(meta_file))]] <- list(
      principle = "MP102",
      check = sprintf("Metadata file %s exists", basename(meta_file)),
      compliant = exists,
      detail = if(exists) sprintf("Found: %s", meta_file) else sprintf("Missing: %s", meta_file)
    )
    
    message(sprintf("  %s %s: %s", 
                   if(exists) "✓" else "✗",
                   basename(meta_file),
                   if(exists) "PASS" else "FAIL"))
  }
  
  # ============================================================
  # Database Validation
  # ============================================================
  
  message("\n[DATABASE] Validating database existence...")
  
  expected_dbs <- c(
    "data/raw_data.duckdb",
    "data/staged_data.duckdb",
    "data/transformed_data.duckdb",
    "data/processed_data.duckdb"
  )
  
  for (db_file in expected_dbs) {
    exists <- file.exists(db_file)
    validation_results[[sprintf("db_%s", basename(db_file))]] <- list(
      principle = "MP108",
      check = sprintf("Database %s exists", basename(db_file)),
      compliant = exists,
      detail = if(exists) sprintf("Size: %.2f MB", file.size(db_file) / 1024^2) else "Missing"
    )
    
    message(sprintf("  %s %s: %s", 
                   if(exists) "✓" else "✗",
                   basename(db_file),
                   if(exists) sprintf("PASS (%.2f MB)", file.size(db_file) / 1024^2) else "FAIL"))
  }
  
  # ============================================================
  # Custom Domain Checks
  # ============================================================
  
  if (length(custom_checks) > 0) {
    message("\n[CUSTOM] Running domain-specific checks...")
    
    for (check_name in names(custom_checks)) {
      check_function <- custom_checks[[check_name]]
      validation_results[[check_name]] <- tryCatch({
        result <- check_function()
        message(sprintf("  %s %s: %s", 
                       if(result$compliant) "✓" else "✗",
                       check_name,
                       if(result$compliant) "PASS" else "FAIL"))
        result
      }, error = function(e) {
        message(sprintf("  ✗ %s: ERROR - %s", check_name, as.character(e)))
        list(
          principle = "CUSTOM",
          check = check_name,
          compliant = FALSE,
          error = as.character(e)
        )
      })
    }
  }
  
  # ============================================================
  # Compile Results
  # ============================================================
  
  message("\n[SUMMARY] Compiling validation results...")
  
  results_df <- bind_rows(lapply(names(validation_results), function(name) {
    result <- validation_results[[name]]
    tibble(
      check_id = name,
      principle = result$principle %||% "UNKNOWN",
      check_description = result$check %||% name,
      compliant = result$compliant %||% FALSE,
      detail = result$detail %||% result$error %||% ""
    )
  }))
  
  compliance_rate <- mean(results_df$compliant, na.rm = TRUE)
  total_checks <- nrow(results_df)
  passed_checks <- sum(results_df$compliant)
  failed_checks <- total_checks - passed_checks
  
  message(sprintf("\n=== Validation Complete: %.1f%% Compliant ===", compliance_rate * 100))
  message(sprintf("Passed: %d / %d checks", passed_checks, total_checks))
  message(sprintf("Failed: %d checks", failed_checks))
  
  if (compliance_rate < 1.0) {
    message("\n⚠️ FAILURES:")
    failed_checks_df <- results_df %>% filter(!compliant)
    for (i in 1:nrow(failed_checks_df)) {
      message(sprintf("  - [%s] %s: %s", 
                     failed_checks_df$principle[i],
                     failed_checks_df$check_description[i],
                     failed_checks_df$detail[i]))
    }
  } else {
    message("\n✅ ALL CHECKS PASSED!")
  }
  
  return(list(
    results = results_df,
    compliance_rate = compliance_rate,
    total_checks = total_checks,
    passed_checks = passed_checks,
    failed_checks = failed_checks,
    timestamp = Sys.time()
  ))
}
