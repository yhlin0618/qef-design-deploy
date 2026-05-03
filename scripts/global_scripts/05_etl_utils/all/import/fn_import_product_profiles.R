#' Import product Profiles from Google Sheets to Raw Data (Universal Import Function)
#'
#' This is a platform-agnostic function that performs pure data import of product profile data 
#' for active product lines from a Google Sheet to the raw_data database. It can be used by 
#' any platform ETL pipeline (Amazon, eBay, Official Website, etc.) and only adds minimal 
#' import metadata without any staging or transformation logic.
#'
#' This function is designed for ETL Phase 0 (Import) and follows the universal import pattern
#' defined in the ETL00 framework.
#'
#' @param db_connection A DBI connection to the database where data will be written
#' @param product_line_df A dataframe containing product line information (default: df_product_line)
#' @param google_sheet_id The ID of the Google Sheet containing product profile data (required)
#' @param sheet_name_prefix Prefix for sheet names (default: "product_profile"), will add "_" automatically
#'
#' @return A list of imported data frames with import metadata, named by product line ID
#' @note This function creates tables with names: df_product_profile_{product_line_id}
#'       Staging operations should be performed separately in ETL Phase 1 (Staging)
#'       This function is platform-agnostic and can be used by any platform ETL pipeline
#'
#' @examples
#' \dontrun{
#' # Connect to database
#' dbConnect_from_list("raw_data")
#' 
#' # Import product profiles for all active product lines
#' import_product_profiles(
#'   db_connection = raw_data,
#'   product_line_df = df_product_line,
#'   google_sheet_id = "16-k48xxFzSZm2p8j9SZf4V041fldcYnR8ectjsjuxZQ"
#' )
#' }
#'
#' @export
import_product_profiles <- function(db_connection = raw_data,
                               product_line_df = df_product_line,
                               google_sheet_id = NULL,
                               sheet_name_prefix = "product_profile") {
  
  # Validate parameters
  if (is.null(google_sheet_id)) {
    stop("google_sheet_id is required")
  }
  
  # Make sure required packages are loaded
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    message("Loading package 'googlesheets4'")
    library(googlesheets4)
  }
  
  # Ensure sheet_name_prefix ends with an underscore
  if (!endsWith(sheet_name_prefix, "_")) {
    sheet_name_prefix <- paste0(sheet_name_prefix, "_")
  }
  
  # Connect to the Google Sheet
  googlesheet_con <- googlesheets4::as_sheets_id(google_sheet_id)
  
  # Initialize results list
  results <- list()
  
  # Filter active product lines (exclude 'all') — config-driven (#363)
  product_lines <- get_active_product_lines(product_line_df = product_line_df) %>%
    dplyr::select(id = product_line_id, name = product_line_name_chinese)
  
  # Process each product line
  for (i in 1:nrow(product_lines)) {
    product_line_id <- product_lines$id[i]
    product_line_name <- product_lines$name[i]
    
    # Construct full sheet name - default is now "product_profile_[product_line_name]"
    sheet_name <- paste0(sheet_name_prefix, product_line_name)
    
    # Import data from Google Sheet
    tryCatch({
      message(paste("Importing data for product line:", product_line_name, "from sheet:", sheet_name))
      
      # Read raw data (Phase 0: Import only)
      df_product_profile <- googlesheets4::read_sheet(googlesheet_con, sheet = sheet_name, .name_repair = "minimal")

      # 檢查重複欄位名稱：保留第一次，發出警告
      dup_logical <- duplicated(names(df_product_profile))
      if (any(dup_logical)) {
        dup_cols <- names(df_product_profile)[dup_logical]
        warning("Google Sheet ", sheet_name, " 含有重複欄位：",
                paste(unique(dup_cols), collapse = ", "),
                "。已保留第一次出現的欄位，移除後續重複欄位。")
        df_product_profile <- df_product_profile[, !dup_logical, drop = FALSE]
      }

      # Import phase: 最小必要格式處理以確保能寫入資料庫
      # 完整格式處理將在 Staging 階段進行
      
      # 將 list 欄位轉為 character，避免 DuckDB 寫入失敗
      list_cols <- sapply(df_product_profile, is.list)
      if (any(list_cols)) {
        df_product_profile[list_cols] <- lapply(df_product_profile[list_cols], function(x) {
          vapply(x, toString, character(1))
        })
      }
      
      # 使用專門的 UTF-8 處理函數，確保編碼正確且移除非法字符
      df_product_profile <- convert_all_columns_to_utf8(df_product_profile)
      df_product_profile <- remove_illegal_utf8(df_product_profile)
      
      # Add minimal import metadata only
      df_product_profile <- df_product_profile %>%
        dplyr::mutate(
          etl_import_timestamp = Sys.time(),
          etl_import_source = sheet_name,
          etl_product_line_name = product_line_name,
          etl_product_line_id = product_line_id,
          etl_phase = "import"
        )
      
      # Create raw data table name (Phase 0 naming)
      table_name <- paste0("df_product_profile_", product_line_id)
      
      # Write to database
      DBI::dbWriteTable(db_connection, table_name, df_product_profile, overwrite = TRUE)
      
      # Store in results
      results[[product_line_id]] <- df_product_profile

      # Log success
      message(paste("Successfully imported", nrow(df_product_profile), "products for product line:", product_line_name))

    }, error = function(e) {
      error_msg <- paste("Error importing data for product line", product_line_name, ":", e$message)
      warning(error_msg)
    })
  }

  # ============================================================================
  # qef-product-master-redesign tasks 3.1 + 3.2:
  # Single catalogue master per Gsheet layout, per-line tabs preserved.
  #
  # Union all per-line tables into df_amz_product_master with marketplace col.
  # Per-line `df_product_profile_*` tables are still written above (backward
  # compat — existing DRV scripts continue to work).
  # ============================================================================
  if (length(results) > 0) {
    tryCatch({
      # Determine marketplace default. Prefer app_config's marketplace_default
      # under etl_sources.product_profiles or company_product_master if available.
      marketplace_default <- "amz_us"
      if (exists("app_configs", inherits = TRUE)) {
        cfg_amz <- tryCatch(get("app_configs", inherits = TRUE)$platforms$amz, error = function(e) NULL)
        cfg_master <- cfg_amz$etl_sources$company_product_master
        if (!is.null(cfg_master) && !is.null(cfg_master$marketplace_default) &&
            nzchar(as.character(cfg_master$marketplace_default))) {
          marketplace_default <- as.character(cfg_master$marketplace_default)
        }
      }

      # Reduce per-line frames to common columns for union, augment per row.
      master_rows <- lapply(names(results), function(pl_id) {
        df <- results[[pl_id]]
        # Standardise minimal columns; downstream 1ST/2TR will handle full schema.
        out <- data.frame(
          amz_asin = if ("ASIN" %in% names(df)) as.character(df$ASIN) else NA_character_,
          marketplace = marketplace_default,
          product_line_id = pl_id,
          brand = if ("品牌" %in% names(df)) as.character(df[["品牌"]])
                  else if ("brand" %in% names(df)) as.character(df$brand)
                  else NA_character_,
          product_name = if ("商品名稱" %in% names(df))
                            as.character(df[["商品名稱"]])
                          else if ("product_name" %in% names(df)) as.character(df$product_name)
                          else NA_character_,
          stringsAsFactors = FALSE
        )
        out[!is.na(out$amz_asin) & nzchar(trimws(out$amz_asin)), , drop = FALSE]
      })

      df_master <- do.call(rbind, master_rows)
      if (!is.null(df_master) && nrow(df_master) > 0) {
        # Standardise marketplace column (D2: backfill default if missing)
        df_master$marketplace[is.na(df_master$marketplace) | !nzchar(df_master$marketplace)] <-
          marketplace_default

        DBI::dbWriteTable(db_connection, "df_amz_product_master", df_master, overwrite = TRUE)
        message(sprintf(
          "Wrote df_amz_product_master with %d ASINs across %d product lines (marketplace_default=%s)",
          nrow(df_master), length(unique(df_master$product_line_id)), marketplace_default
        ))
      } else {
        message("df_amz_product_master: no rows produced (all product lines empty or missing ASIN)")
      }
    }, error = function(e) {
      warning(paste("Failed to build df_amz_product_master union:", e$message), call. = FALSE)
    })
  }

  return(results)
}
