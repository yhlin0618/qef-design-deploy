#' Import Amazon Competitor Sales Data
#'
#' Imports Amazon competitor sales data from CSV/Excel files (including no-extension files) in subfolders
#' and writes it to a database table.
#'
#' @param main_folder The main folder containing subfolders with competitor sales data files
#' @param db_connection A database connection object
#' @return A list containing imported rows, imported files, scanned folders, and skipped folder summaries.
#' @export
#'
#' @importFrom readr read_csv cols col_character
#' @importFrom dplyr mutate rename select rename_with
#' @importFrom purrr walk
#' @importFrom stringr str_replace_all str_extract str_remove
#' @importFrom DBI dbWriteTable dbGetQuery dbExecute
#' @importFrom readxl read_excel
#'
library(DBI)
library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)

#------------------------------------------------
# 建立資料表（如尚未存在）
#------------------------------------------------
initialize_table_if_absent <- function(db_connection) {
  message("建立並取代資料表 df_amz_competitor_sales ...")

  create_sql <- generate_create_table_query(
    or_replace = TRUE,
    con = db_connection,
    target_table = "df_amz_competitor_sales",
    column_defs = list(
      list(name = "asin",  type = "VARCHAR", not_null = TRUE),
      list(name = "date",  type = "DATE", not_null = TRUE),
      list(name = "product_line_id", type = "VARCHAR", not_null = TRUE),
      list(name = "sales", type = "INTEGER"),
      list(name = "trend_line", type = "NUMERIC"),
      list(name = "seven_day_moving_average", type = "NUMERIC")
    ),
      primary_key = c("asin", "date", "product_line_id")
  )

  print_query(create_sql, "建立 df_amz_competitor_sales 的 SQL")
  dbExecute(db_connection, create_sql)
}

#------------------------------------------------
# 安全讀檔（支援 CSV、Excel，含無副檔名）
#------------------------------------------------
detect_readers <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext == "csv") return("csv")
  if (ext %in% c("xlsx", "xls")) return("excel")
  if (ext == "") return(c("excel", "csv"))

  stop("不支援的副檔名：", ext, "（支援 csv, xlsx, xls；無副檔名將嘗試 Excel -> CSV）")
}

read_single_file <- function(path, reader, skip = 0) {
  if (reader == "csv") {
    return(readr::read_csv(
      path,
      skip = skip,
      col_types = cols(.default = col_character()),
      show_col_types = FALSE
    ))
  }

  if (reader == "excel") {
    return(as.data.frame(readxl::read_excel(
      path,
      skip = skip,
      col_types = "text"
    )))
  }

  stop("不支援的讀取器：", reader)
}

safe_read_file <- function(path, skip = NA_integer_) {
  readers <- detect_readers(path)

  read_file_with_reader <- function(reader, read_skip) {
    tryCatch(
      {
        as.data.frame(read_single_file(path, reader, skip = read_skip))
      },
      error = function(e) {
        NULL
      }
    )
  }

  # 固定 skip（例如已知要跳過第一列）時，不再做格式探測
  if (!is.na(skip)) {
    for (reader in readers) {
      df <- read_file_with_reader(reader, skip)
      if (!is.null(df)) return(df)
    }
    stop("讀取檔案失敗（固定 skip）：", basename(path))
  }

  # 自動探測：先讀第一列判斷是否有全空行
  for (reader in readers) {
    peek <- read_file_with_reader(reader, 0)
    if (is.null(peek)) next

    has_row <- nrow(peek) > 0
    first_row_all_na <- if (has_row) all(is.na(peek[1, , drop = TRUE]) ) else FALSE
    read_skip <- ifelse(has_row && first_row_all_na, 1L, 0L)

    df <- read_file_with_reader(reader, read_skip)
    if (!is.null(df)) return(df)
  }

  stop("讀取檔案失敗：", basename(path), "（無支援格式可成功解析）")
}

#------------------------------------------------
# 欄位解析與標準化（支援新/舊格式）
#------------------------------------------------
normalize_col_name <- function(cols) {
  cols |>
    as.character() |>
    trimws() |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("\\s+", "") |>
    stringr::str_replace_all("[_\\-()]+", "")
}

normalize_token <- function(values) {
  values |>
    as.character() |>
    trimws() |>
    tolower() |>
    stringr::str_replace_all("[[:space:]_\\-()]+", "") |>
    stringr::str_replace_all("[^[:alnum:]一-龥]+", "")
}

resolve_sales_col <- function(df_colnames, candidates) {
  if (length(df_colnames) == 0L || length(candidates) == 0L) {
    return(NA_character_)
  }

  # 直接逐步嘗試 exact match + normalized match，避免中英/空白差異造成 miss
  exact <- df_colnames[df_colnames %in% candidates]
  if (length(exact)) return(exact[[1L]])

  normalized_df <- normalize_col_name(df_colnames)
  normalized_candidates <- normalize_col_name(candidates)
  idx <- match(normalized_candidates, normalized_df)
  idx <- idx[!is.na(idx)]
  if (length(idx)) return(df_colnames[idx[[1L]]])

  token_df <- normalize_token(df_colnames)
  token_candidates <- normalize_token(candidates)
  idx <- match(token_candidates, token_df)
  idx <- idx[!is.na(idx)]
  if (length(idx)) return(df_colnames[idx[[1L]]])

  NA_character_
}

resolve_import_schema <- function(df_colnames) {
  list(
    date_col = resolve_sales_col(
      df_colnames,
      c("Time", "time", "日期", "最近几月", "月份")
    ),
    sales_col = resolve_sales_col(
      df_colnames,
      c("Sales", "sales", "月销量", "月銷量")
    ),
    trend_line_col = resolve_sales_col(
      df_colnames,
      c("Trend Line", "trendline", "趨勢線", "趋势线")
    ),
    moving_avg_col = resolve_sales_col(
      df_colnames,
      c("7-Day Moving Average", "7 Day Moving Average", "7daymovingaverage", "7天移动平均", "7天移動平均")
    )
  )
}

coerce_import_date <- function(values) {
  vals <- as.character(values)
  vals <- trimws(vals)
  vals[vals == "" | vals == "NA"] <- NA_character_

  parsed <- suppressWarnings(as.Date(vals, format = "%Y-%m-%d"))
  need_parse <- is.na(parsed) & !is.na(vals)
  if (any(need_parse)) {
    parsed[need_parse] <- suppressWarnings(as.Date(
      paste0(vals[need_parse], "-01"),
      format = "%Y-%m-%d"
    ))

    need_parse2 <- is.na(parsed) & !is.na(vals)
    if (any(need_parse2)) {
      parsed[need_parse2] <- suppressWarnings(as.Date(vals[need_parse2], format = "%Y/%m"))
    }

    need_parse3 <- is.na(parsed) & !is.na(vals)
    if (any(need_parse3)) {
      numeric_vals <- suppressWarnings(as.numeric(vals[need_parse3]))
      parsed[need_parse3] <- suppressWarnings(as.Date(numeric_vals, origin = "1899-12-30"))
    }
  }

  parsed <- as.Date(parsed)
  parsed
}

coerce_numeric <- function(values) {
  values_chr <- as.character(values)
  values_chr <- trimws(values_chr)
  values_chr[is.na(values_chr) | values_chr == ""] <- NA_character_
  values_chr <- gsub(",", "", values_chr, fixed = FALSE)
  suppressWarnings(as.numeric(values_chr))
}

add_meta_cols <- function(df, asin, product_line_id, schema) {
  if (is.na(schema$date_col) || is.na(schema$sales_col)) {
    return(data.frame())
  }

  row_count <- NROW(df)
  trend_line <- if (is.na(schema$trend_line_col)) {
    rep(NA_real_, row_count)
  } else {
    coerce_numeric(df[[schema$trend_line_col]])
  }

  moving_average <- if (is.na(schema$moving_avg_col)) {
    rep(NA_real_, row_count)
  } else {
    coerce_numeric(df[[schema$moving_avg_col]])
  }

  data.frame(
    asin = asin,
    date = coerce_import_date(df[[schema$date_col]]),
    product_line_id = product_line_id,
    sales = ceiling(coerce_numeric(df[[schema$sales_col]])),
    trend_line = trend_line,
    seven_day_moving_average = moving_average,
    stringsAsFactors = FALSE
  ) |>
    dplyr::filter(!is.na(date), !is.na(sales))
}

#------------------------------------------------
# 主函式（處理 CSV/Excel 檔案）
#------------------------------------------------
core_import_df_amz_competitor_sales <- function(main_folder, db_connection) {

  # 初始化表格（如不存在則建立）
  initialize_table_if_absent(db_connection)

  if (!dir.exists(main_folder)) {
    stop("VALIDATE FAILED: competitor_sales directory does not exist: ", main_folder)
  }

  sub_folders <- list.dirs(main_folder, full.names = TRUE, recursive = FALSE)
  sub_folders <- sub_folders[file.path(sub_folders) != normalizePath(main_folder)]
  sub_folders <- sub_folders[file.info(sub_folders)$isdir]
  if (length(sub_folders) == 0L) {
    stop("VALIDATE FAILED: no product-line folders found under ", main_folder)
  }

  folder_candidates <- vapply(sub_folders, function(folder) {
    grepl("^\\d{3}_", basename(folder))
  }, logical(1))
  if (!any(folder_candidates)) {
    stop("VALIDATE FAILED: no valid product-line folders (expected format ddd_name) under ", main_folder)
  }
  sub_folders <- sub_folders[folder_candidates]

  total_rows_written <- 0L
  total_files_processed <- 0L
  total_rows_skipped_duplicate <- 0L
  skipped_folders_no_supported_files <- character(0L)
  skipped_folders_no_rows <- character(0L)
  skipped_folders_invalid_reference <- character(0L)
  seen_keys <- character(0L)

  for (folder in sub_folders) {
    folder_name <- basename(folder)
    # 原始：取出三碼數字
    index_str <- str_extract(folder_name, "^\\d{3}")
    if (is.na(index_str)) {
      warning("SKIP: folder name does not match expected ddd_ format: ", folder_name)
      skipped_folders_invalid_reference <- c(skipped_folders_invalid_reference, folder_name)
      next
    }

      # 轉成數值後 +1，取出對應的 product_line_id
    index <- as.integer(index_str)
    if (is.na(index) || index <= 0 || index > nrow(df_product_line)) {
      warning(
        "SKIP: folder index not found in df_product_line: ",
        folder_name,
        " (index=",
        index_str,
        ")"
      )
      skipped_folders_invalid_reference <- c(skipped_folders_invalid_reference, folder_name)
      next
    }

    product_line_id <- df_product_line[index, "product_line_id", drop = TRUE]
    if (is.na(product_line_id) || !nzchar(trimws(as.character(product_line_id)))) {
      warning("SKIP: missing/empty product_line_id mapping for folder: ", folder_name)
      skipped_folders_invalid_reference <- c(skipped_folders_invalid_reference, folder_name)
      next
    }
    
    message("\u25b6 \u8655\u7406\u8cc7\u6599\u593e：", folder_name)

    file_names <- list.files(folder, full.names = TRUE, recursive = FALSE, all.files = TRUE)
    file_names <- file_names[file.info(file_names)$isdir == FALSE]
    file_names <- file_names[ vapply(file_names, function(file_name) {
      ext <- tolower(tools::file_ext(file_name))
      ext == "" || ext %in% c("csv", "xlsx", "xls")
    }, logical(1))]
    file_names <- file_names[nchar(basename(file_names)) > 0]
    if (length(file_names) == 0L) {
      skipped_folders_no_supported_files <- c(skipped_folders_no_supported_files, folder_name)
      warning("SKIP: no supported competitor sales files in folder ", folder_name)
      next
    }

    rows_before_folder <- total_rows_written

    for (file_name in file_names) {
      if (!file.exists(file_name)) {
        warning("檔案不存在（雲端尚未同步？）：", file_name)
        next
      }

      asin <- tools::file_path_sans_ext(basename(file_name))

      peek <- suppressMessages(safe_read_file(file_name))
      if (is.null(peek)) {
        warning("檔案讀取失敗，已略過：", basename(file_name))
        next
      }
      first_row_all_na <- nrow(peek) > 0 && all(is.na(peek[1, ]))
      skip_rows <- ifelse(first_row_all_na, 1, 0)

      df <- suppressMessages(safe_read_file(file_name, skip = skip_rows))
      if (is.null(df)) {
        warning("檔案再次讀取失敗，已略過：", basename(file_name))
        next
      }

      schema <- resolve_import_schema(names(df))
      required_cols <- c("date", "sales")
      missing_cols <- required_cols[!required_cols %in% c(
        if (!is.na(schema$date_col)) "date",
        if (!is.na(schema$sales_col)) "sales"
      )]
      if (length(missing_cols)) {
        warning("缺少欄位：", paste(missing_cols, collapse = ", "),
                " ——跳過 ", basename(file_name), " (欄位 mapping 失敗)")
        next
      }

      df_prepared <- add_meta_cols(df, asin, product_line_id, schema)

      rows_written <- nrow(df_prepared)
      if (is.na(rows_written) || rows_written == 0L) {
        warning("空資料：", basename(file_name), " 處理後無可寫入筆數")
        next
      }

      # 檔內先移除重複鍵（避免同一檔內多筆同 asin/date/product_line）
      within_file_keys <- paste0(
        df_prepared$product_line_id,
        "|",
        df_prepared$asin,
        "|",
        as.character(df_prepared$date)
      )
      df_prepared <- df_prepared[!duplicated(within_file_keys), , drop = FALSE]
      within_file_keys <- within_file_keys[!duplicated(within_file_keys)]
      rows_written <- nrow(df_prepared)

      if (rows_written == 0L) {
        warning("空資料：", basename(file_name), " 僅有重複筆，已略過")
        next
      }

      file_keys <- within_file_keys
      duplicate_flags <- file_keys %in% seen_keys
      if (any(duplicate_flags)) {
        duplicate_count <- sum(duplicate_flags)
        if (duplicate_count > 0L) {
          warning(
            sprintf(
              "跳過重複鍵資料：%s 之 %d 筆",
              basename(file_name),
              duplicate_count
            )
          )
          total_rows_skipped_duplicate <- total_rows_skipped_duplicate + duplicate_count
        }
        df_prepared <- df_prepared[!duplicate_flags, , drop = FALSE]
      }

      if (nrow(df_prepared) == 0L) {
        warning("空資料：", basename(file_name), " 僅重複鍵，已略過")
        next
      }

      df_prepared <- df_prepared[!duplicated(file_keys[!duplicate_flags]), , drop = FALSE]
      rows_written <- nrow(df_prepared)

      total_files_processed <- total_files_processed + 1L
      dbWriteTable(db_connection, "df_amz_competitor_sales",
                   df_prepared, append = TRUE, row.names = FALSE)
      total_rows_written <- total_rows_written + rows_written
      new_keys <- file_keys[!duplicate_flags]
      seen_keys <- c(seen_keys, new_keys)
      message("　↳ 已寫入 ", rows_written, " 筆：", asin)
    }

    rows_written_in_folder <- total_rows_written - rows_before_folder
    if (rows_written_in_folder == 0L) {
      skipped_folders_no_rows <- c(skipped_folders_no_rows, folder_name)
    }

    message("✓ ", folder_name, " 處理完畢")
  }

  message("--------- IMPORT SUMMARY ---------")
  message("Checked folders: ", length(sub_folders))
  message("Imported files: ", total_files_processed)
  message("Imported rows: ", total_rows_written)
  message("Skipped duplicate rows: ", total_rows_skipped_duplicate)

  if (length(skipped_folders_no_supported_files) > 0L) {
    message("No supported files (skipped): ", paste(skipped_folders_no_supported_files, collapse = ", "))
  }
  if (length(skipped_folders_no_rows) > 0L) {
    message("Supported files with no import rows: ", paste(skipped_folders_no_rows, collapse = ", "))
  }
  if (length(skipped_folders_invalid_reference) > 0L) {
    message("Invalid or unmatched product-line folders (skipped): ", paste(skipped_folders_invalid_reference, collapse = ", "))
  }

  if (total_rows_written == 0L && total_files_processed == 0L && length(skipped_folders_no_supported_files) == length(sub_folders)) {
    stop("VALIDATE FAILED: no readable competitor sales files found under ", main_folder)
  }

  if (total_rows_written == 0L) {
    stop("VALIDATE FAILED: no competitor sales records were imported from ", main_folder)
  }

  message("--------- 資料表結構 ---------")
  print(dbGetQuery(db_connection, "PRAGMA table_info('df_amz_competitor_sales')"))
  message("--------- DB 使用量 ---------")
  print(dbGetQuery(db_connection, "PRAGMA database_size;"))

  invisible(list(
    total_rows_imported = total_rows_written,
    total_files_processed = total_files_processed,
    scanned_folders = length(sub_folders),
    skipped_folders_no_supported_files = skipped_folders_no_supported_files,
    skipped_folders_no_rows = skipped_folders_no_rows,
    skipped_folders_invalid_reference = skipped_folders_invalid_reference
  ))
}
