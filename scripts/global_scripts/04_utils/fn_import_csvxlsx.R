#' Recursively import CSV / Excel files and bind into a single tibble
#'
#' 從指定資料夾（含子資料夾）遞迴尋找 *.csv*、*.xls*、*.xlsx* 檔案，
#' 以 `read_csvxlsx()` 讀入後，新增欄位 `path` 紀錄來源路徑，
#' 最終將所有資料 **直向 bind_rows() 為一張 tibble**。
#' 會自動偵測重複列（以 *除 path 以外的所有欄* 定義），
#' 若發現重複會：
#'   1. `message()` 提示重複筆數；
#'   2. 自動移除重複只保留一筆。
#'
#' @param dir_path  要掃描的資料夾路徑。
#' @param pattern   檔案篩選正規式；預設 `\\.(csv|xlsx?)$`。
#' @param recursive 是否遞迴搜尋子目錄；預設 `TRUE`。
#' @param ...       其他參數傳遞給 `read_csvxlsx()`。
#'
#' @return 一張已去除重複列的 **tibble**（隱式回傳）。
#' @export
import_csvxlsx <- function(
  dir_path,
  pattern   = "\\.(csv|xlsx?)$",
  recursive = TRUE,
  ...
) {
  stopifnot(dir.exists(dir_path))

  # 1. 蒐集檔案 ---------------------------------------------------------------
  files <- list.files(
    path        = dir_path,
    pattern     = pattern,
    recursive   = recursive,
    full.names  = TRUE,
    ignore.case = TRUE
  )
  if (length(files) == 0) {
    stop("在「", dir_path, "」未找到符合格式的檔案。")
  }

  # 2. 讀檔並加 path ----------------------------------------------------------
  dfs <- purrr::map(files, function(f) {
    df <- read_csvxlsx(f, ...)
    tibble::as_tibble(df) |>
      dplyr::mutate(path = f)
  })

  # 3. bind_rows() -----------------------------------------------------------
  all_df <- dplyr::bind_rows(dfs)

  # 4. 檢查並移除重複 ---------------------------------------------------------
  key_cols <- setdiff(names(all_df), "path")
  dup_idx  <- duplicated(all_df[key_cols])
  
  if (any(dup_idx)) {
    # ── 4-1 計算重複筆數 ───────────────────────
    n_dup <- sum(dup_idx)
    
    # ── 4-2 找出哪些主鍵重複 ───────────────────
    dup_keys <- all_df[dup_idx, key_cols, drop = FALSE] |>
      dplyr::distinct()                     # 只留一次即可
    
    msg <- paste0(
      "⚠️  偵測到 ", n_dup, " 列重複（以除 `path` 外的欄位定義）\n",
      "    重複鍵值如下：\n",
      capture.output(print(dup_keys, row.names = FALSE)) |> paste(collapse = "\n")
    )
    message(msg)
    
    # ── 4-3 移除重複列（保留首見） ───────────────
    all_df <- all_df[!dup_idx, , drop = FALSE] %>% 
      janitor::clean_names(ascii = FALSE) %>% 
      remove_illegal_utf8()
  }

  all_df
}