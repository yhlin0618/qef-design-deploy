# tests/testthat/test-read_csvxlsx.R
library(testthat)
library(withr)

# 建立測試資料 ----
make_demo_df <- function() {
  data.frame(
    lg   = c(TRUE, FALSE, NA),
    int  = 1:3,
    dbl  = c(1.1, 2.2, 3.0),     # 第 3 筆應可被轉成 integer
    chr  = letters[1:3],
    fact = factor(c("a", "b", "c")),  # 不允許型別，應轉成 character
    stringsAsFactors = FALSE
  )
}

with_tempdir({
  df   <- make_demo_df()
  big  <- as.data.frame(matrix(1, nrow = 2e6, ncol = 3))  # >100 MB
  
  csv_small <- tempfile(fileext = ".csv")
  csv_big   <- tempfile(fileext = ".csv")
  xlsx_path <- tempfile(fileext = ".xlsx")
  
  readr::write_csv(df, csv_small)
  readr::write_csv(big, csv_big)
  openxlsx::write.xlsx(df, xlsx_path)
  
  ## ── 1. auto engine ───────────────────────────────────────────────
  test_that("auto engine 選擇正確", {
    skip_if_not_installed("vroom")
    skip_if_not_installed("data.table")
    skip_if_not_installed("openxlsx")
    
    expect_message(
      res1 <- read_csvxlsx(csv_small, engine = "auto"),
      "已完成"
    )
    expect_s3_class(res1, "tbl_df")
    
    # 大檔應選 fread
    res2 <- read_csvxlsx(csv_big, engine = "auto")
    expect_s3_class(res2, "tbl_df")
    expect_equal(nrow(res2), nrow(big))
  })
  
  ## ── 2. 指定各 engine ─────────────────────────────────────────────
  test_that("各 engine 正常讀取", {
    skip_if_not_installed("vroom")
    skip_if_not_installed("data.table")
    skip_if_not_installed("openxlsx")
    
    engs <- c("readr", "vroom", "fread")
    for (e in engs) {
      r <- read_csvxlsx(csv_small, engine = e)
      expect_equal(r$int, df$int)
    }
    r_xlsx <- read_csvxlsx(xlsx_path, engine = "openxlsx")
    expect_equal(r_xlsx$chr, df$chr)
  })
  
  ## ── 3. 型別轉換檢查 ───────────────────────────────────────────────
  test_that("double→integer 與非法型別→character", {
    r <- read_csvxlsx(csv_small, engine = "readr")
    expect_true(is.integer(r$int))
    expect_true(is.integer(r$dbl))  # 第 3 欄被轉成 integer
    expect_type(r$fact, "character")
  })
  
  ## ── 4. 錯誤處理 ───────────────────────────────────────────────────
  test_that("錯誤訊息正確", {
    tmp <- tempfile(fileext = ".txt")
    writeLines("x", tmp)
    expect_error(read_csvxlsx(tmp), "不支援的檔案格式")
    
    # vroom 讀 Excel 應報錯
    expect_error(read_csvxlsx(xlsx_path, engine = "vroom"), "vroom 只能讀 CSV")
  })
})