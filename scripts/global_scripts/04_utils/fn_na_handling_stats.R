#' NA-Safe Statistical Functions
#'
#' A collection of wrapper functions around base R statistical functions
#' that set na.rm = TRUE by default for consistent handling of missing values.
#'
#' @examples
#' sum_na_rm(c(1, 2, NA, 4))  # Returns 7 instead of NA
#' mean_na_rm(c(1, NA, 3))    # Returns 2 instead of NA
#' prop_na(c(1, NA, 3, NA))   # Returns 0.5 (50% missing)

# 總和
sum_na_rm <- function(x, ...) sum(x, na.rm = TRUE, ...)

# 平均值
mean_na_rm <- function(x, ...) mean(x, na.rm = TRUE, ...)

# 標準差
sd_na_rm <- function(x, ...) sd(x, na.rm = TRUE, ...)

# 最大值
max_na_rm <- function(x, ...) max(x, na.rm = TRUE, ...)

# 最小值
min_na_rm <- function(x, ...) min(x, na.rm = TRUE, ...)

# 中位數
median_na_rm <- function(x, ...) median(x, na.rm = TRUE, ...)

# 加權平均（需傳入權重變數）
weighted_mean_na_rm <- function(x, w, ...) weighted.mean(x, w, na.rm = TRUE, ...)

# 非缺失值數量
count_non_na <- function(x) sum(!is.na(x))

# 缺失值數量
count_na <- function(x) sum(is.na(x))

# 缺失比例
prop_na <- function(x) mean(is.na(x))