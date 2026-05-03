# 檢查是否已經初始化，避免重複載入
if (exists("INITIALIZATION_COMPLETED") && INITIALIZATION_COMPLETED) {
  message("Initialization already completed. Skipping.")
  return(invisible(NULL))
}

# 設定運行模式
OPERATION_MODE <- "GLOBAL_MODE"
message("Running in GLOBAL_MODE - Maintaining Shared Resources")

# 設定初始化標誌
INITIALIZATION_IN_PROGRESS <- TRUE

# 設定是否顯示詳細載入信息
if(!exists("VERBOSE_INITIALIZATION")) {
  VERBOSE_INITIALIZATION <- TRUE # Global 模式默認為詳細顯示
}

# 自定義 source_with_verbose 函數，可選擇是否顯示詳細載入信息
source_with_verbose <- function(file_path, verbose = VERBOSE_INITIALIZATION) {
  if(verbose) {
    message(paste("Sourcing file:", file_path))
  }

  source(file_path)
  if(verbose) message(paste("Successfully loaded:", file_path))
}

# 加載 library2 函數
source_with_verbose(file.path("scripts", "global_scripts", "04_utils", "library2.R"))
source_with_verbose(file.path("scripts", "global_scripts", "04_utils", "fn_initialize_packages.R"))

# Initialize all required packages
initialize_packages(
  mode = OPERATION_MODE,
  verbose = VERBOSE_INITIALIZATION,
  force_update = FALSE
)

#------------------------------
# 根路徑配置已移除（使用 autoinit() 處理）
# 原本的 fn_root_path_config.R 是殭屍程式碼，已刪除

# bslib functions
page_navbar <- bslib::page_navbar
bs_theme <- bslib::bs_theme
layout_column_wrap <- bslib::layout_column_wrap
layout_columns <- bslib::layout_columns
nav_panel <- bslib::nav_panel
card <- bslib::card
card_body <- bslib::card_body
value_box <- bslib::value_box
navbar_options <- bslib::navbar_options
bs_icon <- function(name) {
  shiny::icon(name)
}

message("版本控制與協作工具已載入，Global Mode 下允許雲端協作。")

##################################################
# 1. 載入本地腳本 (Local Scripts)
##################################################

# 載入品牌特定參數的腳本，該腳本存放於 "local_scripts" 資料夾中
if (file.exists(file.path("local_scripts", "brand_specific_parameters.R"))) {
  source_with_verbose(file.path("local_scripts", "brand_specific_parameters.R"))
}

##################################################
# 2. 載入全域腳本 (Global Scripts) - Global 模式下載入所有全域腳本與測試工具
##################################################

# 設定全域腳本所在的資料夾路徑 (存放於 "update_scripts/global_scripts")
Func_dir <- file.path("scripts", "global_scripts")

# 定義有序的目錄列表，按照載入順序排列 - Global 模式下載入所有目錄，包括測試目錄
ordered_directories <- c(
  "00_principles",      # 首先載入原則文檔
  "14_sql_utils",       # SQL 工具函數
  "02_db_utils",        # 資料庫連接工具
  "04_utils",           # 通用工具函數
  "03_config",          # 配置文件
  "01_db",              # 數據庫表定義
  "06_queries",         # 查詢功能
  "05_data_processing", # 數據處理功能
  "07_models",          # 模型
  "08_ai",              # AI 組件
  "09_python_scripts",  # Python 腳本接口
  "10_rshinyapp_components", # Shiny 應用組件
  "11_rshinyapp_utils", # Shiny 工具函數
  "12_bash",            # Bash 腳本工具
  "13_claude_prompts",  # Claude AI 提示
  "15_tests",           # 測試腳本 (Global 模式特有)
  "16_documentation",   # 文檔生成工具 (Global 模式特有)
  "17_ci_cd",           # 持續集成工具 (Global 模式特有)
  "18_benchmarks"       # 效能基準測試 (Global 模式特有)
)

# 跟踪已加載的文件，以避免重複加載
loaded_files <- c()

# We'll load fn_get_r_files_recursive.R early in the initialization process
# First, make sure the file exists
utils_path <- file.path("scripts", "global_scripts", "11_rshinyapp_utils", "fn_get_r_files_recursive.R")
if (file.exists(utils_path)) {
  source_with_verbose(utils_path)
} else {
  stop("fn_get_r_files_recursive.R not found at ", utils_path)
}

# 依序載入每個目錄中的腳本
for (dir_name in ordered_directories) {
  dir_path <- file.path(Func_dir, dir_name)

  # 檢查目錄是否存在
  if (!dir.exists(dir_path)) {
    stop("Required global script directory not found: ", dir_path)
  }

  # 獲取目錄及其子目錄中所有 .R 文件 - Global 模式下載入所有 R 文件
  r_files <- get_r_files_recursive(dir_path, pattern = "\\.R$")
  
  # 避開初始化和去初始化腳本，避免重複載入
  r_files <- r_files[!grepl("initialization|deinitialization", basename(r_files))]
  
  # 如果目錄不為空，輸出載入信息
  if (length(r_files) > 0) {
    message(paste("Loading", dir_name, "components..."))
    
    # 排序文件以確保按照名稱順序載入
    r_files <- sort(r_files)
    
    # 依序載入每個文件
    for (file in r_files) {
      # 只載入未載入過的文件
      if (!file %in% loaded_files) {
        source_with_verbose(file)
        loaded_files <- c(loaded_files, file)
      }
    }
  }
}

# 檢查根目錄中是否有 .R 文件需要載入
r_root_files <- dir(Func_dir, pattern = "\\.R$", full.names = TRUE)
if (length(r_root_files) > 0) {
  # 過濾掉初始化和反初始化文件
  r_root_files <- r_root_files[!grepl("^(sc_|fn_)", basename(r_root_files))]

  # 過濾掉已經載入的文件
  r_root_files <- r_root_files[!r_root_files %in% loaded_files]

  if (length(r_root_files) > 0) {
    message("Loading scripts from root directory...")
    for (file in r_root_files) {
      if (!file %in% loaded_files) {
        source_with_verbose(file)
        loaded_files <- c(loaded_files, file)
      }
    }
  }
}

# 載入測試框架配置 - Global 模式特有
message("Setting up testing framework...")
testthat::test_dir(file.path(Func_dir, "15_tests"), reporter = "summary")
message("Test framework initialized successfully.")

# 標記初始化完成
INITIALIZATION_COMPLETED <- TRUE

# 顯示載入統計
message(paste("GLOBAL_MODE initialization completed. Successfully loaded", 
             length(loaded_files), "files."))
message("NOTICE: All global_scripts have been loaded, including test and development tools.")

# 設置 Global 模式特有的工作環境
# 允許修改全局環境以便於共享資源開發和測試
locked_environment <- FALSE
allow_file_write <- TRUE
enable_testing <- TRUE
enable_benchmarking <- TRUE
enable_documentation <- TRUE

# 設置並行計算參數 - Global 模式下啟用多核心處理
future::plan(future::multisession, workers = parallel::detectCores() - 1)
message("Parallel processing enabled with ", parallel::detectCores() - 1, " worker cores.")

# 顯示 Global 模式啟動完成的消息
message("GLOBAL_MODE is ready. Shared resource development environment initialized.")
message("ATTENTION: This mode has full access to modify global_scripts. Use with caution.")
