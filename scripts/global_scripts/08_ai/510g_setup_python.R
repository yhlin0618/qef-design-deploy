setup_python <- function(import_str = "", env_name = "myenv") {
  library(reticulate)
  
  # 檢查是否有可用的 Python，如果沒有則安裝
  if (!py_available(initialize = FALSE)) {
    message("Python 不可用，開始安裝 Python ...")
    install_python()
  }
  
  # 若虛擬環境不存在，則建立虛擬環境
  if (!virtualenv_exists(env_name)) {
    message("虛擬環境 '", env_name, "' 不存在，正在建立...")
    virtualenv_create(env_name)
  }
  
  # 指定使用該虛擬環境，請盡量在任何可能初始化 Python 前就呼叫此函數
  use_virtualenv(env_name, required = TRUE)
  message("使用虛擬環境：", env_name)
  
  # 如果 import_str 為空，則直接返回空列表
  if (nchar(trimws(import_str)) == 0) {
    message("未提供 import 字串，沒有載入任何 Python 模組。")
    return(list())
  }
  
  # 將傳入的 import 字串按照換行拆分，每行去除前後空白，忽略空行及註解行
  lines <- unlist(strsplit(import_str, "\n"))
  lines <- trimws(lines)
  lines <- lines[lines != "" & !grepl("^#", lines)]
  
  imported <- list()
  
  # 處理每一行的 import 語句
  for (line in lines) {
    if (grepl("^import ", line)) {
      # 處理 "import 模組" 或 "import 模組 as 別名"
      line_trim <- sub("^import ", "", line)
      if (grepl(" as ", line_trim)) {
        parts <- unlist(strsplit(line_trim, " as "))
        module_name <- trimws(parts[1])
        alias <- trimws(parts[2])
      } else {
        module_name <- trimws(line_trim)
        alias <- module_name
      }
      # 檢查模組是否存在，若不存在則自動安裝
      if (!py_module_available(module_name)) {
        message("模組 '", module_name, "' 不存在，正在安裝...")
        py_install(module_name, envname = env_name)
      }
      message("Importing module '", module_name, "' as '", alias, "'.")
      imported[[alias]] <- import(module_name)
      
    } else if (grepl("^from ", line)) {
      # 處理 "from 模組 import 物件" 或 "from 模組 import 物件 as 別名"
      line_trim <- sub("^from ", "", line)
      parts <- unlist(strsplit(line_trim, " import "))
      if (length(parts) < 2) {
        warning("無法解析的語句：", line)
        next
      }
      module_name <- trimws(parts[1])
      imported_part <- trimws(parts[2])
      
      if (grepl(" as ", imported_part)) {
        parts2 <- unlist(strsplit(imported_part, " as "))
        object_name <- trimws(parts2[1])
        alias <- trimws(parts2[2])
      } else {
        object_name <- imported_part
        alias <- object_name
      }
      # 檢查模組是否存在，若不存在則自動安裝
      if (!py_module_available(module_name)) {
        message("模組 '", module_name, "' 不存在，正在安裝...")
        py_install(module_name, envname = env_name)
      }
      message("從模組 '", module_name, "' 中導入 '", object_name, "' 作為 '", alias, "'.")
      mod <- import(module_name, convert = FALSE)
      imported[[alias]] <- mod[[object_name]]
      
    } else {
      warning("無法解析的行：", line)
    }
  }
  
  message("已成功載入以下 Python 模組/物件：", paste(names(imported), collapse = ", "))
  return(imported)
}
