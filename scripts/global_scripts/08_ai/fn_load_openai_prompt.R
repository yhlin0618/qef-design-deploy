#' loadOpenAIPrompt  ----------------------------------------------------------
#' 載入集中式 OpenAI prompt 設定
#'
#' 從預先載入的 app_configs$ai_prompts 存取指定的 AI prompt 配置。
#' 支援 system_prompt 參照解析（例如：{system_prompts.product_strategist.content}）。
#' 支援巢狀路徑存取（例如："position_analysis.strategy_quadrant_analysis"）。
#'
#' **重要 (MP123: AI Prompt Configuration Management)**:
#' - 此函數從預先載入的 app_configs 讀取，不會重新讀取 YAML 檔案
#' - YAML 檔案在 initialization 階段已載入至 app_configs$ai_prompts
#' - 這確保配置只載入一次，提升效能並避免檔案路徑問題
#'
#' @param prompt_name Character. prompt 名稱，支援點分隔的巢狀路徑（如 "position_analysis.strategy_quadrant_analysis"）
#'
#' @return List 含 model, system_prompt, user_prompt_template
#' @export
#' @examples
#' # 載入策略四象限分析的 prompt (MP123: 從預載配置讀取)
#' prompt_config <- load_openai_prompt("position_analysis.strategy_quadrant_analysis")
#'
#' # 使用載入的 prompt 設定 (MP051: Explicit Parameter Specification)
#' messages <- list(
#'   list(role = "system", content = prompt_config$system_prompt),
#'   list(role = "user", content = gsub("{strategy_data}", strategy_txt,
#'                                     prompt_config$user_prompt_template))
#' )
load_openai_prompt <- function(prompt_name) {

  # ---- 從預載配置讀取 (MP123: AI Prompt Configuration Management) ----------
  # 檢查 app_configs 是否已載入 OpenAI prompts
  if (!exists("app_configs", envir = .GlobalEnv) ||
      is.null(app_configs$ai_prompts)) {
    stop(
      "app_configs$ai_prompts not initialized. ",
      "Please ensure initialization script has loaded the OpenAI prompt configuration. ",
      "See MP123: AI Prompt Configuration Management"
    )
  }

  prompts <- app_configs$ai_prompts

  # ---- 處理巢狀路徑存取 (MP051: Explicit Parameter Specification) ----------
  if (grepl("\\.", prompt_name)) {
    # 支援點分隔的巢狀路徑，例如："position_analysis.strategy_quadrant_analysis"
    path_parts <- strsplit(prompt_name, "\\.")[[1]]
    prompt_config <- prompts

    for (i in seq_along(path_parts)) {
      part <- path_parts[i]
      if (is.list(prompt_config) && part %in% names(prompt_config)) {
        prompt_config <- prompt_config[[part]]
      } else {
        # 提供清楚的錯誤訊息，顯示可用的選項
        if (i == 1) {
          available_options <- paste(names(prompts), collapse = ", ")
          stop("Top-level section '", part, "' not found. Available sections: ",
               available_options)
        } else {
          parent_path <- paste(path_parts[1:(i-1)], collapse = ".")
          available_options <- paste(names(prompt_config), collapse = ", ")
          stop("Key '", part, "' not found in '", parent_path,
               "'. Available options: ", available_options)
        }
      }
    }
  } else {
    # 簡單的頂層存取
    if (!prompt_name %in% names(prompts)) {
      available_prompts <- paste(names(prompts), collapse = ", ")
      stop("Prompt '", prompt_name, "' not found in YAML. Available prompts: ",
           available_prompts)
    }
    prompt_config <- prompts[[prompt_name]]
  }

  # ---- 解析 system_prompt 參照 (MP032: DRY - reuse common prompts) --------
  if (!is.null(prompt_config$system_prompt) &&
      grepl("\\{.*\\}", prompt_config$system_prompt)) {

    # 解析參照格式：{system_prompts.product_strategist.content}
    ref_pattern <- "\\{(.+)\\}"
    ref_match <- regmatches(prompt_config$system_prompt,
                           regexpr(ref_pattern, prompt_config$system_prompt))

    if (length(ref_match) > 0) {
      ref_path <- gsub("[{}]", "", ref_match)
      path_parts <- strsplit(ref_path, "\\.")[[1]]

      # 遞迴解析參照路徑 (MP031: Separation of Concerns)
      resolved_value <- prompts
      for (part in path_parts) {
        if (is.list(resolved_value) && part %in% names(resolved_value)) {
          resolved_value <- resolved_value[[part]]
        } else {
          warning("Cannot resolve system_prompt reference: ", ref_path)
          resolved_value <- prompt_config$system_prompt  # 保持原值
          break
        }
      }
      prompt_config$system_prompt <- resolved_value
    }
  }

  # ---- 驗證必要欄位 (MP051: Explicit Parameter Specification) -------------
  required_fields <- c("model", "system_prompt", "user_prompt_template")
  missing_fields <- setdiff(required_fields, names(prompt_config))

  if (length(missing_fields) > 0) {
    stop("Missing required fields in prompt '", prompt_name, "': ",
         paste(missing_fields, collapse = ", "))
  }

  return(prompt_config)
}