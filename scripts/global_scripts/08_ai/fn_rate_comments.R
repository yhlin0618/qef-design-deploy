#' Rate Comments Using OpenAI API
#'
#' Analyzes review text to extract sentiment by property as part of D03_07 (Rate Reviews) step.
#' Delegates API communication to response_api() for unified model/endpoint management.
#'
#' @param title Character. The review title (optional, can be NULL or NA).
#' @param body Character. The review content/body (optional, can be NULL or NA).
#' @param product_line_name Character. The product line name in English.
#' @param property_name Character. The property to evaluate.
#' @param type Character. The type of property (e.g., "屬性", "缺點", "場景").
#' @param scale Character. The scale type: "5尺度" for 1-5 Likert, "2尺度" for binary (default: "5尺度").
#' @param gpt_key Character. OpenAI API key.
#' @param model Character. The model to use (default: "gpt-5.4-mini").
#' @param reasoning_effort Character. Reasoning effort level: "low", "medium", "high" (default: "medium").
#'
#' @return Character. Response in the format "[Score, Reason]" or "[NaN,NaN]" if not applicable.
#'
#' @examples
#' \dontrun{
#' rate_comments(
#'   title = "Great taste",
#'   body = "This helps my digestion a lot.",
#'   product_line_name = "safety_glasses",
#'   property_name = "配戴舒適",
#'   type = "屬性",
#'   scale = "5尺度",
#'   gpt_key = Sys.getenv("OPENAI_API_KEY"),
#'   model = "gpt-5.4-mini"
#' )
#' }
#'
#' @export
rate_comments <- function(title,
                          body,
                          product_line_name,
                          property_name,
                          type,
                          scale = "5尺度",
                          gpt_key = Sys.getenv("OPENAI_API_KEY"),
                          model = "gpt-5.4-mini",
                          reasoning_effort = "medium") {

  if (!requireNamespace("glue", quietly = TRUE)) library(glue)

  # Handle optional title and body
  has_title <- !is.null(title) && !is.na(title) && nchar(trimws(title)) > 0
  has_body <- !is.null(body) && !is.na(body) && nchar(trimws(body)) > 0

  # Build comment text based on available fields
  comment_text <- if (has_title && has_body) {
    glue::glue("Title: '{title}'\nBody: '{body}'")
  } else if (has_title) {
    glue::glue("Comment: '{title}'")
  } else if (has_body) {
    glue::glue("Comment: '{body}'")
  } else {
    return("[NaN,No_comment_text]")
  }

  # Build scale-specific prompt
  double_check <- paste0(
    "** Please double check that 'If the comment does not ",
    "demonstrate the stated characteristic in any way, reply ",
    "exactly [NaN,NaN] without additional reasoning or explanation.'"
  )

  if (identical(scale, "2尺度")) {
    # Binary scale: present (1) or absent (0)
    scale_instruction <- paste0(
      "2. If the comment demonstrates the stated characteristic ",
      "to any degree, reply [1, Reason].\n",
      "If it does not, reply [0, Reason]."
    )
  } else {
    # 5-point Likert scale (default)
    scale_instruction <- paste0(
      "2. If the comment demonstrates the stated characteristic ",
      "to any degree:\n",
      "    Rate your agreement with the statement on a scale from 1 to 5:\n",
      "    - '5' for Strongly Agree\n",
      "    - '4' for Agree\n",
      "    - '3' for Neither Agree nor Disagree\n",
      "    - '2' for Disagree\n",
      "    - '1' for Strongly Disagree\n",
      "Provide your rationale in the format: [Score, Reason]."
    )
  }

  prompt <- glue::glue(
    "The following is a comment on a {product_line_name} product:\n",
    "{comment_text}\n",
    "Evaluate whether the statement '{property_name}' ",
    "(categorized as: {type}) is supported by the comment.\n\n",
    "Use the following rules to respond:\n",
    "1. If the comment does not demonstrate the stated characteristic ",
    "in any way, reply exactly [NaN,NaN] without additional reasoning ",
    "or explanation.\n",
    "{scale_instruction}\n",
    "{double_check}\n",
    "{double_check}\n",
    "{double_check}"
  )

  # Delegate to response_api() for unified API management
  tryCatch({
    resp <- response_api(
      input = prompt,
      api_key = gpt_key,
      model = model,
      instructions = "Forget any previous information.",
      reasoning = list(effort = reasoning_effort),
      temperature = NULL,
      return_full = FALSE
    )
    return(trimws(resp))
  }, error = function(e) {
    return(paste0("[NaN,API_error:", e$message, "]"))
  })
}