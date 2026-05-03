#' Batch Rate Comments Using OpenAI Structured Outputs
#'
#' Evaluates one review against multiple properties in a single API call using
#' JSON Schema structured output. Automatically splits 5尺度 and 2尺度 properties
#' into separate calls with scale-specific prompts, then merges results.
#'
#' Compared to rate_comments() (1 call per property), this sends at most 2 calls
#' per review (one for Likert, one for binary), reducing API calls by ~N/2×.
#'
#' @param title Character. The review title.
#' @param body Character. The review body.
#' @param product_line_name Character. The product line name in English.
#' @param properties data.frame. Must contain columns: property_id, property_name, type, scale.
#' @param gpt_key Character. OpenAI API key.
#' @param model Character. Model to use (default: "gpt-5.4-mini").
#' @param reasoning_effort Character. "low", "medium", or "high" (default: "medium").
#'
#' @return data.frame with columns: property_id, score (integer or NA), reason (character).
#'   For 5尺度: score = 1-5 or NA (not applicable).
#'   For 2尺度: score = 0 or 1 (0 = not mentioned, 1 = mentioned).
#'
#' @export
rate_comments_batch <- function(title,
                                body,
                                product_line_name,
                                properties,
                                gpt_key = Sys.getenv("OPENAI_API_KEY"),
                                model = "gpt-5.4-mini",
                                reasoning_effort = "medium") {

  if (!requireNamespace("glue", quietly = TRUE)) library(glue)
  if (!requireNamespace("jsonlite", quietly = TRUE)) library(jsonlite)

  # Validate inputs
  required_cols <- c("property_id", "property_name", "type", "scale")
  missing <- setdiff(required_cols, names(properties))
  if (length(missing) > 0) {
    stop("properties data.frame missing columns: ", paste(missing, collapse = ", "))
  }

  if (nrow(properties) == 0) {
    return(data.frame(property_id = integer(0), score = integer(0),
                      reason = character(0), stringsAsFactors = FALSE))
  }

  # Build comment text
  has_title <- !is.null(title) && !is.na(title) && nchar(trimws(title)) > 0
  has_body <- !is.null(body) && !is.na(body) && nchar(trimws(body)) > 0

  comment_text <- if (has_title && has_body) {
    glue::glue("Title: '{title}'\nBody: '{body}'")
  } else if (has_title) {
    glue::glue("Comment: '{title}'")
  } else if (has_body) {
    glue::glue("Comment: '{body}'")
  } else {
    return(data.frame(
      property_id = properties$property_id,
      score = NA_integer_,
      reason = "No_comment_text",
      stringsAsFactors = FALSE
    ))
  }

  # Split by scale type
  props_likert <- properties[!identical_vec(properties$scale, "2尺度"), ]
  props_binary <- properties[identical_vec(properties$scale, "2尺度"), ]

  results <- list()

  # --- 5尺度 (Likert 1-5) ---
  if (nrow(props_likert) > 0) {
    results$likert <- .call_batch_api(
      comment_text = comment_text,
      product_line_name = product_line_name,
      props = props_likert,
      scale_mode = "likert",
      gpt_key = gpt_key,
      model = model,
      reasoning_effort = reasoning_effort
    )
  }

  # --- 2尺度 (Binary 0/1) ---
  if (nrow(props_binary) > 0) {
    results$binary <- .call_batch_api(
      comment_text = comment_text,
      product_line_name = product_line_name,
      props = props_binary,
      scale_mode = "binary",
      gpt_key = gpt_key,
      model = model,
      reasoning_effort = reasoning_effort
    )
  }

  do.call(rbind, results)
}


# --- Internal: vectorized identical check ---
identical_vec <- function(x, value) {
  vapply(x, function(v) identical(v, value), logical(1))
}


# --- Internal: single API call for one scale type ---
.call_batch_api <- function(comment_text,
                            product_line_name,
                            props,
                            scale_mode,
                            gpt_key,
                            model,
                            reasoning_effort) {

  n <- nrow(props)

  # Build statement list
  stmt_lines <- sprintf("%d. %s (type: %s)",
                        props$property_id, props$property_name, props$type)

  # Scale-specific prompt and schema
  if (scale_mode == "binary") {
    rules <- paste0(
      "Rules:\n",
      "- If the comment does NOT mention or imply the stated topic ",
      "in any way, set score to 0.\n",
      "- If the comment mentions or implies the stated topic ",
      "to any degree, set score to 1.\n",
      "- Always provide a brief reason."
    )
    score_schema <- list(
      type = "integer",
      description = "0 = not mentioned, 1 = mentioned"
    )
    schema_name <- "ewom_binary_ratings"
  } else {
    rules <- paste0(
      "Rules:\n",
      "- If the comment does NOT demonstrate the stated characteristic ",
      "in any way, set score to null.\n",
      "- If it does, rate your agreement from 1 to 5:\n",
      "  5 = Strongly Agree, 4 = Agree, 3 = Neutral, ",
      "2 = Disagree, 1 = Strongly Disagree\n",
      "- Always provide a brief reason."
    )
    score_schema <- list(
      type = I(c("integer", "null")),
      description = "1-5 Likert or null if not applicable"
    )
    schema_name <- "ewom_likert_ratings"
  }

  prompt <- glue::glue(
    "The following is a comment on a {product_line_name} product:\n",
    "{comment_text}\n\n",
    "For each statement below, evaluate whether it is supported by the comment.\n",
    "{rules}\n\n",
    "Statements:\n",
    "{paste(stmt_lines, collapse = '\n')}"
  )

  json_schema <- list(
    type = "json_schema",
    name = schema_name,
    strict = TRUE,
    schema = list(
      type = "object",
      properties = list(
        ratings = list(
          type = "array",
          items = list(
            type = "object",
            properties = list(
              id = list(type = "integer"),
              score = score_schema,
              reason = list(type = "string")
            ),
            required = I(c("id", "score", "reason")),
            additionalProperties = FALSE
          )
        )
      ),
      required = I(c("ratings")),
      additionalProperties = FALSE
    )
  )

  tryCatch({
    resp <- response_api(
      input = prompt,
      api_key = gpt_key,
      model = model,
      instructions = "Forget any previous information.",
      reasoning = list(effort = reasoning_effort),
      text = list(format = json_schema),
      temperature = NULL,
      timeout_sec = 600,
      return_full = FALSE
    )

    parsed <- jsonlite::fromJSON(resp, simplifyDataFrame = TRUE)

    if (!is.null(parsed$ratings) && nrow(parsed$ratings) > 0) {
      df <- parsed$ratings
      df$score <- as.integer(df$score)  # null → NA automatically
      df$id <- as.integer(df$id)
      names(df)[names(df) == "id"] <- "property_id"
      return(df)
    }

    warning("rate_comments_batch: no ratings in response for ", schema_name)
    data.frame(property_id = props$property_id, score = NA_integer_,
               reason = "parse_empty", stringsAsFactors = FALSE)

  }, error = function(e) {
    warning("rate_comments_batch API error (", schema_name, "): ", e$message)
    data.frame(property_id = props$property_id, score = NA_integer_,
               reason = paste0("API_error:", e$message), stringsAsFactors = FALSE)
  })
}
