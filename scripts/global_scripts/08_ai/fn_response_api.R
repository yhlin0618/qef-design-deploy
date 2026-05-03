# LOCK FILE
#
# fn_response_api.R
#
# Following principles:
# - R21: One Function One File
# - R69: Function File Naming (fn_ prefix)
# - MP47: Functional Programming
# - MP81: Explicit Parameter Specification
# - MP123: AI Prompt Configuration Management
# - DEV_P021: Performance Acceleration (async patterns)
#
# Unified OpenAI API wrapper with built-in support for both Chat Completions and
# Responses endpoints.
# -----------------------------------------------------------------------------

.coalesce_null <- function(x, fallback) {
  if (is.null(x)) fallback else x
}

#' Generic OpenAI API caller (Chat Completions + Responses)
#'
#' Unified wrapper that dispatches to either the Responses API or Chat
#' Completions API based on model name or explicit `api_family` selection.
#'
#' @param input Character or list input for Responses API (or legacy fallback).
#' @param messages Backward-compatible role/message list for call sites using
#'   `list(list(role = "system", content = "..."), ...)`.
#' @param api_key Character. OpenAI API key (defaults to OPENAI_API_KEY env var).
#' @param model Character. Model to use.
#' @param api_family Character. API family selection mode.
#'   - `auto` (default): Responses API preferred for all models.
#'     Only legacy models (gpt-3.5*, gpt-4, gpt-4-*) fall
#'     back to Chat Completions.
#'   - `chat` / `chat_completions`: force Chat Completions API
#'   - `responses`: force Responses API
#' @param api_url Character. Deprecated. Kept for backward
#'   compatibility; ignored in auto dispatch.
#' @param chat_api_url Character. Chat Completions URL.
#' @param responses_api_url Character. Responses URL.
#'
#' @section Shared Parameters (both APIs):
#' @param instructions Character. Optional top-level instruction string.
#'   Responses API: maps to `instructions`. Chat Completions: prepended as
#'   system message.
#' @param tools List. Optional tool definitions (Responses/Chat where supported).
#' @param tool_choice List/Character. Tool invocation mode (e.g., "auto",
#'   "required", "none", or a specific function).
#' @param temperature Numeric. Sampling temperature (0-2). Higher = more random.
#' @param top_p Numeric. Nucleus sampling (0-1). Alternative to temperature.
#' @param max_output_tokens Numeric. Max tokens in response. Responses API uses
#'   this directly; Chat Completions maps to max_completion_tokens.
#' @param max_completion_tokens Numeric. Chat Completions token budget. If both
#'   this and max_output_tokens are set, this takes precedence for Chat.
#' @param response_format List/Character. Output format config. Use
#'   `list(type = "json_schema", json_schema = ...)` for structured outputs.
#' @param metadata List. Custom key-value pairs for tracking/filtering.
#' @param store Logical. Whether to store response on OpenAI side (default TRUE
#'   for Responses API, 30-day retention).
#' @param user Character. Caller/user identifier for abuse monitoring.
#' @param service_tier Character. Service tier for the request. Use "flex" for
#'   lower-priority, cost-optimized processing; "default" for standard.
#'
#' @section Responses API Only:
#' @param previous_response_id Character. Chain responses by referencing a prior
#'   response ID. All previous input tokens are re-billed.
#' @param conversation List. Persistent conversation config for durable
#'   multi-turn state management across sessions.
#' @param prompt List. Reusable prompt template reference with `id`, `version`,
#'   and `variables` fields.
#' @param reasoning List. Reasoning config for GPT-5 / o-series models.
#'   Use `list(effort = "low"|"medium"|"high")` to control thinking depth.
#'   Optional `summary` field controls reasoning summary visibility.
#' @param text List. Text rendering config.
#'   Use `list(format = list(type = "text"))` for plain text (default), or
#'   `list(format = list(type = "json_schema", ...))` for structured output.
#' @param truncation Character. Input truncation policy ("disabled" or "auto").
#' @param parallel_tool_calls Logical. Whether to allow parallel tool calls
#'   (default TRUE).
#' @param background Logical. Run as background task on OpenAI servers. Useful
#'   for long-running requests. Poll for completion via response ID.
#' @param max_tool_calls Numeric. Maximum number of tool calls allowed per
#'   response turn.
#' @param top_logprobs Numeric. Number of top log probabilities to return per
#'   output token (0-20).
#' @param tool_resources List. Tool resource config (e.g., vector store IDs for
#'   file_search).
#'
#' @section Chat Completions Only:
#' @param seed Numeric. Deterministic sampling seed for reproducibility.
#' @param stop Character or list. Up to 4 sequences where the API will stop
#'   generating further tokens.
#' @param n Numeric. Number of chat completion choices to generate (default 1).
#' @param presence_penalty Numeric. Penalize new tokens based on whether they
#'   appear in text so far (-2.0 to 2.0).
#' @param frequency_penalty Numeric. Penalize new tokens based on existing
#'   frequency in text so far (-2.0 to 2.0).
#' @param logprobs Logical. Whether to return log probabilities of output tokens.
#' @param top_logprobs_chat Numeric. Number of most likely tokens to return at
#'   each position (0-20). Requires `logprobs = TRUE`.
#' @param logit_bias List. Named list mapping token IDs to bias values (-100 to
#'   100). Modifies likelihood of specified tokens.
#'
#' @section Control Parameters:
#' @param timeout_sec Numeric. Request timeout seconds (default 300).
#' @param return_full Logical. Return full parsed response object instead of
#'   extracted text (default FALSE).
#' @param stream Logical. Enable SSE stream mode (default FALSE).
#' @param stream_file Character. Optional file path for stream text sink.
#' @param on_chunk Function. Optional `function(chunk_text)` callback in stream.
#' @param ... Named list of additional API parameters passed directly to the
#'   request body.
#'
#' @return Character response text (default) or full response object.
response_api <- function(input = NULL,
                         messages = NULL,
                         api_key = Sys.getenv("OPENAI_API_KEY"),
                         model = "gpt-5.2",  # Default fallback; callers should pass model from ai_prompts.yaml
                         api_family = c("auto", "chat", "chat_completions", "responses"),
                         api_url = "https://api.openai.com/v1/responses",
                         chat_api_url = "https://api.openai.com/v1/chat/completions",
                         responses_api_url = "https://api.openai.com/v1/responses",
                         # -- Shared parameters --
                         instructions = NULL,
                         tools = NULL,
                         tool_choice = NULL,
                         temperature = NULL,
                         top_p = NULL,
                         max_output_tokens = NULL,
                         max_completion_tokens = NULL,
                         response_format = NULL,
                         metadata = NULL,
                         store = NULL,
                         user = NULL,
                         service_tier = NULL,
                         # -- Responses API only --
                         previous_response_id = NULL,
                         conversation = NULL,
                         prompt = NULL,
                         reasoning = NULL,
                         text = NULL,
                         truncation = NULL,
                         parallel_tool_calls = NULL,
                         background = NULL,
                         max_tool_calls = NULL,
                         top_logprobs = NULL,
                         tool_resources = NULL,
                         # -- Chat Completions only --
                         seed = NULL,
                         stop = NULL,
                         n = NULL,
                         presence_penalty = NULL,
                         frequency_penalty = NULL,
                         logprobs = NULL,
                         top_logprobs_chat = NULL,
                         logit_bias = NULL,
                         # -- Control parameters --
                         timeout_sec = 300,
                         return_full = FALSE,
                         stream = FALSE,
                         stream_file = NULL,
                         on_chunk = NULL,
                         ...) {
  # Check required input
  if (is.null(input) && is.null(messages)) {
    stop("`input` or `messages` must be provided.")
  }

  # Check API key
  if (!nzchar(api_key)) {
    stop("🔑 OPENAI_API_KEY is missing. Set it in environment variables or pass it directly.")
  }

  if (!grepl("^sk-", api_key)) {
    warning("OpenAI API key format appears incorrect. Should start with 'sk-'")
  }

  # Check dependencies
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("Package 'httr2' is required. Please install it.")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Please install it.")
  }

  # Normalize helper
  api_family <- match.arg(api_family)

  api_url <- .resolve_api_url(
    api_family = api_family,
    model = model,
    chat_api_url = chat_api_url,
    responses_api_url = responses_api_url
  )
  use_responses <- grepl("responses", api_url)

  resolved_input <- .normalize_response_input(input, messages)

  # Build common request body
  drop_nulls <- function(x) {
    x[vapply(x, function(v) !is.null(v), logical(1), USE.NAMES = FALSE)]
  }

  body <- list(
    model = model,
    stream = stream
  )

  # Additional request params
  if (use_responses) {
    body <- c(
      body,
      list(
        input = resolved_input,
        instructions = instructions,
        tools = tools,
        tool_choice = tool_choice,
        tool_resources = tool_resources,
        previous_response_id = previous_response_id,
        conversation = conversation,
        prompt = prompt,
        max_output_tokens = max_output_tokens,
        truncation = truncation,
        temperature = temperature,
        top_p = top_p,
        reasoning = reasoning,
        text = text,
        response_format = response_format,
        metadata = metadata,
        store = store,
        user = user,
        service_tier = service_tier,
        parallel_tool_calls = parallel_tool_calls,
        background = background,
        max_tool_calls = max_tool_calls,
        top_logprobs = top_logprobs
      )
    )
  } else {
    chat_messages <- .normalize_chat_messages(input, messages)

    # Prepend instructions as system message (documented contract)
    if (!is.null(instructions)) {
      sys_msg <- list(list(role = "system",
                           content = instructions))
      chat_messages <- c(sys_msg, chat_messages)
    }

    max_completion_tokens <- if (
      !is.null(max_output_tokens) &&
      is.null(max_completion_tokens)
    ) {
      max_output_tokens
    } else {
      max_completion_tokens
    }

    body <- c(
      body,
      list(
        messages = chat_messages,
        temperature = temperature,
        top_p = top_p,
        max_completion_tokens = max_completion_tokens,
        response_format = response_format,
        tools = tools,
        tool_choice = tool_choice,
        user = user,
        service_tier = service_tier,
        seed = seed,
        stop = stop,
        n = n,
        presence_penalty = presence_penalty,
        frequency_penalty = frequency_penalty,
        logprobs = logprobs,
        top_logprobs = top_logprobs_chat,
        logit_bias = logit_bias,
        metadata = metadata,
        store = store
      )
    )
  }

  body <- drop_nulls(c(body, drop_nulls(as.list(list(...)))))

  if (isTRUE(stream)) {
    response_api_stream(
      input = resolved_input,
      api_key = api_key,
      model = model,
      api_url = api_url,
      api_family = if (use_responses) "responses" else "chat_completions",
      body = body,
      timeout_sec = timeout_sec,
      stream_file = stream_file,
      on_chunk = on_chunk
    )
  } else {
    req <- httr2::request(api_url) |>
      httr2::req_auth_bearer_token(api_key) |>
      httr2::req_headers(`Content-Type` = "application/json") |>
      httr2::req_body_json(body) |>
      httr2::req_timeout(timeout_sec)

    resp <- httr2::req_perform(req)

    if (httr2::resp_status(resp) >= 400) {
      err_text <- httr2::resp_body_string(resp)
      status_code <- httr2::resp_status(resp)
      err_msg <- err_text

      tryCatch({
        err_json <- jsonlite::fromJSON(err_text)
        if (!is.null(err_json$error$message)) {
          err_msg <- err_json$error$message
        }
      }, error = function(e) {
        # Keep raw body.
      })

      stop(sprintf("OpenAI %s API error %s for model '%s':\n%s",
                  if (use_responses) "Responses" else "Chat Completions",
                  status_code,
                  model,
                  err_msg))
    }

    content <- httr2::resp_body_json(resp)

    if (exists("update_token_usage", mode = "function", envir = .GlobalEnv)) {
      usage_info <- .extract_token_usage(content, use_responses)
      if (!is.null(usage_info)) {
        tryCatch({
          update_token_usage <- get("update_token_usage", envir = .GlobalEnv)
          update_token_usage(usage_info)
          message(sprintf(
            "📊 Token: +%d (in: %d, out: %d)",
            usage_info$total_tokens,
            usage_info$input_tokens,
            usage_info$output_tokens
          ))
        }, error = function(e) {
          message(sprintf("⚠️ Token tracking failed: %s", e$message))
        })
      }
    }

    if (isTRUE(return_full)) {
      return(content)
    }

    response_text <- if (use_responses) {
      .extract_response_api_text(content)
    } else {
      .extract_chat_completion_text(content)
    }

    return(trimws(response_text))
  }
}

#' OpenAI Chat Completions wrapper via response_api()
#'
#' @inheritParams response_api
#' @return Character response text (or full object when return_full = TRUE).
response_api_chat_completions <- function(input = NULL,
                                         messages = NULL,
                                         api_key = Sys.getenv("OPENAI_API_KEY"),
                                         model = "gpt-5.2",  # Default fallback; callers should pass model from ai_prompts.yaml
                                         api_url = "https://api.openai.com/v1/chat/completions",
                                         ...,
                                         timeout_sec = 300,
                                         return_full = FALSE,
                                         stream = FALSE,
                                         stream_file = NULL,
                                         on_chunk = NULL) {
  response_api(
    input = input,
    messages = messages,
    api_key = api_key,
    model = model,
    api_family = "chat",
    api_url = api_url,
    timeout_sec = timeout_sec,
    return_full = return_full,
    stream = stream,
    stream_file = stream_file,
    on_chunk = on_chunk,
    ...
  )
}

#' OpenAI Responses wrapper via response_api()
#'
#' @inheritParams response_api
#' @return Character response text (or full object when return_full = TRUE).
response_api_responses <- function(input = NULL,
                                   messages = NULL,
                                   api_key = Sys.getenv("OPENAI_API_KEY"),
                                   model = "gpt-5.2",  # Default fallback; callers should pass model from ai_prompts.yaml
                                   api_url = "https://api.openai.com/v1/responses",
                                   ...,
                                   timeout_sec = 300,
                                   return_full = FALSE,
                                   stream = FALSE,
                                   stream_file = NULL,
                                   on_chunk = NULL) {
  response_api(
    input = input,
    messages = messages,
    api_key = api_key,
    model = model,
    api_family = "responses",
    api_url = api_url,
    timeout_sec = timeout_sec,
    return_full = return_full,
    stream = stream,
    stream_file = stream_file,
    on_chunk = on_chunk,
    ...
  )
}

#' Stream OpenAI API calls (chat completions / responses)
#'
#' @param input Character or list. Inputs are forwarded as request body input.
#' @param api_key Character. OpenAI API key.
#' @param model Character. Model to use.
#' @param api_url Character. Endpoint URL.
#' @param api_family Character. `responses` or `chat_completions`.
#' @param body List. Request body already containing model and input/messages.
#' @param timeout_sec Numeric. Request timeout.
#' @param stream_file Character. Optional text sink path.
#' @param on_chunk Function. Optional callback for each text delta.
#' @return Character.
response_api_stream <- function(input,
                               api_key,
                               model,
                               api_url,
                               api_family,
                               body,
                               timeout_sec,
                               stream_file = NULL,
                               on_chunk = NULL) {
  if (!grepl("^gpt-5", model) && api_family == "responses") {
    message("⚠️ Streaming responses is commonly used with GPT-5 series.")
  }

  if (is.null(stream_file)) {
    stream_file <- tempfile(pattern = "openai_stream_", fileext = ".txt")
  }
  writeLines("", stream_file)

  if (api_family == "responses") {
    body$stream <- TRUE
  } else {
    body$stream <- TRUE
  }

  req <- httr2::request(api_url) |>
    httr2::req_auth_bearer_token(api_key) |>
    httr2::req_headers(`Content-Type` = "application/json") |>
    httr2::req_body_json(body) |>
    httr2::req_timeout(timeout_sec)

  accumulated_text <- ""

  stream_handler <- function(chunk) {
    lines <- strsplit(rawToChar(chunk), "\\n")[[1]]

    for (line in lines) {
      if (nchar(trimws(line)) == 0) next
      if (!grepl("^data:", line)) next

      data_json <- sub("^data:\\s*", "", line)
      if (identical(trimws(data_json), "[DONE]")) next

      tryCatch({
        event_data <- jsonlite::fromJSON(data_json)
        delta <- .extract_stream_delta(event_data, api_family)

        if (is.character(delta) && nchar(delta) > 0) {
          accumulated_text <<- paste0(accumulated_text, delta)
          write(accumulated_text, file = stream_file)

          if (!is.null(on_chunk)) {
            tryCatch(on_chunk(delta), error = function(e) {
              message("DEBUG: Stream callback failed: ", e$message)
            })
          }
        }
      }, error = function(e) {
        message("DEBUG: Stream JSON parse error: ", e$message)
      })
    }
  }

  tryCatch({
    httr2::req_perform_stream(req, callback = stream_handler, buffer_kb = 64)
    return(trimws(accumulated_text))
  }, error = function(e) {
    err_msg <- e$message
    if (!is.null(e$parent) && !is.null(e$parent$body)) {
      tryCatch({
        err_json <- jsonlite::fromJSON(rawToChar(e$parent$body))
        if (!is.null(err_json$error$message)) {
          err_msg <- err_json$error$message
        }
      }, error = function(e_parse) {
        # keep original
      })
    }

    stop(sprintf("OpenAI streaming API error for model '%s': %s", model, err_msg))
  })
}

#' Normalize chat-style messages into request-ready format.
.normalize_chat_messages <- function(input, messages = NULL) {
  if (!is.null(messages)) {
    if (!is.list(messages) || length(messages) == 0) {
      stop("`messages` must be a non-empty list of role/content message objects.")
    }

    if (!all(vapply(messages, function(msg) {
      is.list(msg) && !is.null(msg$role) && !is.null(msg$content)
    }, logical(1), USE.NAMES = FALSE))) {
      stop("`messages` must contain role/content message objects.")
    }

    return(messages)
  }

  if (is.character(input)) {
    return(list(list(role = "user", content = trimws(paste(input, collapse = "\n")))))
  }

  if (is.list(input)) {
    if (all(vapply(input, function(item) {
      is.list(item) && !is.null(item$role) && !is.null(item$content)
    }, logical(1), USE.NAMES = FALSE))) {
      return(input)
    }

    stop("`input` must be a character/string or role+content message list when `messages` is NULL.")
  }

  stop("`input` must be a character/string or role+content message list.")
}

# Normalize chat-style messages into a Responses-compatible input string.
.normalize_response_input <- function(input, messages = NULL) {
  if (!is.null(input)) {
    if (is.character(input)) {
      return(trimws(paste(input, collapse = "\n")))
    }

    if (is.list(input)) {
      if (all(vapply(input, function(item) {
        is.list(item) && !is.null(item$role) && !is.null(item$content)
      }, logical(1), USE.NAMES = FALSE))) {
        # preserve structure for Responses (works with many modern models)
        return(lapply(input, function(msg) {
          list(
            role = msg$role,
            content = as.character(msg$content)
          )
        }))
      }

      return(input)
    }

    stop("`input` must be a character/string or list for Responses API usage.")
  }

  if (!is.null(messages)) {
    if (!is.list(messages) || length(messages) == 0) {
      stop("`messages` must be a non-empty list of message objects.")
    }

    if (all(vapply(messages, function(msg) {
      is.list(msg) && !is.null(msg$role) && !is.null(msg$content)
    }, logical(1), USE.NAMES = FALSE))) {
      return(lapply(messages, function(msg) {
        list(role = msg$role, content = as.character(msg$content))
      }))
    }

    stop("`messages` must contain role/content message objects when `input` is NULL.")
  }

  stop("Either `input` or `messages` must be provided.")
}

# Resolve API URL from family/model.
.resolve_api_url <- function(api_family, model,
                              chat_api_url, responses_api_url) {
  # Explicit family override
  if (api_family == "responses") return(responses_api_url)
  if (api_family %in% c("chat", "chat_completions")) {
    return(chat_api_url)
  }

  # Auto mode: Responses API preferred (OpenAI recommended).
  # Only known legacy models that predate Responses API
  # fall back to Chat Completions.
  chat_only <- c(
    "^gpt-3\\.5",        # GPT-3.5 family
    "^gpt-4$",           # exact "gpt-4"
    "^gpt-4-"            # gpt-4-turbo, gpt-4-0613, etc.
  )
  is_chat_only <- any(vapply(
    chat_only,
    function(p) grepl(p, model),
    logical(1)
  ))
  if (is_chat_only) return(chat_api_url)

  # All other models → Responses API
  # gpt-4o*, gpt-4.1*, gpt-5*, o1*, o3*, o4*, etc.
  responses_api_url
}

# Extract incremental SSE text.
.extract_stream_delta <- function(event_data, api_family) {
  if (api_family == "responses") {
    if (!is.null(event_data$type)) {
      if (grepl("output_text\\.delta$", event_data$type) && !is.null(event_data$delta)) {
        if (is.character(event_data$delta)) {
          return(event_data$delta)
        }
      }
      if (grepl("text\\.delta$", event_data$type)) {
        if (!is.null(event_data$delta)) return(as.character(event_data$delta))
        if (!is.null(event_data$delta$text)) return(as.character(event_data$delta$text))
      }
    }
    return(NULL)
  }

  # chat completions stream delta
  if (!is.null(event_data$choices) && length(event_data$choices) > 0) {
    delta <- event_data$choices[[1]]$delta
    if (!is.null(delta$role)) {
      return(NULL)
    }
    if (!is.null(delta$content)) {
      return(as.character(delta$content))
    }
  }

  NULL
}

# Extract text from Responses API output schema.
.extract_response_api_text <- function(content) {
  if (!is.null(content$output) && length(content$output) > 0) {
    chunks <- character(0)
    for (item in content$output) {
      if (!is.list(item)) next

      if (!is.null(item$type) && item$type == "message") {
        item_content <- item$content
        if (!is.null(item_content)) {
          for (content_item in item_content) {
            if (is.list(content_item) && !is.null(content_item$text)) {
              chunks <- c(chunks, as.character(content_item$text))
            }
            if (is.list(content_item) && !is.null(content_item$description)) {
              chunks <- c(chunks, as.character(content_item$description))
            }
          }
        }
      }

      if (!is.null(item$role) && item$role == "assistant" && !is.null(item$refusal)) {
        chunks <- c(chunks, as.character(item$refusal))
      }
    }

    if (length(chunks) > 0) {
      return(paste(chunks, collapse = "\n"))
    }
  }

  if (!is.null(content$output_text) && length(content$output_text) > 0) {
    return(paste(content$output_text, collapse = "\n"))
  }

  if (!is.null(content$choices) && length(content$choices) > 0 &&
      !is.null(content$choices[[1]]$message$content)) {
    return(content$choices[[1]]$message$content)
  }

  if (!is.null(content$error$message)) {
    stop(sprintf("Responses API error: %s", content$error$message))
  }

  return("")
}

# Extract text from Chat Completions response.
.extract_chat_completion_text <- function(content) {
  if (!is.null(content$choices) && length(content$choices) > 0) {
    message_content <- content$choices[[1]]$message$content
    if (!is.null(message_content)) {
      return(message_content)
    }
  }

  if (!is.null(content$choices) && length(content$choices) > 0 &&
      !is.null(content$choices[[1]]$delta$content)) {
    return(content$choices[[1]]$delta$content)
  }

  if (!is.null(content$error$message)) {
    stop(sprintf("Chat Completions API error: %s", content$error$message))
  }

  ""
}

# Normalize usage object across families.
.extract_token_usage <- function(content, is_responses = TRUE) {
  if (!is.list(content) || is.null(content$usage)) {
    return(NULL)
  }

  if (is_responses) {
    return(list(
      input_tokens = .coalesce_null(content$usage$input_tokens, 0),
      output_tokens = .coalesce_null(content$usage$output_tokens, 0),
      total_tokens = .coalesce_null(content$usage$input_tokens, 0) +
        .coalesce_null(content$usage$output_tokens, 0),
      model = content$model
    ))
  }

  list(
    input_tokens = .coalesce_null(content$usage$prompt_tokens, 0),
    output_tokens = .coalesce_null(content$usage$completion_tokens, 0),
    total_tokens = .coalesce_null(content$usage$total_tokens, 0),
    model = content$model
  )
}
