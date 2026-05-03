#LOCK FILE
#
# fn_chat_api_stream.R
#
# Following principles:
# - R21: One Function One File
# - R69: Function File Naming (fn_ prefix)
# - MP099: Real-Time Progress Reporting
# - UI_R019: AI Process Notification Rule
# - MP123: AI Prompt Configuration Management
# - DEV_P021: Performance Acceleration (async patterns)
#
# Function to stream OpenAI Responses API with real-time text accumulation
# -----------------------------------------------------------------------------

#' Stream OpenAI Responses API (GPT-5) with Real-Time Updates
#'
#' This function enables streaming responses from GPT-5 models, allowing
#' real-time display of generated text as it arrives. Text is written to
#' a temporary file that can be read by Shiny's reactiveFileReader.
#'
#' **Architecture**: File-based intermediate storage
#' - Streaming runs in background (non-blocking)
#' - Chunks written to temp file
#' - Shiny UI polls file with reactiveFileReader
#' - Avoids main thread blocking
#'
#' **GPT-5 Streaming Format**:
#' - event: response.output_text.delta
#' - data: {"type":"response.output_text.delta","delta":"text chunk"}
#'
#' @param messages List. List of message objects with 'role' and 'content' fields.
#' @param api_key Character. OpenAI API key (defaults to OPENAI_API_KEY env var).
#' @param model Character. Model to use (must be gpt-5-*).
#' @param stream_file Character. Path to temporary file for streaming output.
#'   If NULL, a temp file will be created.
#' @param on_chunk Function. Optional callback function(chunk_text) called for each chunk.
#' @param timeout_sec Numeric. Request timeout in seconds (defaults to 300).
#' @return Character. The complete response text.
#'
#' @examples
#' # Basic streaming usage
#' messages <- list(
#'   list(role = "system", content = "You are a helpful assistant."),
#'   list(role = "user", content = "Write a short story.")
#' )
#'
#' stream_file <- tempfile(pattern = "gpt5_", fileext = ".txt")
#' result <- chat_api_stream(messages, stream_file = stream_file)
#' cat(result)
#' unlink(stream_file)
#'
#' # With callback for real-time monitoring
#' chat_api_stream(messages, on_chunk = function(chunk) {
#'   cat("Received:", chunk, "\n")
#' })
chat_api_stream <- function(messages,
                           api_key = Sys.getenv("OPENAI_API_KEY"),
                           model = "gpt-5.2",  # Default fallback; callers should pass model from ai_prompts.yaml
                           stream_file = NULL,
                           on_chunk = NULL,
                           timeout_sec = 300) {

  # Validate GPT-5 model (streaming only supported for GPT-5)
  if (!grepl("^gpt-5", model)) {
    stop("Streaming only supported for GPT-5 models (gpt-5-*). Use chat_api() for other models.")
  }

  # Check for API key
  if (!nzchar(api_key)) {
    stop("OPENAI_API_KEY is missing. Please set it in environment variables.")
  }

  # Validate API key format
  if (!grepl("^sk-", api_key)) {
    warning("OpenAI API key format appears incorrect. Should start with 'sk-'")
  }

  # Check required packages
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("Package 'httr2' is required for streaming. Please install it.")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Please install it.")
  }

  # Create temp file if not provided
  if (is.null(stream_file)) {
    stream_file <- tempfile(pattern = "gpt5_stream_", fileext = ".txt")
  }

  # Initialize stream file (empty)
  writeLines("", stream_file)

  # Prepare input for GPT-5 Responses API
  # Combine system and user messages into single input
  system_msg <- ""
  user_msg <- ""

  for (msg in messages) {
    if (msg$role == "system") {
      system_msg <- paste0(system_msg, msg$content, "\n\n")
    } else if (msg$role == "user") {
      user_msg <- paste0(user_msg, msg$content, "\n\n")
    }
  }

  full_input <- paste0(trimws(system_msg), "\n\n", trimws(user_msg))

  # Prepare request body (GPT-5 Responses API format)
  body <- list(
    model = model,
    input = trimws(full_input),
    reasoning = list(effort = "low"),  # Low reasoning for faster response
    text = list(verbosity = "medium"),
    max_output_tokens = 16000,  # Support long reports (10,000 words)
    stream = TRUE  # Enable streaming
  )

  # Prepare HTTP request
  req <- httr2::request("https://api.openai.com/v1/responses") |>
    httr2::req_auth_bearer_token(api_key) |>
    httr2::req_headers(`Content-Type` = "application/json") |>
    httr2::req_body_json(body) |>
    httr2::req_timeout(timeout_sec)

  # Accumulated text (updated as chunks arrive)
  accumulated_text <- ""

  # Stream handler function - processes each SSE chunk
  stream_handler <- function(chunk) {
    # Convert raw bytes to character string
    lines <- strsplit(rawToChar(chunk), "\n")[[1]]

    for (line in lines) {
      # Skip empty lines
      if (nchar(trimws(line)) == 0) next

      # Parse SSE data line
      if (grepl("^data:", line)) {
        data_json <- sub("^data:\\s*", "", line)

        # Skip [DONE] message
        if (data_json == "[DONE]") next

        # Parse JSON data
        tryCatch({
          event_data <- jsonlite::fromJSON(data_json)

          # Extract delta for output_text.delta events
          # GPT-5 format: {type: "response.output_text.delta", delta: "text"}
          if (!is.null(event_data$type) &&
              event_data$type == "response.output_text.delta" &&
              !is.null(event_data$delta)) {

            # Append to accumulated text
            accumulated_text <<- paste0(accumulated_text, event_data$delta)

            # Write complete text to stream file
            # (Shiny's reactiveFileReader will detect this change)
            write(accumulated_text, file = stream_file)

            # Call optional callback
            if (!is.null(on_chunk)) {
              tryCatch({
                on_chunk(event_data$delta)
              }, error = function(e) {
                message("DEBUG: Callback error: ", e$message)
              })
            }
          }

        }, error = function(e) {
          # Log parsing errors but continue streaming
          message("DEBUG: Error parsing SSE data: ", e$message)
          message("DEBUG: Problematic line: ", substr(line, 1, 200))
        })
      }
    }
  }

  # Perform streaming request
  tryCatch({
    httr2::req_perform_stream(
      req,
      callback = stream_handler,
      buffer_kb = 64  # Buffer size for streaming
    )

    # Return final accumulated text
    return(trimws(accumulated_text))

  }, error = function(e) {
    # Extract detailed error message
    err_msg <- e$message

    # Try to parse API error response
    if (!is.null(e$parent)) {
      tryCatch({
        err_json <- jsonlite::fromJSON(rawToChar(e$parent$body))
        if (!is.null(err_json$error$message)) {
          err_msg <- err_json$error$message
        }
      }, error = function(parse_err) {
        # Keep original error message
      })
    }

    stop(sprintf("Streaming API error for model '%s': %s", model, err_msg))
  })
}
