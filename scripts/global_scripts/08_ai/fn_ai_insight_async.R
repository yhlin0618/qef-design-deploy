# =============================================================================
# fn_ai_insight_async.R — Reusable Non-Blocking AI Insight for Shiny Components
# Following: SO_R007, DEV_R001, GUIDE03, TD_P004 (ExtendedTask, not furrr)
#
# Provides three functions:
#   create_ai_insight_task()      — Creates an ExtendedTask for background AI calls
#   ai_insight_ui()               — Standard UI block (button + spinner + result card)
#   setup_ai_insight_server()     — Wires button → invoke → status → render
#
# Usage (in any component):
#   1. Add ai_insight_ui(ns, translate) to ui_display
#   2. In server: call create_ai_insight_task() and setup_ai_insight_server()
#
# Pattern extracted from positionMSPlotly.R ExtendedTask implementation.
# =============================================================================

#' Create an ExtendedTask for non-blocking AI insight generation
#'
#' Returns an ExtendedTask that runs chat_api() in a background future process.
#' Requires future::plan() to be set (e.g., future::plan(multisession, workers=1))
#' in the app initialization (union_production_test.R).
#'
#' @param gpt_key Character. OpenAI API key. If empty/NULL, returns NULL.
#' @return ExtendedTask object or NULL if no API key.
create_ai_insight_task <- function(gpt_key) {
  if (is.null(gpt_key) || !nzchar(gpt_key)) return(NULL)

  ExtendedTask$new(function(prompt_key, template_vars, api_key) {
    future::future({
      # --- Inside background process: must reload functions ---
      # load_openai_prompt requires app_configs in .GlobalEnv (set by autoinit)
      prompt_config <- load_openai_prompt(prompt_key)

      # Build user content from template + variables
      user_content <- prompt_config$user_prompt_template
      for (var_name in names(template_vars)) {
        user_content <- gsub(
          paste0("{", var_name, "}"),
          template_vars[[var_name]],
          user_content, fixed = TRUE
        )
      }

      sys_msg <- list(role = "system", content = prompt_config$system_prompt)
      usr_msg <- list(role = "user", content = user_content)

      chat_api(list(sys_msg, usr_msg), api_key, model = prompt_config$model)
    }, seed = TRUE)
  })
}


#' AI insight button — placed in ui_filter (left panel)
#'
#' @param ns Namespace function from the calling module.
#' @param translate Translation function.
#' @param button_label Character. Custom label for the button (optional).
#' @return tagList with actionButton.
ai_insight_button_ui <- function(ns, translate, button_label = NULL) {
  btn_label <- if (!is.null(button_label)) button_label else translate("Generate AI Insight")
  tagList(
    actionButton(ns("generate_ai_insight"), btn_label,
                 icon = icon("robot"), class = "btn-primary btn-block mb-2",
                 style = "width: 100%;")
  )
}


#' AI insight result — placed at bottom of ui_display (main panel)
#'
#' @param ns Namespace function from the calling module.
#' @param translate Translation function.
#' @return tagList with spinner and result card.
ai_insight_result_ui <- function(ns, translate) {
  tagList(
    # Spinner — hidden by default, shown while API call runs
    div(id = ns("ai_insight_spinner"), style = "display: none;",
        div(class = "text-center p-3",
            tags$i(class = "fas fa-spinner fa-spin fa-2x text-primary"),
            p(class = "mt-2 text-muted", translate("AI is analyzing your data...")))),
    # Result card — hidden by default, shown when result arrives
    div(id = ns("ai_insights_section"), style = "display: none;",
        div(class = "card",
            div(class = "card-header bg-primary text-white",
                h4(icon("brain"), translate("AI Data Insight"))),
            div(class = "card-body", uiOutput(ns("ai_insight_output")))))
  )
}


#' Standard UI block for AI insight (button + spinner + result card)
#' Backward-compatible wrapper — calls ai_insight_button_ui() + ai_insight_result_ui()
#'
#' @param ns Namespace function from the calling module.
#' @param translate Translation function.
#' @param button_label Character. Custom label for the button (optional).
#' @return tagList with actionButton, spinner, and result card.
ai_insight_ui <- function(ns, translate, button_label = NULL) {
  tagList(
    ai_insight_button_ui(ns, translate, button_label),
    ai_insight_result_ui(ns, translate)
  )
}


#' Wire an ExtendedTask AI insight into a Shiny module server
#'
#' Handles: button click → data summary → invoke task → spinner → result render.
#'
#' @param input,output,session Shiny module I/O.
#' @param ns Namespace function.
#' @param task ExtendedTask object from create_ai_insight_task(). May be NULL.
#' @param gpt_key Character. OpenAI API key.
#' @param prompt_key Character. Dot-path into ai_prompts.yaml.
#' @param get_template_vars Function. Called with no args, returns a named list of
#'   template variable strings. Should summarize data as text, NOT pass data frames.
#' @param component_label Character. For log messages, e.g. "customerValue".
setup_ai_insight_server <- function(input, output, session, ns,
                                     task, gpt_key, prompt_key,
                                     get_template_vars,
                                     component_label = "component") {

  # If no task (no API key), disable button and exit
  if (is.null(task)) {
    observe({
      shinyjs::disable(id = ns("generate_ai_insight"), asis = TRUE)
    })
    return(invisible(NULL))
  }

  ai_insight_result <- reactiveVal(NULL)

  # Button click → invoke ExtendedTask
  observeEvent(input$generate_ai_insight, {
    template_vars <- tryCatch(
      get_template_vars(),
      error = function(e) {
        message("[", component_label, "] Failed to build template vars: ", e$message)
        NULL
      }
    )

    # DEBUG_MODE: log template vars for E2E verification
    if (is_debug_mode() && !is.null(template_vars)) {
      debug_log(component_label, "Template vars built OK (",
                length(template_vars), " vars: ",
                paste(names(template_vars), collapse = ", "), ")")
      for (vn in names(template_vars)) {
        debug_log(component_label, "  ", vn, ": ",
                  substr(as.character(template_vars[[vn]]), 1, 300))
      }
    }

    if (is.null(template_vars)) {
      showNotification(
        paste0("Data not ready for AI analysis"),
        type = "warning", duration = 3, session = session
      )
      return()
    }

    # Show spinner, hide previous result
    # Use ns() + asis=TRUE to avoid namespace resolution issues in helper functions
    shinyjs::show(id = ns("ai_insight_spinner"), asis = TRUE)
    shinyjs::hide(id = ns("ai_insights_section"), asis = TRUE)

    message("[", component_label, "] AI insight requested — invoking ExtendedTask")
    task$invoke(prompt_key, template_vars, gpt_key)
  })

  # Monitor task status
  observe({
    status <- task$status()

    if (status == "success") {
      shinyjs::hide(id = ns("ai_insight_spinner"), asis = TRUE)

      tryCatch({
        result_text <- task$result()
        ai_insight_result(result_text)
        shinyjs::show(id = ns("ai_insights_section"), asis = TRUE)

        showNotification(
          paste0("AI insight generated"),
          type = "message", duration = 3, session = session
        )
        message("[", component_label, "] AI insight completed successfully")
        # DEBUG_MODE: log result preview for E2E verification
        if (is_debug_mode()) {
          debug_log(component_label, "AI result preview: ",
                    substr(as.character(result_text), 1, 500))
        }
      }, error = function(e) {
        message("[", component_label, "] Error reading task result: ", e$message)
        ai_insight_result(paste0("Error: ", e$message))
        shinyjs::show(id = ns("ai_insights_section"), asis = TRUE)
      })

    } else if (status == "error") {
      shinyjs::hide(id = ns("ai_insight_spinner"), asis = TRUE)

      showNotification(
        paste0("AI insight failed — please try again"),
        type = "error", duration = 5, session = session
      )
      message("[", component_label, "] AI insight task failed")
      ai_insight_result("Error generating AI insight. Please try again.")
      shinyjs::show(id = ns("ai_insights_section"), asis = TRUE)
    }
  })

  # Render result as markdown → HTML
  output$ai_insight_output <- renderUI({
    txt <- ai_insight_result()
    if (is.null(txt)) return(NULL)
    res <- strip_code_fence(txt)
    html_content <- markdown::markdownToHTML(text = res, fragment.only = TRUE)
    HTML(html_content)
  })
}
