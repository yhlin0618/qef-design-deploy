#LOCK FILE
#
# positionMSPlotly.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP73: Interactive Visualization Preference (Plotly for interactive visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
# - MP47: Functional Programming (data transformation functions)
#

#
# Features:
#   • Competitive Set Analysis (CSA) with MDS visualization
#   • Interactive scatter plot with cluster grouping
#   • Brand differentiation with shapes and colors
#   • Hover information with brand and Product ID details
#   • Dynamic clustering and positioning analysis
#   • AI Market Segmentation Analysis Report generation
# -----------------------------------------------------------------------------

# Required packages for this component
if (!requireNamespace("later", quietly = TRUE)) {
  install.packages("later")
}
library(later)

# helper ----------------------------------------------------------------------
#' Paste operator for string concatenation
#' @param x Character string. First string to concatenate.
#' @param y Character string. Second string to concatenate.
#' @return Character string. The concatenated result of x and y.
`%+%` <- function(x, y) paste0(x, y)

#' Get language configuration and return appropriate language codes
#' @return list with language info for prompts
get_language_config <- function() {
  # Default to Traditional Chinese
  default_config <- list(
    code = "zh_TW",
    name = "Traditional Chinese",
    system_prompt = "You are a professional market research analyst. Please respond in Traditional Chinese.",
    ai_naming_instruction = "請用繁體中文命名市場區隔（每個名稱2-4個詞）",
    report_instruction = "請根據以下市場區隔表格數據提供完整的分析報告，使用 markdown 格式："
  )
  
  # Try to get language from app_configs
  if (exists("app_configs") && !is.null(app_configs$language)) {
    lang <- app_configs$language
    
    # Parse language settings
    if (grepl("zh_TW|zh-TW|zh_Hant", lang)) {
      return(list(
        code = "zh_TW",
        name = "Traditional Chinese",
        system_prompt = "You are a professional market research analyst. Please respond in Traditional Chinese.",
        ai_naming_instruction = "請用繁體中文命名市場區隔（每個名稱2-4個詞）",
        report_instruction = "請根據以下市場區隔表格數據提供完整的分析報告，使用 markdown 格式："
      ))
    } else if (grepl("zh_CN|zh-CN|zh_Hans", lang)) {
      return(list(
        code = "zh_CN", 
        name = "Simplified Chinese",
        system_prompt = "You are a professional market research analyst. Please respond in Simplified Chinese.",
        ai_naming_instruction = "请用简体中文命名市场细分（每个名称2-4个词）",
        report_instruction = "请根据以下市场细分表格数据提供完整的分析报告，使用 markdown 格式："
      ))
    } else if (grepl("en|EN", lang)) {
      return(list(
        code = "en",
        name = "English", 
        system_prompt = "You are a professional market research analyst. Please respond in English.",
        ai_naming_instruction = "Please name market segments in English (2-4 words each)",
        report_instruction = "Please provide a comprehensive analysis report based on the following market segmentation table data, using markdown format:"
      ))
    }
  }
  
  # Return default if no match or no config
  return(default_config)
}

#' NULL coalescing operator
#' @param x Any value. The value to use if not NULL.
#' @param y Any value. The fallback value to use if x is NULL.
#' @return Either x or y. Returns x if it's not NULL, otherwise returns y.
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Safe row count function
#' @param x Object. The object to count rows for.
#' @return Numeric. The number of rows in x, or 0 if x is not a data frame or has no rows.
nrow2 <- function(x) {
  if (is.null(x)) return(0)
  if (!is.data.frame(x) && !is.matrix(x)) return(0)
  return(nrow(x))
}

# Data transformation functions (MP47) ----------------------------------------
#' Perform CSA analysis following the original KitchenMAMA methodology
#' @param data data.frame. Position data with numerical attributes
#' @param exclude_vars character vector. Variables to exclude from analysis
#' @param na_threshold numeric. Maximum proportion of NA values allowed per column (default: 0.5)
#' @param use_data_modification logical. Whether to apply data modification (replace below-mean with mean)
#' @return list. Contains MDS results, clustering, and transformed data
perform_csa_analysis <- function(data, exclude_vars = NULL, na_threshold = 0.5, use_data_modification = TRUE) {
  # Filter out special rows first
  position_dta_no_na_token <- data %>%
    dplyr::filter(!product_id %in% c("Rating", "Revenue"))
  
  # Remove excluded variables if specified
  key_cols <- c("product_id", "brand", "product_line_id", "platform_id")
  
  # Create df_withoutname (similar to original)
  df_withoutname <- position_dta_no_na_token %>% 
    dplyr::select(-dplyr::any_of(c(exclude_vars, key_cols))) %>%
    dplyr::select_if(is.numeric)
  
  if (ncol(df_withoutname) == 0) {
    warning("No numeric columns found for CSA analysis")
    return(NULL)
  }
  
  # Filter out columns with too many NA values
  na_counts <- apply(df_withoutname, 2, function(x) sum(is.na(x)))
  na_props <- na_counts / nrow(df_withoutname)
  good_cols <- names(na_props[na_props <= na_threshold])
  
  if (length(good_cols) == 0) {
    warning("No columns with sufficient data for CSA analysis (all exceed NA threshold)")
    return(NULL)
  }
  
  # Keep only columns with acceptable NA levels
  df_withoutname <- df_withoutname %>% dplyr::select(dplyr::all_of(good_cols))
  
  # Remove rows with any remaining NA values
  complete_rows <- complete.cases(df_withoutname)
  
  # Check if we have valid complete rows
  if (sum(complete_rows) == 0) {
    warning("No complete rows after NA filtering")
    return(NULL)
  }
  
  df_withoutname <- df_withoutname[complete_rows, ]
  position_dta_no_na_token <- position_dta_no_na_token[complete_rows, ]
  
  if (nrow(df_withoutname) < 3) {
    warning("Insufficient complete rows for CSA analysis (need at least 3)")
    return(NULL)
  }
  
  tryCatch({
    # Step 1: Data modification (following original methodology)
    if (use_data_modification) {
      df_modified <- df_withoutname %>%
        dplyr::mutate(dplyr::across(
          .cols = dplyr::everything(),
          .fns = ~ {
            col_mean <- mean(., na.rm = TRUE)
            if (is.na(col_mean) || !is.finite(col_mean)) {
              # If mean is NA or infinite, return original values
              .
            } else {
              ifelse(. < col_mean, col_mean, .)
            }
          }
        ))
    } else {
      df_modified <- df_withoutname
    }
    
    # Additional check for any remaining NA or infinite values
    if (any(is.na(df_modified)) || any(!is.finite(as.matrix(df_modified)))) {
      warning("Data modification resulted in NA or infinite values, using original data")
      df_modified <- df_withoutname
    }
    
    # Step 2: Similarity matrix and clustering (following original methodology)
    tryCatch({
      similarity_matrix <- t(df_modified) %>%
        cor(use = "pairwise.complete.obs")
      
      # Check for valid correlation matrix
      if (any(is.na(similarity_matrix)) || any(is.infinite(similarity_matrix)) || 
          nrow(similarity_matrix) == 0 || ncol(similarity_matrix) == 0) {
        stop("Invalid similarity matrix")
      }
      
      # Pearson distance and hierarchical clustering
      Pearson_dist <- as.dist(1 - similarity_matrix)
      
      # Check if distance matrix is valid
      if (any(is.na(Pearson_dist)) || any(is.infinite(Pearson_dist)) || length(Pearson_dist) == 0) {
        stop("Invalid distance matrix")
      }
      
      hc_eu <- hclust(Pearson_dist, method = "complete")
      h <- (min(Pearson_dist, na.rm = TRUE) + max(Pearson_dist, na.rm = TRUE)) / 2
      
      if (is.na(h) || !is.finite(h)) {
        stop("Invalid clustering height")
      }
      
      clusterCut <- cutree(hc_eu, h = h)
      
      # Check if clustering was successful
      if (any(is.na(clusterCut)) || length(clusterCut) != nrow(df_withoutname)) {
        stop("Invalid clustering result")
      }
      
    }, error = function(e) {
      warning("Hierarchical clustering failed (", e$message, "), using k-means fallback")
      # Fallback to simple k-means clustering
      tryCatch({
        cluster_result <- kmeans(df_withoutname, centers = min(3, nrow(df_withoutname)-1), nstart = 20)
        clusterCut <- cluster_result$cluster
      }, error = function(e2) {
        warning("K-means clustering also failed, using single cluster")
        clusterCut <- rep(1, nrow(df_withoutname))
      })
    })
    
    # Step 3: MDS on original data (following original methodology)
    d <- dist(df_withoutname)  # euclidean distances between the rows
    
    # Check if distance matrix is valid
    if (any(is.na(d)) || any(is.infinite(d)) || length(d) == 0) {
      stop("Invalid distance matrix for MDS")
    }
    
    fit <- MASS::isoMDS(d, k = 2, trace = FALSE)  # k is the number of dim
    
    # Check if MDS was successful
    if (is.null(fit$points) || any(is.na(fit$points)) || any(is.infinite(fit$points))) {
      stop("MDS analysis failed or produced invalid results")
    }
    
    # Return comprehensive results
    list(
      points = fit$points,
      stress = fit$stress,
      clusters = clusterCut,
      complete_rows = complete_rows,
      variables_used = good_cols,
      n_observations = nrow(df_withoutname),
      n_variables = ncol(df_withoutname),
      original_data = position_dta_no_na_token,
      df_withoutname = df_withoutname,
      similarity_matrix = if (exists("similarity_matrix")) similarity_matrix else NULL
    )
  }, error = function(e) {
    warning("CSA analysis failed: ", e$message)
    NULL
  })
}

#' Create CSA data following original methodology
#' @param csa_result list. Results from CSA analysis
#' @param brand_highlight character. Brand name to highlight with stars
#' @return data.frame. CSA data with MDS coordinates and clusters
create_csa_data <- function(csa_result, brand_highlight = NULL) {
  if (is.null(csa_result) || is.null(csa_result$original_data)) return(data.frame())
  
  # Start with the original filtered data
  CSA <- csa_result$original_data
  
  if (nrow(CSA) != nrow(csa_result$points)) {
    warning("Mismatch between MDS points and data rows")
    return(data.frame())
  }
  
  # Add MDS coordinates (following original methodology)
  CSA$xMDS <- csa_result$points[, 1]
  CSA$yMDS <- csa_result$points[, 2]
  
  # Add cluster groups (from hierarchical clustering)
  CSA$group <- as.factor(csa_result$clusters)
  
  # Add shapes for brand highlighting (following original methodology)
  CSA$shapes <- "circle"
  if (!is.null(brand_highlight) && nchar(brand_highlight) > 0 && "brand" %in% colnames(CSA)) {
    # Safely check for brand matches
    brand_matches <- tryCatch({
      stringr::str_detect(CSA$brand, brand_highlight)
    }, error = function(e) {
      warning("Error in brand highlighting: ", e$message)
      rep(FALSE, nrow(CSA))
    })
    
    # Only apply highlighting if we have valid matches
    if (length(brand_matches) == nrow(CSA) && any(!is.na(brand_matches))) {
      CSA$shapes[brand_matches] <- "star"
    }
  }
  
  # Group by cluster (following original methodology)
  tryCatch({
    CSA <- CSA %>% dplyr::group_by(group)
  }, error = function(e) {
    warning("Error in grouping by cluster: ", e$message)
    # Return ungrouped data if grouping fails
  })
  
  return(CSA)
}

#' Analyze clusters to identify significant variables
#' @param csa_result list. Results from CSA analysis
#' @param position_data data.frame. Original position data
#' @param significance_threshold numeric. P-value threshold for significance (default: 0.05)
#' @return data.frame. Cluster analysis results
analyze_clusters <- function(csa_result, position_data, significance_threshold = 0.05) {
  if (is.null(csa_result) || is.null(csa_result$clusters)) return(data.frame())
  
  # Get numeric variables used in analysis
  numeric_vars <- csa_result$variables_used
  if (length(numeric_vars) == 0) return(data.frame())
  
  # Add cluster assignments to position data
  cluster_data <- position_data[csa_result$complete_rows, ]
  cluster_data$cluster <- as.factor(csa_result$clusters)
  
  # Calculate cluster statistics
  # Check if revenue column exists, otherwise use sales or a placeholder
  revenue_col <- if ("revenue" %in% names(cluster_data)) {
    "revenue"
  } else if ("sales" %in% names(cluster_data)) {
    "sales"
  } else if ("Sales" %in% names(cluster_data)) {
    "Sales"
  } else {
    warning("No revenue or sales column found in data")
    NULL
  }
  
  if (!is.null(revenue_col)) {
    cluster_stats <- cluster_data %>%
      dplyr::group_by(cluster) %>%
      dplyr::summarise(
        n_companies = n(),
        total_revenue = sum(.data[[revenue_col]], na.rm = TRUE),
        avg_revenue = mean(.data[[revenue_col]], na.rm = TRUE),
        .groups = 'drop'
      )
  } else {
    # Create basic stats without revenue
    cluster_stats <- cluster_data %>%
      dplyr::group_by(cluster) %>%
      dplyr::summarise(
        n_companies = n(),
        total_revenue = 0,
        avg_revenue = 0,
        .groups = 'drop'
      )
  }
  
  # Identify significant variables for each cluster
  significant_vars <- list()
  
  for (cluster_id in unique(cluster_data$cluster)) {
    cluster_vars <- c()
    
    for (var in numeric_vars) {
      if (var %in% names(cluster_data)) {
        # Perform t-test comparing cluster to others
        cluster_vals <- cluster_data[[var]][cluster_data$cluster == cluster_id]
        other_vals <- cluster_data[[var]][cluster_data$cluster != cluster_id]
        
        # Skip if not enough data
        if (length(cluster_vals) < 2 || length(other_vals) < 2) next
        if (all(is.na(cluster_vals)) || all(is.na(other_vals))) next
        
        tryCatch({
          t_result <- t.test(cluster_vals, other_vals, na.rm = TRUE)
          
          if (t_result$p.value < significance_threshold) {
            # Calculate effect size (Cohen's d)
            cluster_mean <- mean(cluster_vals, na.rm = TRUE)
            other_mean <- mean(other_vals, na.rm = TRUE)
            pooled_sd <- sqrt(((length(cluster_vals)-1)*var(cluster_vals, na.rm = TRUE) + 
                              (length(other_vals)-1)*var(other_vals, na.rm = TRUE)) / 
                             (length(cluster_vals) + length(other_vals) - 2))
            
            if (pooled_sd > 0) {
              cohens_d <- abs(cluster_mean - other_mean) / pooled_sd
              
              # Only include if effect size is meaningful (>0.5) AND cluster mean is higher
              if (cohens_d > 0.5 && cluster_mean > other_mean) {
                cluster_vars <- c(cluster_vars, var)
              }
            }
          }
        }, error = function(e) {
          # Skip variables that cause errors
        })
      }
    }
    
    significant_vars[[as.character(cluster_id)]] <- cluster_vars
  }
  
  # Combine results
  cluster_stats$significant_vars <- sapply(as.character(cluster_stats$cluster), function(x) {
    vars <- significant_vars[[x]]
    if (length(vars) > 0) paste(vars, collapse = ", ") else "No distinctive strengths"
  })
  
  return(cluster_stats)
}

#' Generate cluster names using GPT API
#' @param cluster_stats data.frame. Cluster statistics with significant variables
#' @param gpt_key Character. OpenAI API key
#' @param model Character. Model to use (default: "gpt-5.2")
#' @return Character vector. Generated cluster names
generate_cluster_names <- function(cluster_stats, gpt_key, model = "gpt-5.2") {  # Default; callers should pass model from ai_prompts.yaml
  if (is.null(cluster_stats) || nrow(cluster_stats) == 0) return(character(0))
  
  # Load required packages
  if (!requireNamespace("httr", quietly = TRUE)) {
    warning("httr package not available, using default cluster names")
    return(paste("Segment", cluster_stats$cluster))
  }
  
  # Get language configuration
  lang_config <- get_language_config()
  
  # Create a summary table for all clusters
  cluster_summary <- paste(
    lapply(1:nrow(cluster_stats), function(i) {
      paste0(
        "Segment ", cluster_stats$cluster[i], ": ",
        cluster_stats$significant_vars[i]
      )
    }),
    collapse = "\n"
  )

  # Load centralized prompt configuration (MP123: AI Prompt Configuration Management)
  prompt_config <- load_openai_prompt("position_analysis.csa_segment_naming")

  # Replace user template variables only (system_prompt already resolved by load_openai_prompt)
  user_content <- prompt_config$user_prompt_template
  user_content <- gsub("{cluster_summary}", cluster_summary, user_content, fixed = TRUE)

  tryCatch({
    # First, let's see what we're sending
    message("Sending request for ", nrow(cluster_stats), " clusters")
    message("First 200 chars of prompt: ", substr(user_content, 1, 200), "...")

    # Use chat_api for GPT-5 compatibility (MP123: AI Prompt Configuration Management)
    sys <- list(role = "system", content = prompt_config$system_prompt)
    usr <- list(role = "user", content = user_content)

    response_text <- chat_api(list(sys, usr), gpt_key, model = prompt_config$model)

    # Check for NULL or empty content
    if (is.null(response_text) || response_text == "") {
      message("ERROR: Response content is NULL or empty")
      return(paste("Segment", cluster_stats$cluster))
    }

    # Log the actual GPT response for debugging
    message("GPT response: '", response_text, "'")
    message("Response length: ", nchar(response_text))
      
    # Try to parse the R vector format
    # Clean up common issues
    # Remove markdown code blocks if present
    response_text <- gsub("```r?\\n?", "", response_text)
    response_text <- gsub("\\n```", "", response_text)
    response_text <- trimws(response_text)
    
    # Check if it starts with c(
    if (grepl("^c\\(", response_text)) {
      # Evaluate the R expression safely
      parsed_names <- eval(parse(text = response_text))
      
      # Ensure we have the right number of names
      if (length(parsed_names) == nrow(cluster_stats)) {
        message("Successfully parsed ", length(parsed_names), " cluster names")
        return(parsed_names)
      } else {
        message("ERROR: GPT returned ", length(parsed_names), " names but expected ", nrow(cluster_stats))
        message("Parsed names: ", paste(parsed_names, collapse = ", "))
        return(paste("Segment", cluster_stats$cluster))
      }
    } else {
      message("ERROR: GPT response does not start with 'c('. Response: ", response_text)
      # Try to extract if it's just the content without c()
      if (grepl("\".*\"", response_text)) {
        message("Attempting to extract quoted strings...")
        # Extract all quoted strings
        matches <- regmatches(response_text, gregexpr("\"[^\"]+\"", response_text))[[1]]
        if (length(matches) == nrow(cluster_stats)) {
          # Remove quotes and return
          cleaned_names <- gsub("\"", "", matches)
          message("Extracted names: ", paste(cleaned_names, collapse = ", "))
          return(cleaned_names)
        }
      }
      return(paste("Segment", cluster_stats$cluster))
    }
  }, error = function(e) {
    warning("Error generating cluster names: ", e$message)
    return(paste("Segment", cluster_stats$cluster))
  })
}


# Filter UI -------------------------------------------------------------------
#' positionMSPlotlyFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the CSA plotly component.
positionMSPlotlyFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    
    # Hide unnecessary filters with CSS
    tags$style(HTML(paste0("
      #", ns("competitor_filters"), " { display: none; }
      #", ns("analysis_options"), " { display: none; }
      #", ns("display_options"), " { display: none; }
    "))),
    
    # Hidden filters (keep for functionality but hide visually)
    div(id = ns("competitor_filters"),
        h4(translate("Competitor Analysis Filters")),
        
        # NA threshold for variable selection
        sliderInput(
          inputId = ns("na_threshold"),
          label = translate("Data Quality Threshold (Max % NA)"),
          min = 0.1,
          max = 0.9,
          value = 0.5,
          step = 0.1
        )
    ),
    
    # Analysis options (hidden)
    div(id = ns("analysis_options"),
        hr(),
        h4(translate("Analysis Options")),
        
        # Data modification option (following original methodology)
        checkboxInput(
          inputId = ns("use_data_modification"),
          label = translate("Apply data modification (replace below-mean with mean)"),
          value = TRUE
        )
    ),
    
    # Display options (hidden)
    div(id = ns("display_options"),
        hr(),
        h4(translate("Display Options")),
        
        # Show cluster centers
        checkboxInput(
          inputId = ns("show_centers"),
          label = translate("Show cluster centers"),
          value = FALSE
        ),
        
        # Marker size
        sliderInput(
          inputId = ns("marker_size"),
          label = translate("Marker Size"),
          min = 5,
          max = 20,
          value = 10,
          step = 1
        ),
        
        # Reset button
        actionButton(
          inputId = ns("reset_filters"),
          label = translate("Reset Filters"),
          class = "btn-outline-secondary btn-block mt-3"
        )
    ),
    
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' positionMSPlotlyDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @return shiny.tag. A Shiny UI component containing the display elements for the CSA plotly.
positionMSPlotlyDisplayUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    div(class = "component-header mb-3 text-center",
        h3(translate("Visualization of Competitive Positioning"))),
    div(class = "component-output p-3",
        plotlyOutput(ns("csa_plot"), width = "100%", height = "500px")),
    
    # Cluster analysis table with manual spinner control
    div(class = "mt-4",
        div(id = ns("spinner_overlay"), 
            class = "text-center", 
            style = "display: none; padding: 20px;",
            tags$div(
              tags$i(class = "fa fa-spinner fa-spin fa-2x", style = "color: #0d6efd;"),
              tags$p("AI naming in progress...", style = "margin-top: 10px;")
            )
        ),
        DTOutput(ns("cluster_table"), width = "100%")),
    
    # AI Market Segmentation Analysis Report
    div(class = "mt-4",
        h4(translate("AI Market Segmentation Analysis Report"), class = "text-center mb-3"),
        div(class = "card",
            div(class = "card-body",
                div(id = ns("ms_analysis_content"),
                    style = "min-height: 300px;",
                    if (requireNamespace("shinycssloaders", quietly = TRUE)) {
                      shinycssloaders::withSpinner(
                        htmlOutput(ns("ms_analysis_output")),
                        type = 6,
                        color = "#0d6efd"
                      )
                    } else {
                      htmlOutput(ns("ms_analysis_output"))
                    }
                )
            )
        )
    )
  )
}

# Server ----------------------------------------------------------------------
#' positionMSPlotlyServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data.
positionMSPlotlyServer <- function(id, app_data_connection = NULL, config = NULL,
                                    session = getDefaultReactiveDomain(),
                                    active_tab = NULL) {  # Add active_tab parameter
  moduleServer(id, function(input, output, session) {
    
    # ------------ Setup namespaced IDs (outside reactive context) ---
    ns <- session$ns
    cluster_table_id <- ns("cluster_table")
    
    # ------------ Check if component is active -------------------------
    is_component_active <- reactive({
      if (is.null(active_tab)) return(TRUE)  # Always active if no tab specified
      
      current_tab <- if (is.reactive(active_tab)) active_tab() else active_tab
      # Component is active when positionMS tab is selected
      identical(current_tab, "positionMS")
    })
    
    # ------------ Get OpenAI API key from environment --------------
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
    
    if (nzchar(gpt_key)) {
      # Basic validation of API key format
      if (!grepl("^sk-", gpt_key)) {
        warning("OpenAI API key format appears incorrect. Should start with 'sk-'")
      }
    } else {
      warning("OpenAI API key not found in environment. AI analysis features will be disabled.")
      gpt_key <- NULL
    }
    
    # ------------ Status tracking ----------------------------------
    component_status <- reactiveVal("idle")
    
    # ------------ Extract configuration parameters -----------------
    platform_id <- reactive({
      tryCatch({
        if (is.null(config)) return(NULL)
        
        cfg <- if (is.function(config)) config() else config
        
        if (!is.null(cfg[["platform_id"]])) {
          return(as.character(cfg[["platform_id"]]))
        }
        if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
          return(as.character(cfg[["filters"]][["platform_id"]]))
        }
        
        NULL
      }, error = function(e) {
        warning("Error extracting platform_id from config: ", e$message)
        NULL
      })
    })
    
    # Get brand name from app_config
    brand_highlight <- reactive({
      # Try to get from app_configs first (global variable)
      if (exists("app_configs") && !is.null(app_configs$brand_name)) {
        return(app_configs$brand_name)
      }
      # Otherwise return NULL
      return(NULL)
    })
    
    product_line_id <- reactive({
      tryCatch({
        if (is.null(config)) return("all")
        
        cfg <- if (is.function(config)) config() else config
        
        if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["product_line_id_chosen"]])) {
          return(as.character(cfg[["filters"]][["product_line_id_chosen"]]))
        }
        
        "all"
      }, error = function(e) {
        warning("Error extracting product_line_id: ", e$message)
        "all"
      })
    })
    
    # ------------ Data access (R116) -----------------------------------
    position_data <- reactive({
      if (product_line_id() == "all") {
        component_status("idle")
        return(data.frame())
      }
      
      component_status("loading")
      
      prod_line <- product_line_id()
      
      result <- tryCatch({
        if (is.null(app_data_connection)) {
          warning("No valid database connection available")
          return(data.frame())
        }
        
        # Use demonstrate case function to get position data with type filtering
        filtered_data <- fn_get_position_demonstrate_case(
          app_data_connection = app_data_connection,
          product_line_id = prod_line,
          apply_iterative_filter = FALSE,  # No iterative filtering for MS analysis
          apply_type_filter = TRUE
        )
        
        # Check if product_id column exists, if not try to find platform-specific column
        if (!"product_id" %in% names(filtered_data) && nrow(filtered_data) > 0) {
          platform <- platform_id()

          # Ensure platform is a scalar value for switch statement
          if (is.null(platform) || length(platform) == 0) {
            platform <- "default"
          } else if (length(platform) > 1) {
            warning("platform_id() returned multiple values, using first: ", paste(platform, collapse=", "))
            platform <- as.character(platform[1])
          } else {
            platform <- as.character(platform)
          }

          item_col <- switch(platform,
            "amz" = "asin",  # Amazon
            "eby" = "ebay_item_number",  # eBay
            "product_id"  # Default fallback
          )
          
          if (item_col %in% names(filtered_data)) {
            message("DEBUG: Renaming '", item_col, "' to 'product_id' in positionMSPlotly")
            filtered_data <- filtered_data %>% dplyr::rename(product_id = !!sym(item_col))
          } else {
            warning("No product identifier column found in MS position data. Available columns: ", paste(names(filtered_data), collapse = ", "))
            component_status("error")
            return(data.frame())
          }
        }
        
        component_status("ready")
        return(filtered_data)
      }, error = function(e) {
        warning("Error fetching position data: ", e$message)
        component_status("error")
        data.frame()
      })
      
      return(result)
    })
    
    # ------------ CSA Analysis -----------------------------------
    csa_result <- reactive({
      # Only perform expensive CSA analysis when component is active
      if (!is_component_active()) {
        return(NULL)
      }
      
      data <- position_data()
      if (is.null(data) || nrow(data) == 0) return(NULL)
      
      component_status("computing")
      
      # Define variables to exclude from CSA analysis (similar to original Exclude_Variable)
      # Note: We exclude non-numeric, identifier columns, and revenue/rating from MDS analysis
      exclude_vars <- c("product_line_id", "platform_id", "revenue", "rating", "sales", "Revenue", "Rating")
      
      result <- perform_csa_analysis(
        data = data, 
        exclude_vars = exclude_vars, 
        na_threshold = input$na_threshold,
        use_data_modification = input$use_data_modification
      )
      
      if (!is.null(result)) {
        component_status("ready")
      } else {
        component_status("error")
      }
      
      return(result)
    })
    
    # ------------ CSA Data -----------------------------------
    csa_data <- reactive({
      csa <- csa_result()
      
      if (is.null(csa)) {
        return(data.frame())
      }
      
      create_csa_data(
        csa_result = csa,
        brand_highlight = brand_highlight()
      )
    })
    
    # ------------ Cluster Analysis (Immediate Full Analysis) --------
    cluster_analysis <- reactive({
      csa <- csa_result()
      pos_data <- position_data()
      
      if (is.null(csa) || is.null(pos_data)) return(NULL)
      
      # Run full analysis immediately (this should be fast enough)
      analyze_clusters(csa, pos_data)
    })
    
    # ------------ AI Names (Extended Task) ------------------------
    # Create ExtendedTask for AI naming
    # This allows long-running API calls without blocking the UI
    ai_naming_task <- if (!is.null(gpt_key)) {
      ExtendedTask$new(function(analysis_data, api_key) {
        # This function runs in a separate process via future
        future::future({
          generate_cluster_names(analysis_data, api_key)
        }, seed = TRUE)
      })
    } else {
      NULL  # No task if no API key
    }
    
    # Reactive value to track current analysis ID
    current_analysis_id <- reactiveVal(NULL)
    
    # Trigger AI naming when cluster analysis changes
    observe({
      # Only perform AI analysis when component is active
      if (!is_component_active()) return()
      if (is.null(ai_naming_task)) return()
      
      analysis <- cluster_analysis()
      if (is.null(analysis) || nrow(analysis) == 0) return()
      
      # Create unique ID for this analysis
      content_string <- paste(analysis$significant_vars, collapse = "|")
      simple_checksum <- sum(utf8ToInt(content_string)) %% 10000
      new_id <- paste0(nrow(analysis), "_", simple_checksum)
      
      # Check if this is a new analysis
      if (!identical(current_analysis_id(), new_id)) {
        current_analysis_id(new_id)
        
        # Check task status before invoking
        task_status <- ai_naming_task$status()
        
        if (task_status == "running") {
          message("AI naming task already running, new analysis will be queued")
        }
        
        # Show start notification and spinner
        tryCatch({
          showNotification(
            "🤖 AI naming started...",
            id = "ai_naming_status",
            duration = NULL,
            closeButton = FALSE,
            type = "message",
            session = session
          )
          
          # Show spinner overlay
          shinyjs::show(id = "spinner_overlay")
          shinyjs::hide(id = "cluster_table")
        }, error = function(e) {
          message("Error showing start notification: ", e$message)
        })
        
        # Invoke the task with current analysis data
        # ExtendedTask automatically queues if already running
        ai_naming_task$invoke(analysis, gpt_key)
        message("AI naming task invoked for analysis ID: ", new_id)
      }
    })
    
    # Monitor task status and show completion notifications
    observe({
      if (is.null(ai_naming_task)) return()
      
      # React to task status changes
      task_status <- ai_naming_task$status()
      
      if (task_status == "success") {
        # Task completed successfully
        tryCatch({
          # Remove the processing notification and hide spinner
          removeNotification("ai_naming_status", session = session)
          shinyjs::hide(id = "spinner_overlay")
          shinyjs::show(id = "cluster_table")
          
          # Get the result and show success notification
          result <- ai_naming_task$result()
          if (!is.null(result) && length(result) > 0) {
            showNotification(
              paste("✅ AI naming completed:", paste(head(result, 3), collapse = ", "), 
                    if(length(result) > 3) "..." else ""),
              duration = 5,
              type = "success",
              session = session
            )
          }
        }, error = function(e) {
          message("Error showing success notification: ", e$message)
        })
      } else if (task_status == "error") {
        # Task failed
        tryCatch({
          # Remove the processing notification and hide spinner
          removeNotification("ai_naming_status", session = session)
          shinyjs::hide(id = "spinner_overlay")
          shinyjs::show(id = "cluster_table")
          
          # Show error notification
          showNotification(
            "❌ AI naming failed, using default names",
            duration = 5,
            type = "error",
            session = session
          )
        }, error = function(e) {
          message("Error showing error notification: ", e$message)
        })
      }
    })
    
    # AI names update will automatically trigger renderDT re-execution
    # ExtendedTask handles all state management internally
    
    # ------------ Reset filters ------------------------------------
    observeEvent(input$reset_filters, {
      updateSliderInput(session, "na_threshold", value = 0.5)
      updateCheckboxInput(session, "use_data_modification", value = TRUE)
      updateCheckboxInput(session, "show_centers", value = FALSE)
      updateSliderInput(session, "marker_size", value = 10)
      
      message("MS plot filters reset")
    })
    
    
    # ------------ Plot Rendering ------------------------------------
    output$csa_plot <- renderPlotly({
      data <- csa_data()
      
      if (is.null(data) || nrow(data) == 0) {
        return(plotly_empty() %>% 
               layout(title = "No data available for competitive analysis"))
      }
      
      # Create hover text with market share information
      if ("brand" %in% names(data) && "product_id" %in% names(data)) {
        # Calculate market share within each segment
        hover_text <- character(nrow(data))
        
        for (i in 1:nrow(data)) {
          # Get current item's group and revenue
          current_group <- data$group[i]
          current_revenue <- 0
          
          # Try different revenue column names
          if ("revenue" %in% names(data)) {
            current_revenue <- data$revenue[i]
          } else if ("sales" %in% names(data)) {
            current_revenue <- data$sales[i]
          } else if ("Sales" %in% names(data)) {
            current_revenue <- data$Sales[i]
          }
          
          # Calculate total revenue for this segment
          segment_data <- data[data$group == current_group, ]
          segment_total_revenue <- 0
          
          if ("revenue" %in% names(segment_data)) {
            segment_total_revenue <- sum(segment_data$revenue, na.rm = TRUE)
          } else if ("sales" %in% names(segment_data)) {
            segment_total_revenue <- sum(segment_data$sales, na.rm = TRUE)
          } else if ("Sales" %in% names(segment_data)) {
            segment_total_revenue <- sum(segment_data$Sales, na.rm = TRUE)
          }
          
          # Calculate market share within segment
          market_share_in_segment <- ifelse(segment_total_revenue > 0,
                                          round(current_revenue / segment_total_revenue * 100, 1),
                                          0)
          
          hover_text[i] <- paste('Brand:', data$brand[i], 
                               '<br>Product ID:', data$product_id[i],
                               '<br>Market Share in Segment:', market_share_in_segment, '%')
        }
      } else {
        hover_text <- paste('Group:', data$group)
      }
      
      # Define colors based on number of groups
      n_groups <- length(unique(data$group))
      if (n_groups == 1) {
        colors <- c("#1f77b4")
      } else if (n_groups == 2) {
        colors <- c("#1f77b4", "#ff7f0e")
      } else {
        colors <- NULL  # Let plotly use default color scale
      }
      
      # Create the base plot
      p <- plot_ly(
        data,
        x = ~xMDS,
        y = ~yMDS,
        color = ~group,
        colors = colors,
        mode = 'markers',
        symbol = ~shapes,
        text = hover_text,
        hoverinfo = 'text',
        marker = list(size = input$marker_size),
        type = 'scatter'
      )
      
      # Add cluster centers if requested
      if (input$show_centers && nrow(data) > 0) {
        cluster_centers <- data %>%
          dplyr::group_by(group) %>%
          dplyr::summarise(
            center_x = mean(xMDS, na.rm = TRUE),
            center_y = mean(yMDS, na.rm = TRUE),
            .groups = 'drop'
          )
        
        p <- p %>% add_trace(
          data = cluster_centers,
          x = ~center_x,
          y = ~center_y,
          mode = 'markers',
          marker = list(
            size = input$marker_size + 5,
            symbol = 'x',
            color = 'black'
          ),
          name = 'Cluster Centers',
          hoverinfo = 'none'
        )
      }
      
      # Configure layout
      p <- layout(
        p,
        title = "",
        xaxis = list(title = ""),
        yaxis = list(title = ""),
        hovermode = 'closest',
        showlegend = TRUE
      )
      
      return(p)
    })
    
    # ------------ Cluster Table Rendering ---------------------------
    output$cluster_table <- renderDT({
      # Get full analysis immediately
      analysis <- cluster_analysis()
      if (is.null(analysis) || nrow(analysis) == 0) {
        return(data.frame(Message = "No cluster analysis available"))
      }
      
      # Get AI names based on task status
      if (!is.null(ai_naming_task)) {
        task_status <- ai_naming_task$status()  # This is a reactive read
        
        if (task_status == "success") {
          # Get the result from the completed task
          tryCatch({
            ai_names <- ai_naming_task$result()
            if (!is.null(ai_names) && length(ai_names) == nrow(analysis)) {
              analysis$cluster_name <- ai_names
              message("Using AI names in DT render: ", paste(ai_names, collapse = ", "))
            } else {
              analysis$cluster_name <- paste0("Segment ", analysis$cluster)
              message("AI names length mismatch, using default names")
            }
          }, error = function(e) {
            # Error retrieving results
            analysis$cluster_name <- paste0("Segment ", analysis$cluster)
            message("Error retrieving AI names: ", e$message)
          })
        } else if (task_status == "running") {
          # Task is still running, show placeholder
          analysis$cluster_name <- paste0("Segment ", analysis$cluster, " (AI naming...)")
          message("AI naming in progress")
        } else if (task_status == "error") {
          # Task failed, use default names
          analysis$cluster_name <- paste0("Segment ", analysis$cluster)
          message("AI naming failed, using default names")
        } else {
          # Initial state - task hasn't been invoked yet
          analysis$cluster_name <- paste0("Segment ", analysis$cluster)
          message("AI naming not started")
        }
      } else {
        # No AI task available (no API key) 
        analysis$cluster_name <- paste0("Segment ", analysis$cluster)
        message("No AI naming available - OpenAI API key not configured")
      }
      
      csa <- csa_data()
      brand_name <- brand_highlight()
      
      # Find which cluster the brand belongs to
      brand_cluster <- NULL
      brand_revenue <- 0
      if (!is.null(brand_name) && !is.null(csa) && "brand" %in% names(csa)) {
        brand_data <- csa %>% dplyr::filter(brand == brand_name)
        if (nrow(brand_data) > 0) {
          brand_cluster <- as.numeric(brand_data$group[1])
          # Get brand revenue if available
          if ("revenue" %in% names(brand_data)) {
            brand_revenue <- sum(brand_data$revenue, na.rm = TRUE)
          } else if ("sales" %in% names(brand_data)) {
            brand_revenue <- sum(brand_data$sales, na.rm = TRUE)
          } else if ("Sales" %in% names(brand_data)) {
            brand_revenue <- sum(brand_data$Sales, na.rm = TRUE)
          }
        }
      }
      
      # Find app brand data for footer (from app_configs$brand_name)
      app_brand_clusters <- NULL
      app_brand_revenue <- 0
      app_brand_product_id_count <- 0
      app_brand_name <- if (exists("app_configs") && !is.null(app_configs$brand_name)) app_configs$brand_name else NULL
      
      if (!is.null(app_brand_name) && !is.null(csa) && "brand" %in% names(csa)) {
        app_brand_data <- csa %>% dplyr::filter(brand == app_brand_name)
        if (nrow(app_brand_data) > 0) {
          # Get unique clusters for all Product IDs of this brand
          app_brand_clusters <- unique(as.numeric(app_brand_data$group))
          app_brand_product_id_count <- nrow(app_brand_data)
          
          # Sum revenue across all Product IDs
          if ("revenue" %in% names(app_brand_data)) {
            app_brand_revenue <- sum(app_brand_data$revenue, na.rm = TRUE)
          } else if ("sales" %in% names(app_brand_data)) {
            app_brand_revenue <- sum(app_brand_data$sales, na.rm = TRUE)
          } else if ("Sales" %in% names(app_brand_data)) {
            app_brand_revenue <- sum(app_brand_data$Sales, na.rm = TRUE)
          }
        }
      }
      
      # Prepare table data
      # Check if we have revenue data
      has_revenue <- sum(analysis$total_revenue) > 0
      
      if (has_revenue) {
        # Prepare data with Segment column
        table_data <- analysis %>%
          dplyr::mutate(
            `Segment` = cluster_name,
            `Companies` = n_companies,
            `Market Share` = paste0(round(total_revenue / sum(total_revenue) * 100, 1), "%"),
            `Total Revenue` = paste0("$", format(total_revenue, big.mark = ",", scientific = FALSE)),
            `Avg Revenue` = paste0("$", format(round(avg_revenue), big.mark = ",", scientific = FALSE)),
            `Key Characteristics` = significant_vars
          ) %>%
          dplyr::select(`Segment`, `Companies`, `Market Share`, `Total Revenue`, `Avg Revenue`, `Key Characteristics`)
        
        # Add brand row if brand is found and it's not the app brand
        if (!is.null(brand_cluster) && !is.null(brand_name) && brand_name != app_brand_name) {
          total_market_revenue <- sum(analysis$total_revenue)
          brand_share <- ifelse(total_market_revenue > 0, 
                               round(brand_revenue / total_market_revenue * 100, 1), 
                               0)
          
          brand_row <- data.frame(
            `Segment` = brand_name,
            `Companies` = "-",
            `Market Share` = paste0(brand_share, "%"),
            `Total Revenue` = paste0("$", format(brand_revenue, big.mark = ",", scientific = FALSE)),
            `Avg Revenue` = "-",
            `Key Characteristics` = paste0("(Part of ", analysis$cluster_name[analysis$cluster == brand_cluster], ")"),
            stringsAsFactors = FALSE,
            check.names = FALSE
          )
          
          # Append brand row
          table_data <- rbind(table_data, brand_row)
        }
      } else {
        # No revenue data available, show simplified table
        table_data <- analysis %>%
          dplyr::mutate(
            `Segment` = cluster_name,
            `Companies` = n_companies,
            `Key Characteristics` = significant_vars
          ) %>%
          dplyr::select(`Segment`, `Companies`, `Key Characteristics`)
        
        # Add brand row without revenue info (excluding app brand)
        if (!is.null(brand_cluster) && !is.null(brand_name) && brand_name != app_brand_name) {
          brand_row <- data.frame(
            `Segment` = brand_name,
            `Companies` = "-",
            `Key Characteristics` = paste0("(Part of ", analysis$cluster_name[analysis$cluster == brand_cluster], ")"),
            stringsAsFactors = FALSE,
            check.names = FALSE
          )
          
          table_data <- rbind(table_data, brand_row)
        }
      }
      
      # Prepare app brand footer if data exists
      app_brand_footer <- NULL
      if (!is.null(app_brand_clusters) && length(app_brand_clusters) > 0 && !is.null(app_brand_name)) {
        total_market_revenue <- sum(analysis$total_revenue)
        app_brand_share <- ifelse(total_market_revenue > 0 && has_revenue, 
                                 round(app_brand_revenue / total_market_revenue * 100, 1), 
                                 0)
        
        # Calculate average revenue per Product ID
        app_brand_avg_revenue <- ifelse(app_brand_product_id_count > 0, 
                                       app_brand_revenue / app_brand_product_id_count, 
                                       0)
        
        # Get cluster names for all clusters this brand belongs to
        cluster_names <- unique(analysis$cluster_name[analysis$cluster %in% app_brand_clusters])
        cluster_text <- ifelse(length(cluster_names) > 1,
                              paste0("(Part of ", paste(cluster_names, collapse = ", "), ")"),
                              paste0("(Part of ", cluster_names[1], ")"))
        
        # Add Product ID count info if multiple Product IDs
        product_id_info <- ifelse(app_brand_product_id_count > 1, 
                           paste0(" - ", app_brand_product_id_count, " Product IDs"), 
                           "")
        
        if (has_revenue) {
          app_brand_footer <- c(
            paste0(app_brand_name, product_id_info),
            "-",
            paste0(app_brand_share, "%"),
            paste0("$", format(app_brand_revenue, big.mark = ",", scientific = FALSE)),
            paste0("$", format(round(app_brand_avg_revenue), big.mark = ",", scientific = FALSE)),
            cluster_text
          )
        } else {
          app_brand_footer <- c(
            paste0(app_brand_name, product_id_info),
            "-",
            cluster_text
          )
        }
      }
      
      # Create DataTable with dynamic column definitions
      if (has_revenue) {
        # Create container with footer if app brand exists
        if (!is.null(app_brand_footer)) {
          container <- htmltools::tags$table(
            DT::tableHeader(names(table_data)),
            DT::tableFooter(app_brand_footer)
          )
          
          dt <- datatable(
            table_data,
            container = container,
            options = list(
              pageLength = 10,
              searching = FALSE,
              lengthChange = FALSE,
              info = FALSE,
              paging = FALSE,
              scrollX = TRUE,
              columnDefs = list(
                list(className = 'dt-left', targets = c(0, 5)),  # Segment and Key Characteristics
                list(className = 'dt-center', targets = c(1)),   # Companies
                list(className = 'dt-right', targets = c(2, 3, 4))  # Market Share, Revenue columns
              )
            ),
            rownames = FALSE
          )
        } else {
          dt <- datatable(
            table_data,
            options = list(
              pageLength = 10,
              searching = FALSE,
              lengthChange = FALSE,
              info = FALSE,
              paging = FALSE,
              scrollX = TRUE,
              columnDefs = list(
                list(className = 'dt-left', targets = c(0, 5)),  # Segment and Key Characteristics
                list(className = 'dt-center', targets = c(1)),   # Companies
                list(className = 'dt-right', targets = c(2, 3, 4))  # Market Share, Revenue columns
              )
            ),
            rownames = FALSE
          )
        }
        
        dt <- dt %>%
          formatStyle(
            'Segment',  # Segment column
            fontWeight = 'bold'
          ) %>%
          formatStyle(
            'Market Share',
            background = styleColorBar(c(0, 100), 'lightblue'),
            backgroundSize = '100% 90%',
            backgroundRepeat = 'no-repeat',
            backgroundPosition = 'center'
          )
        
        # Highlight brand row if present
        if (!is.null(brand_name) && brand_name %in% table_data$Segment && brand_name != app_brand_name) {
          dt <- dt %>%
            formatStyle(
              'Segment',
              target = 'row',
              backgroundColor = styleEqual(brand_name, '#fffacd')  # Light yellow background
            )
        }
      } else {
        # No revenue case
        if (!is.null(app_brand_footer)) {
          container <- htmltools::tags$table(
            DT::tableHeader(names(table_data)),
            DT::tableFooter(app_brand_footer)
          )
          
          dt <- datatable(
            table_data,
            container = container,
            options = list(
              pageLength = 10,
              searching = FALSE,
              lengthChange = FALSE,
              info = FALSE,
              paging = FALSE,
              scrollX = TRUE,
              columnDefs = list(
                list(className = 'dt-left', targets = c(0, 2)),  # Segment and Key Characteristics
                list(className = 'dt-center', targets = 1)        # Companies
              )
            ),
            rownames = FALSE
          )
        } else {
          dt <- datatable(
            table_data,
            options = list(
              pageLength = 10,
              searching = FALSE,
              lengthChange = FALSE,
              info = FALSE,
              paging = FALSE,
              scrollX = TRUE,
              columnDefs = list(
                list(className = 'dt-left', targets = c(0, 2)),  # Segment and Key Characteristics
                list(className = 'dt-center', targets = 1)        # Companies
              )
            ),
            rownames = FALSE
          )
        }
        
        dt <- dt %>%
          formatStyle(
            'Segment',  # Segment column
            fontWeight = 'bold'
          )
        
        # Highlight brand row if present (for non-revenue case)
        if (!is.null(brand_name) && brand_name %in% table_data$Segment && brand_name != app_brand_name) {
          dt <- dt %>%
            formatStyle(
              'Segment',
              target = 'row',
              backgroundColor = styleEqual(brand_name, '#fffacd')  # Light yellow background
            )
        }
      }
      
      return(dt)
    })
    
    # Display component status
    output$component_status <- renderText({
      # MP031: Defensive programming - check for NULL/empty values before switch
      # R113: Error handling for reactive expressions
      status_val <- tryCatch({
        component_status()
      }, error = function(e) {
        warning("Error getting component status: ", e$message)
        "idle"
      })

      # MP099: Defensive check for NULL or empty status
      if (is.null(status_val) || length(status_val) == 0 || status_val == "") {
        return("Ready for position analysis")
      }

      # Ensure status_val is character and length 1 for switch
      status_val <- as.character(status_val)[1]

      switch(status_val,
             idle = "Ready for position analysis",
             loading = "Loading position data...",
             ready = paste0("Position data loaded: ", nrow(position_data()), " records"),
             computing = "Computing position metrics...",
             error = "Error loading position data",
             status_val)  # Default: return the status value itself
    })
    
    # ---- AI Market Segmentation Analysis Report --------------------------------
    # Get OpenAI API key (same as for AI naming)
    
    # AI analysis result storage
    ms_ai_analysis_result <- reactiveVal("")
    
    # Track if analysis has been generated for current data
    ms_analysis_generated <- reactiveVal(FALSE)
    
    # REMOVED: Legacy implementation - now using chat_api for GPT-5 compatibility
    # See MP123: AI Prompt Configuration Management
    
    # Render AI analysis output
    output$ms_analysis_output <- renderUI({
      result <- ms_ai_analysis_result()
      if (result == "") {
        return(div(class = "text-muted text-center p-4",
                   style = "font-style: italic;",
                   "AI market segmentation report will be automatically generated when visualization data is ready..."))
      }
      
      # Parse markdown content and convert to HTML
      if (requireNamespace("markdown", quietly = TRUE)) {
        HTML(markdown::markdownToHTML(text = result, fragment.only = TRUE))
      } else {
        # Fallback: display as preformatted text
        pre(result)
      }
    })
    
    # Reset analysis flag when filters change
    observeEvent({
      platform_id()
      product_line_id()
    }, {
      # Clear previous analysis and reset flag
      ms_ai_analysis_result("Filters changed. AI analysis will be regenerated with new data...")
      ms_analysis_generated(FALSE)
    }, ignoreInit = TRUE)
    
    # Auto-generate AI analysis when AI naming is complete
    observeEvent({
      # Monitor AI naming task completion
      if (!is.null(ai_naming_task)) {
        ai_naming_task$status()
      } else {
        component_status()  # Fallback if no AI task
      }
    }, {
      # Only generate when component is ready and not already generated
      if (component_status() == "ready" && !ms_analysis_generated()) {
        
        if (is.null(gpt_key)) {
          ms_ai_analysis_result("OpenAI API key not configured. AI analysis features are disabled.")
          ms_analysis_generated(TRUE)
          return()
        }
        
        # Check if AI naming is complete (or if no AI naming task exists)
        should_generate <- FALSE
        if (!is.null(ai_naming_task)) {
          task_status <- ai_naming_task$status()
          if (task_status == "success" || task_status == "error") {
            should_generate <- TRUE
            message("DEBUG: AI naming completed with status: ", task_status, ", generating analysis report...")
          }
        } else {
          # No AI naming task - generate immediately
          should_generate <- TRUE
          message("DEBUG: No AI naming task available, generating analysis report with default names...")
        }
        
        if (!should_generate) {
          message("DEBUG: AI naming still in progress, waiting...")
          return()
        }
        
        # Mark as generating to prevent duplicate calls
        ms_analysis_generated(TRUE)
        
        # Generate report immediately since AI naming is complete
        later::later(function() {
          tryCatch({
            # Get the actual table data that will be displayed to users
            # This is the same logic as in the cluster_table renderDT
            analysis <- isolate(cluster_analysis())
            if (is.null(analysis) || nrow(analysis) == 0) {
              ms_ai_analysis_result("No cluster analysis data available for AI report.")
              return()
            }
            
            message("DEBUG: Getting actual table data for AI analysis...")
            
            # Get the same table data that's shown in the DT
            table_data <- tryCatch({
              # Get cluster data with AI names (same logic as DT render)
              analysis_with_names <- analysis
              
              # Add AI names if available (same logic as cluster_table render)
              if (!is.null(ai_naming_task)) {
                task_status <- isolate(ai_naming_task$status())
                if (task_status == "success") {
                  ai_names <- isolate(ai_naming_task$result())
                  if (!is.null(ai_names) && length(ai_names) == nrow(analysis_with_names)) {
                    analysis_with_names$cluster_name <- ai_names
                  } else {
                    analysis_with_names$cluster_name <- paste0("Segment ", analysis_with_names$cluster)
                  }
                } else {
                  analysis_with_names$cluster_name <- paste0("Segment ", analysis_with_names$cluster)
                }
              } else {
                analysis_with_names$cluster_name <- paste0("Segment ", analysis_with_names$cluster)
              }
              
              # Create the table in the same format as shown to users
              csa <- isolate(csa_data())
              app_brand_name <- isolate(brand_highlight())
              
              # Check if we have revenue data
              has_revenue <- "total_revenue" %in% names(analysis_with_names) && 
                           any(!is.na(analysis_with_names$total_revenue) & analysis_with_names$total_revenue > 0)
              
              if (has_revenue) {
                # Same table structure as in renderDT
                final_table <- analysis_with_names %>%
                  dplyr::mutate(
                    `Segment` = cluster_name,
                    `Companies` = n_companies,
                    `Market Share` = paste0(round(total_revenue / sum(total_revenue) * 100, 1), "%"),
                    `Total Revenue` = paste0("$", format(total_revenue, big.mark = ",", scientific = FALSE)),
                    `Avg Revenue` = paste0("$", format(round(avg_revenue), big.mark = ",", scientific = FALSE)),
                    `Key Characteristics` = significant_vars
                  ) %>%
                  dplyr::select(`Segment`, `Companies`, `Market Share`, `Total Revenue`, `Avg Revenue`, `Key Characteristics`)
              } else {
                # Simplified table without revenue
                final_table <- analysis_with_names %>%
                  dplyr::mutate(
                    `Segment` = cluster_name,
                    `Companies` = n_companies,
                    `Key Characteristics` = significant_vars
                  ) %>%
                  dplyr::select(`Segment`, `Companies`, `Key Characteristics`)
              }
              
              final_table
              
            }, error = function(e) {
              message("DEBUG: Error creating table data: ", e$message)
              data.frame(
                Segment = "Error creating table",
                Companies = 0,
                `Key Characteristics` = paste("Error:", e$message)
              )
            })
            
            # Convert table to CSV format for GPT
            csv_data <- tryCatch({
              # Create a string representation of the table
              con <- textConnection(NULL, "w")
              write.csv(table_data, file = con, row.names = FALSE)
              csv_string <- paste(textConnectionValue(con), collapse = "\n")
              close(con)
              csv_string
            }, error = function(e) {
              message("DEBUG: Error converting to CSV: ", e$message)
              "Error converting table data to CSV format"
            })
            
            message("DEBUG: Table data converted to CSV, length: ", nchar(csv_data))
            message("DEBUG: First 200 chars of CSV: ", substr(csv_data, 1, 200))
            
            # Load centralized prompt configuration (MP123: AI Prompt Configuration Management)
            prompt_config <- load_openai_prompt("position_analysis.csa_market_segments")

            # Prepare segment summary and product distribution from CSV data
            segment_summary <- csv_data
            product_distribution <- csv_data  # Same data for now, can be customized

            # Replace user template variables only (system_prompt already resolved by load_openai_prompt)
            user_content <- prompt_config$user_prompt_template
            user_content <- gsub("{segment_summary}", segment_summary, user_content, fixed = TRUE)
            user_content <- gsub("{product_distribution}", product_distribution, user_content, fixed = TRUE)

            # Create messages (system_prompt already resolved by load_openai_prompt, MP032: DRY)
            sys <- list(role = "system", content = prompt_config$system_prompt)
            usr <- list(role = "user", content = user_content)

            message("DEBUG: About to call GPT for market segmentation analysis...")
            message("DEBUG: CSV data being sent to GPT: ", substr(csv_data, 1, 200), "...")

            # Use model from centralized config (MP051: Explicit Parameter Specification)
            txt <- chat_api(list(sys, usr), gpt_key, model = prompt_config$model)
            
            message("DEBUG: GPT response received, length: ", nchar(txt))
            message("DEBUG: First 100 chars of response: ", substr(txt, 1, 100))
            
            ms_ai_analysis_result(txt)
            
          }, error = function(e) {
            ms_ai_analysis_result(paste("Error generating analysis:", e$message))
          })
        }, delay = 1)  # Short delay since AI naming is already complete
      }
    }, ignoreInit = TRUE)
    
    # Return reactive values for external use
    return(list(
      position_data = position_data,
      csa_result = csa_result,
      csa_data = csa_data,
      component_status = component_status
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' positionMSPlotlyComponent
#' 
#' Implements a competitive set analysis visualization component using MDS
#' following the Connected Component principle.
#' 
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. The data connection supporting Enhanced Data Access pattern (R116).
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Configuration parameters for customizing component behavior (optional).
#'        If reactive, will be re-evaluated when dependencies change.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return A list containing UI and server functions structured according to the Connected Component Principle (MP56).
#'         The UI element contains 'filter' and 'display' components, and the server function initializes component functionality.
#' @examples
#' # Basic usage with default settings
#' csaComp <- positionMSPlotlyComponent("csa_plot")
#' 
#' # Usage with database connection
#' csaComp <- positionMSPlotlyComponent(
#'   id = "csa_plot",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")
#' )
#'
#' # Usage with reactive configuration
#' csaComp <- positionMSPlotlyComponent(
#'   id = "csa_plot",
#'   app_data_connection = app_conn,
#'   config = reactive({ list(filters = list(platform_id = input$platform)) })
#' )
#' @export
positionMSPlotlyComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = positionMSPlotlyFilterUI(id, translate),
              display = positionMSPlotlyDisplayUI(id, translate)),
    server = function(input, output, session) {
      positionMSPlotlyServer(id, app_data_connection, config, session)
    }
  )
}
