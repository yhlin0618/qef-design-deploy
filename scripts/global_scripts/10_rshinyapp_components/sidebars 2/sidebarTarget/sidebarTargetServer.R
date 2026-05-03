#' Target Section Sidebar Server Component
#'
#' This component provides server-side logic for the target marketing sidebar,
#' handling campaign creation, selection, and configuration.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarTargetUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarTargetDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param data_source The data source specification, which can be:
#'   - NULL: Will use defaults
#'   - String: A table/query name (e.g., "campaigns")
#'   - Array: Multiple related tables in specific order
#'   - Object: Multiple tables with specific roles (e.g., {campaigns: "campaigns_table"})
#'
#' @return None (server component with side effects)
#' @export
sidebarTargetServer <- function(id, data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get default values for all outputs
    defaults <- sidebarTargetDefaults()
    
    # Process data source using the utility function
    tables <- reactive({
      processDataSource(
        data_source = data_source, 
        table_names = c("campaigns", "audiences", "primary"),
        get_table_func = function(table_name) {
          tryCatch({
            if (is.null(data_source)) {
              return(data.frame())
            } else if (is.function(data_source[[table_name]])) {
              return(data_source[[table_name]]())
            } else {
              return(data.frame())
            }
          }, error = function(e) {
            message("Error retrieving table ", table_name, ": ", e$message)
            return(data.frame())
          })
        }
      )
    })
    
    # Populate campaigns dropdown
    observe({
      campaigns_data <- tables()$campaigns
      
      if (!is.null(campaigns_data) && nrow(campaigns_data) > 0 && 
          all(c("campaign_id", "campaign_name") %in% names(campaigns_data))) {
        
        # Create named list for dropdown with a "New Campaign" option
        campaign_choices <- c(
          "新建活動 (New Campaign)" = "new",
          setNames(
            as.list(campaigns_data$campaign_id),
            campaigns_data$campaign_name
          )
        )
        
        updateSelectInput(
          session,
          "campaign_selector",
          choices = campaign_choices,
          selected = campaign_choices[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "campaign_selector",
          choices = c(
            "新建活動 (New Campaign)" = "new",
            defaults$campaigns
          ),
          selected = "new"
        )
      }
    })
    
    # Populate audience segments dropdown
    observe({
      audience_data <- tables()$audiences
      
      if (!is.null(audience_data) && nrow(audience_data) > 0 && 
          all(c("audience_id", "audience_name", "audience_size") %in% names(audience_data))) {
        
        # Create named list for dropdown with audience size in the labels
        audiences <- setNames(
          as.list(audience_data$audience_id),
          paste0(
            audience_data$audience_name, 
            " (", format(audience_data$audience_size, big.mark = ","), " 顧客)"
          )
        )
        
        updateSelectInput(
          session,
          "target_audience",
          choices = audiences,
          selected = audiences[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "target_audience",
          choices = defaults$audiences,
          selected = names(defaults$audiences)[1]
        )
      }
    })
    
    # Create reactive for current campaign
    current_campaign <- reactiveVal(NULL)
    
    # Handle campaign selection
    observeEvent(input$campaign_selector, {
      # If "new" is selected, clear the form
      if (input$campaign_selector == "new") {
        current_campaign(NULL)
        
        # Reset form to defaults
        updateRadioButtons(session, "campaign_type", selected = "promotional")
        updateCheckboxGroupInput(session, "campaign_channels", selected = c("email"))
        updateSliderInput(session, "campaign_budget", value = 10000)
        updateDateRangeInput(
          session, 
          "campaign_dates", 
          start = Sys.Date() + 7,
          end = Sys.Date() + 21
        )
        
      } else {
        # Load the selected campaign
        campaigns_data <- tables()$campaigns
        
        if (!is.null(campaigns_data) && nrow(campaigns_data) > 0) {
          # Find the selected campaign
          selected_campaign <- campaigns_data[campaigns_data$campaign_id == input$campaign_selector, ]
          
          if (nrow(selected_campaign) > 0) {
            # Store the current campaign
            current_campaign(selected_campaign)
            
            # Update form with campaign values
            updateSelectInput(session, "target_audience", selected = selected_campaign$audience_id)
            updateRadioButtons(session, "campaign_type", selected = selected_campaign$campaign_type)
            
            # Parse channels (stored as comma-separated string)
            channels <- strsplit(selected_campaign$channels, ",")[[1]]
            updateCheckboxGroupInput(session, "campaign_channels", selected = channels)
            
            updateSliderInput(session, "campaign_budget", value = selected_campaign$budget)
            updateDateRangeInput(
              session, 
              "campaign_dates", 
              start = as.Date(selected_campaign$start_date),
              end = as.Date(selected_campaign$end_date)
            )
          }
        }
      }
    })
    
    # Handle "New Campaign" button
    observeEvent(input$new_campaign, {
      updateSelectInput(session, "campaign_selector", selected = "new")
    })
    
    # Calculate and show projected metrics
    output$projected_metrics <- renderUI({
      req(input$campaign_selector != "new")
      req(input$campaign_budget)
      req(input$campaign_dates)
      
      # Get the current campaign if available
      campaign <- current_campaign()
      
      # Calculate projected metrics based on inputs
      budget <- input$campaign_budget
      duration <- as.numeric(difftime(input$campaign_dates[2], input$campaign_dates[1], units = "days"))
      channels_count <- length(input$campaign_channels)
      
      # Simple projection logic (would be more sophisticated in real app)
      # These are dummy calculations for illustration
      reach <- budget * 10 * channels_count
      response_rate <- 0.05 - (0.005 * channels_count)  # Diminishing returns with more channels
      revenue <- budget * 3.5 * (1 + (duration / 30))   # ROI improves with longer campaigns
      
      # If we have historical data use it instead
      if (!is.null(campaign) && "projected_reach" %in% names(campaign)) {
        reach <- campaign$projected_reach
        response_rate <- campaign$projected_response_rate
        revenue <- campaign$projected_revenue
      }
      
      # Render metrics
      tagList(
        div(
          style = "margin-bottom: 8px;",
          tags$strong("預計觸及數: "), 
          format(ceiling(reach), big.mark = ","),
          tags$span(" 顧客", style = "color: #666;")
        ),
        div(
          style = "margin-bottom: 8px;",
          tags$strong("預計回應率: "), 
          paste0(format(response_rate * 100, digits = 2), "%")
        ),
        div(
          style = "margin-bottom: 8px;",
          tags$strong("預計營收: "), 
          paste0("$", format(ceiling(revenue), big.mark = ","))
        ),
        div(
          style = "margin-bottom: 8px;",
          tags$strong("預計投資報酬率: "), 
          paste0(format((revenue / budget - 1) * 100, digits = 2), "%")
        )
      )
    })
    
    # Handle Save Campaign button
    observeEvent(input$save_campaign, {
      # Check if required fields are filled
      req(input$target_audience)
      req(input$campaign_type)
      req(length(input$campaign_channels) > 0)
      req(input$campaign_budget)
      req(input$campaign_dates[1])
      req(input$campaign_dates[2])
      
      # Check if this is a new campaign or an update
      is_new <- input$campaign_selector == "new"
      
      # Create campaign data
      campaign_data <- list(
        campaign_id = if(is_new) paste0("camp_", format(Sys.time(), "%Y%m%d%H%M%S")) else input$campaign_selector,
        campaign_name = if(is_new) paste("Campaign", format(Sys.Date(), "%Y-%m-%d")) else current_campaign()$campaign_name,
        audience_id = input$target_audience,
        campaign_type = input$campaign_type,
        channels = paste(input$campaign_channels, collapse = ","),
        budget = input$campaign_budget,
        start_date = input$campaign_dates[1],
        end_date = input$campaign_dates[2],
        created_date = if(is_new) Sys.Date() else current_campaign()$created_date,
        modified_date = Sys.Date()
      )
      
      # In a real app, we would save this to the database
      # For now, just show a notification
      showNotification(
        if(is_new) "新活動已建立 (New campaign created)" else "活動已更新 (Campaign updated)",
        type = "message"
      )
      
      # Store in session for access by target modules
      session$userData$current_campaign <- campaign_data
      
      # If this was a new campaign, we would update the campaigns list
      if (is_new) {
        # In a real app, we would add this to the database and reload
        # For demonstration, we'll pretend it was successful
        showNotification(
          "新活動已加入清單 (New campaign added to list)",
          type = "message"
        )
      }
    })
  })
}