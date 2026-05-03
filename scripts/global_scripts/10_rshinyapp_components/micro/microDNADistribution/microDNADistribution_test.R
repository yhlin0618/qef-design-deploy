#LOCK FILE
#
# microDNADistribution_test.R 
#
# Test file for the microDNADistribution component
#

autoinit()

# # Initialize in APP_MODE using the standard initialization script
# init_script_path <- file.path("update_scripts", "global_scripts", "22_initializations", 
#                             "sc_initialization_app_mode.R")
# # Source the initialization script
# source(init_script_path)

# Load required libraries
library(shiny)
library(shinydashboard)
library(plotly)
library(dplyr)

# Source the component file
component_path <- file.path("update_scripts", "global_scripts", "10_rshinyapp_components", 
                          "micro", "microDNADistribution", "microDNADistribution.R")
source(component_path)

# Source the tbl2 function for Enhanced Data Access (R116)
tbl2_path <- file.path("update_scripts", "global_scripts", "02_db_utils", "fn_tbl2.R")
source(tbl2_path)

# Create mock data access function
create_mock_dna_data <- function(n = 200) {
  set.seed(123)  # For reproducibility
  
  # Generate random data for customer DNA attributes
  data.frame(
    customer_id = 1:n,
    platform_id = sample(c(6, 7), n, replace = TRUE),
    M = round(rlnorm(n, meanlog = 4, sdlog = 1), 2),  # Purchase amount (log-normal distribution)
    R = round(runif(n, 1, 365), 0),                  # Recency (days)
    F = sample(1:10, n, replace = TRUE, prob = exp(-0.3*(1:10))),  # Frequency with decaying probability
    IPT_mean = round(runif(n, 10, 120), 1),           # Inter-purchase time (mean)
    NES = sample(c("E0", "S", "L"), n, replace = TRUE, prob = c(0.2, 0.6, 0.2))  # NES status
  )
}

# Override tbl2 for testing with mock data
.original_tbl2 <- tbl2
tbl2 <- function(src, from, ...) {
  # For testing, intercept calls to df_dna_by_customer
  if (is.character(from) && from == "df_dna_by_customer") {
    mock_data <- create_mock_dna_data(200)
    
    # If src is NULL, return all data
    if (is.null(src)) {
      return(mock_data)
    }
    
    # Handle filtering by platform_id for later calls in the chain
    return(mock_data)
  }
  
  # Fall back to the original implementation for other cases
  .original_tbl2(src, from, ...)
}

# Define the app
ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      .component-header {
        margin-bottom: 15px;
        border-bottom: 1px solid #eee;
        padding-bottom: 10px;
      }
      .component-output {
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 15px;
        background-color: #f9f9f9;
      }
      .stats-container {
        display: flex;
        flex-wrap: wrap;
        margin-top: 10px;
      }
      .stat-box {
        flex: 1 0 22%;
        margin: 5px;
        padding: 10px;
        background-color: #f5f5f5;
        border-radius: 4px;
        text-align: center;
      }
      .stat-box h5 {
        margin-top: 0;
        font-weight: bold;
      }
    "))
  ),
  titlePanel("microDNADistribution Component Test"),
  
  # Info box
  wellPanel(
    h3("Component Test Environment"),
    p("This is a test environment for the microDNADistribution component."),
    p("The component is using mock data for testing purposes."),
    code("OPERATION_MODE = '", OPERATION_MODE, "'")
  ),
  
  # Component UI
  fluidRow(
    column(width = 12,
           microDNADistribution("test_module")$ui()
    )
  )
)

server <- function(input, output, session) {
  # Initialize component
  microDNADistribution("test_module")$server(
    id = "test_module",
    app_data_connection = NULL,  # Using mock data
    session = session
  )
}

# Run the test app
shinyApp(ui, server)
