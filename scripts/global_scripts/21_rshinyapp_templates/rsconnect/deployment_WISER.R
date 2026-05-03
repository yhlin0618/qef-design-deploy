# AI Marketing Platform - Customer DNA App Deployment Script
#
# @file deployment.R
# @principle R103 Package Documentation Reference Rule
# @principle R95 Import Requirements Rule
# @uses_package rsconnect See 20_R_packages/rsconnect.md for usage patterns
# @uses_package stringr See 20_R_packages/stringr.md for string manipulation patterns
#
# This script deploys the Customer DNA app to shinyapps.io or Posit Connect
# using the reusable fn_deploy_shiny_app utility.

library(rsconnect)
library(stringr)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
APP_NAME  <- "WISER_Precision_Markerting_Martech" 
APP_TITLE <- "AI Marketing Platform"
ACCOUNT   <- "kyle-lin"  # Replace with your actual shinyapps.io account

# -----------------------------------------------------------------------------
# Source deployment utility and deploy
# -----------------------------------------------------------------------------
deploy_utils <- file.path(
  "update_scripts", "global_scripts",
  "11_rshinyapp_utils", "fn_deploy_shiny_app.R"
)
source(deploy_utils)

fn_deploy_shiny_app(
  app_name   = APP_NAME,
  app_title  = APP_TITLE,
  account    = ACCOUNT
  # primary_doc = file.path(
  #   "update_scripts", "global_scripts",
  #   "10_rshinyapp_components", "unions",
  #   "union_production_MAMBA2_test.R"
  # )
)

cat("\nDeployment complete!\n")
cat(paste0("Application URL: https://", ACCOUNT, ".shinyapps.io/", APP_NAME, "/\n"))
