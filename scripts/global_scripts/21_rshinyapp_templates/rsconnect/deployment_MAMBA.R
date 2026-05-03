# AI Marketing Platform - MAMBA App Deployment Script
#
# @file deployment_MAMBA.R
# @principle R103 Package Documentation Reference Rule
# @principle R95 Import Requirements Rule
# @uses_package rsconnect See 20_R_packages/rsconnect.md for usage patterns
# @uses_package stringr See 20_R_packages/stringr.md for string manipulation patterns
#
# This script deploys the MAMBA Precision Marketing app to shinyapps.io
# using the reusable fn_deploy_shiny_app utility.
# 
# The app_file parameter allows specifying any R file as the main app file.
# The deployment function will temporarily copy it to app.R, deploy, then restore
# the original app.R file. This avoids Shiny's requirement for app.R naming.

library(rsconnect)
library(stringr)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
APP_NAME  <- "MAMBA_Precision_Markerting_Martech" 
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
  account    = ACCOUNT,
  app_file = file.path(
    "update_scripts", "global_scripts",
    "10_rshinyapp_components", "unions",
    "union_production_test.R"
  )
)

cat("\nDeployment complete!\n")
cat(paste0("Application URL: https://", ACCOUNT, ".shinyapps.io/", APP_NAME, "/\n"))
