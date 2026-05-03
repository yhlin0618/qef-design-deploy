# =============================================================================
# fn_dna_marketing_utils.R — Per-Metric Marketing Recommendation Utilities
# Migrated from L3 sandbox dna_marketing_utils.R → L4 enterprise pattern
# Following: UI_R001, MP029, DEV_R001
# =============================================================================

#' Get default marketing recommendations for a given metric and segment
#' @param metric Character: R, F, M, CAI, PCV, CRI, NES
#' @param segment Character: segment label (Chinese)
#' @return List with strategy and actions
# DEV_R052: Return English keys for strategy/actions; translate() at UI boundary
get_default_marketing_recommendations <- function(metric, segment) {
  recommendations <- switch(
    metric,
    "R" = switch(
      as.character(segment),
      "Recent Buyer"    = list(strategy = "Deepen Engagement", actions = c("Cross-sell related products", "Provide VIP exclusive offers", "Invite to new product trials")),
      "Medium Inactive" = list(strategy = "Maintain Interaction", actions = c("Regular content push", "Seasonal promotions", "Member points rewards")),
      "Long Inactive"   = list(strategy = "Win-Back Activation", actions = c("Send personalized win-back emails", "Provide limited-time coupons", "Push nostalgic products")),
      list(strategy = "Standard Maintenance", actions = c("Regular care", "Product update notifications", "Member benefits"))
    ),
    "F" = switch(
      as.character(segment),
      "High Frequency"   = list(strategy = "Loyalty Deepening", actions = c("VIP membership program", "Bulk purchase discounts", "Dedicated customer service")),
      "Medium Frequency"  = list(strategy = "Stable Maintenance", actions = c("Cumulative spending rewards", "Birthday offers", "Regular care")),
      "Low Frequency"    = list(strategy = "Frequency Boost", actions = c("Post-purchase follow-up", "Free sample gifts", "Purchase reminders")),
      list(strategy = "Basic Maintenance", actions = c("Regular promotions", "Product recommendations", "Satisfaction surveys"))
    ),
    "M" = switch(
      as.character(segment),
      "High Value"   = list(strategy = "Value Maximization", actions = c("Premium product recommendations", "Customized services", "VIP experience")),
      "Medium Value"  = list(strategy = "Upselling", actions = c("Upgrade product recommendations", "Bundle sales", "Membership upgrade")),
      "Low Value"    = list(strategy = "Value Enhancement", actions = c("Entry product recommendations", "Combo offers", "Installment payments")),
      list(strategy = "Upselling", actions = c("Upgrade product recommendations", "Bundle sales", "Membership upgrade"))
    ),
    "CAI" = switch(
      as.character(segment),
      "Increasingly Active" = list(strategy = "Capture Growth Momentum", actions = c("Recommend new products", "Increase interaction frequency", "Provide upgrade plans")),
      "Stable"              = list(strategy = "Stable Maintenance", actions = c("Regular care", "Seasonal activities", "Maintain current services")),
      "Gradually Inactive"  = list(strategy = "Urgent Win-Back", actions = c("Personalized care", "Special offers", "Understand churn reasons")),
      list(strategy = "Standard Service", actions = c("Basic maintenance", "Product updates", "Customer surveys"))
    ),
    "PCV" = switch(
      as.character(segment),
      "High Value" = list(strategy = "Value Maximization", actions = c("VIP dedicated services", "Provide premium products", "Customized plans")),
      "Low Value"  = list(strategy = "Value Nurturing", actions = c("Entry offers", "Educational content", "Small trial offers")),
      list(strategy = "Value Enhancement", actions = c("Upgrade guidance", "Combo offers", "Loyalty programs"))
    ),
    "CRI" = switch(
      as.character(segment),
      "High Engagement"  = list(strategy = "Core Customer Management", actions = c("Priority service", "Exclusive activities", "Co-create value")),
      "Medium Engagement" = list(strategy = "Potential Development", actions = c("Goal setting", "Stage rewards", "Social interaction")),
      "Low Engagement"   = list(strategy = "Engagement Boost", actions = c("Interaction incentives", "Simplify process", "New user onboarding")),
      list(strategy = "Basic Interaction", actions = c("Regular communication", "Opinion collection", "Service improvement"))
    ),
    "NES" = switch(
      as.character(segment),
      "New Customer" =, "N" = list(strategy = "New Customer Nurturing", actions = c("Welcome package", "New user onboarding", "First purchase offer")),
      "Main Customer" =, "E0" = list(strategy = "Core Maintenance", actions = c("VIP services", "Loyalty rewards", "Exclusive activities")),
      "Risk Customer" =, "S1" = list(strategy = "Risk Management", actions = c("Early warning care", "Satisfaction surveys", "Retention plans")),
      "Lost Customer" =, "S2" = list(strategy = "Win-Back Strategy", actions = c("Awakening activities", "Special offers", "Repositioning")),
      "S3" = list(strategy = "Low-Cost Maintenance", actions = c("Automated recommendations", "Low-cost exposure", "Basic maintenance")),
      list(strategy = "Standard Service", actions = c("Basic maintenance", "Regular communication", "Product updates"))
    ),
    # Default
    list(strategy = "Standard Maintenance", actions = c("Regular care and maintenance", "Provide personalized services", "Build long-term relationships"))
  )

  if (is.null(recommendations)) {
    recommendations <- list(strategy = "Standard Maintenance", actions = c("Regular care", "Product update notifications", "Member benefits"))
  }

  recommendations
}

#' Generate recommendations HTML cards for display in Shiny UI
#' @param data Data frame with segment column
#' @param metric Character: R, F, M, CAI, PCV, CRI, NES
#' @param translate Translation function
#' @return tagList of recommendation cards
generate_recommendations_html <- function(data, metric, translate = identity) {
  if (is.null(data) || nrow(data) == 0 || !"segment" %in% names(data)) {
    return(tags$p(translate("No Data")))
  }

  # DEV_R052: English keys for titles, translate() at UI boundary
  metric_titles <- list(
    "R" = "Recency Marketing Strategy",
    "F" = "Frequency Marketing Strategy",
    "M" = "Monetary Marketing Strategy",
    "CAI" = "Customer Activity Marketing Strategy",
    "PCV" = "Past Customer Value Marketing Strategy",
    "CRI" = "Customer Relationship Marketing Strategy",
    "NES" = "Customer Status Marketing Strategy"
  )

  title <- metric_titles[[metric]]
  if (is.null(title)) title <- "Marketing Strategy"

  segments <- unique(data$segment)
  segments <- segments[!is.na(segments)]

  tagList(
    tags$h5(paste0("\ud83c\udfaf ", translate(title))),
    lapply(segments, function(seg) {
      seg_data <- data[data$segment == seg & !is.na(data$segment), , drop = FALSE]
      seg_count <- nrow(seg_data)
      seg_pct <- round(seg_count / nrow(data) * 100, 1)
      rec <- get_default_marketing_recommendations(metric, seg)

      tags$div(
        class = "mb-3 p-3",
        style = "background: #f8f9fa; border-radius: 8px; border-left: 4px solid #007bff;",
        tags$h6(
          paste0("\ud83d\udd38 ", translate(seg)),
          tags$small(class = "text-muted ml-2", paste0("(", seg_count, "\u4eba, ", seg_pct, "%)")),
          style = "color: #2c3e50; font-weight: bold; margin-bottom: 10px;"
        ),
        if (!is.null(rec$strategy)) {
          tags$p(tags$b(translate("Strategy"), "\uff1a"), translate(rec$strategy), style = "margin-bottom: 8px;")
        },
        if (!is.null(rec$actions) && length(rec$actions) > 0) {
          tags$ul(
            style = "margin: 0; padding-left: 20px;",
            lapply(rec$actions, function(action) {
              tags$li(translate(action), style = "margin: 3px 0; color: #495057;")
            })
          )
        }
      )
    })
  )
}
