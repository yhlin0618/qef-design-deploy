# Function to convert California time to local time
convert_to_local_time <- function(data) {
  
  state_data <- data.frame(
    abbreviation = state.abb,
    fullname=state.name,
    timezone = c(
      "CT", "ET", "MT", "MT", "PT", "MT", "ET", "ET", "ET", "ET", "ET", "CT", "ET", 
      "CT", "CT", "ET", "ET", "CT", "CT", "ET", "ET", "ET", "CT", "CT", "ET", "ET", 
      "ET", "MT", "ET", "ET", "ET", "ET", "ET", "MT", "MT", "ET", "CT", "ET", "ET", 
      "ET", "MT", "CT", "ET", "CT", "MT", "PT", "ET", "CT", "CT", "HT"
    ),
    offset = c(
      2, 3, 1, 1, 0, 1, 3, 3, 3, 3, 3, 2, 3, 2, 2, 3, 3, 2, 2, 3, 3, 3, 2, 2, 3, 
      3, 3, 1, 3, 3, 3, 3, 3, 1, 1, 3, 2, 3, 3, 3, 1, 2, 3, 2, 1, 0, 3, 2, 2, -3
    )
  )
  
  local_time <- mapply(function(state, time) {
    if (is.na(time)) {
      return(NA)
    } else {
      offset <- state_data$offset[state_data$abbreviation == state]
      local_time <- as.POSIXct(time, tz="America/Los_Angeles") + offset * 3600
      return(format(local_time, tz="UTC", usetz=TRUE))
    }
  }, data$Billing_Province, data$time)
  
  data$Local_time <- as.character(local_time)
  return(data)
}
