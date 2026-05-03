Recode_time_TraceBack <- function(profile) {
  switch(profile,
         "m1quarter" = "m1quarter",
         "m1year" = "m1year",
         "m1month" = "m1month",
         "quarter" = "m1quarter",
         "year" = "m1year",
         "month" = "m1month",
         NA)  # If no match, return NA
}
