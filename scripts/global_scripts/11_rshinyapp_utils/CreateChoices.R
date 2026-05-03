CreateChoices <- function(dta, variable) {
  dta %>%
    dplyr::select({{ variable }}) %>%
    dplyr::pull() %>%
    unique() %>% 
    sort()
}
