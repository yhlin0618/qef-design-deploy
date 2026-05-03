remove_elements <- function(vector, elements) {
  vector <- vector[!tolower(vector) %in% tolower(elements)]
  return(vector)
}
