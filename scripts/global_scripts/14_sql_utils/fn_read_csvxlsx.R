# Function to read CSV or Excel files based on file extension
read_csvxlsx <- function(file_name) {
  ext <- tools::file_ext(file_name)
  if (ext == "csv") {
    df <- suppressMessages(read_csv(file_name))
  } else if (ext %in% c("xlsx", "xls")) {
    df <- suppressMessages(read_excel(file_name))
  } else {
    message(glue("Unsupported file format: {file_name}"))
    stop("Unsupported file format")
  }
  return(df)
}