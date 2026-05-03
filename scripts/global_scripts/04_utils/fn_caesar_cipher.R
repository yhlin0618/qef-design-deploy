#' Caesar Cipher Encoding Function
#'
#' @title Simple Caesar Cipher Implementation
#' @description Implements a basic Caesar cipher for encoding lowercase alphabetic
#' strings. The Caesar cipher is a substitution cipher where each letter in the
#' plaintext is shifted a certain number of places down the alphabet. This
#' implementation handles only lowercase letters and applies modulo arithmetic
#' to wrap around the alphabet.
#'
#' @param input_string Character string to encode. Will be converted to lowercase
#'        before encoding. Non-alphabetic characters may produce unexpected results.
#' @param shift Integer value specifying the number of positions to shift each
#'        letter in the alphabet. Default is 3 (classic Caesar cipher). Negative
#'        values shift backwards.
#'        
#' @return Character string with each letter shifted by the specified amount.
#'         Only works correctly with lowercase alphabetic characters (a-z).
#'         
#' @examples
#' # Classic Caesar cipher with shift of 3
#' caesar_cipher("hello", 3)  # Returns "khoor"
#' 
#' # Decrypt by using negative shift
#' caesar_cipher("khoor", -3) # Returns "hello"
#' 
#' # Custom shift value
#' caesar_cipher("abc", 1)     # Returns "bcd"
#' caesar_cipher("xyz", 1)     # Returns "yza" (wraps around)
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle MP047 Functional programming (pure function)
#' @note This is a simple educational implementation. For actual encryption,
#'       use proper cryptographic libraries.
caesar_cipher <- function(input_string, shift = 3) {
  # 將所有字母轉換為小寫（或大寫，視需求而定）
  input_string <- tolower(input_string)
  
  # 將輸入字符串轉換為 ASCII 數字
  ascii_values <- utf8ToInt(input_string)
  
  # 計算轉換後的 ASCII 值
  shifted_values <- (ascii_values - 97 + shift) %% 26 + 97
  
  # 將轉換後的 ASCII 值轉換回字符
  encoded_string <- intToUtf8(shifted_values)
  
  return(encoded_string)
}
