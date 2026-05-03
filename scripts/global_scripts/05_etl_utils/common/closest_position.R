closest_position_name <- function(transaction, NameList) {
  # 先檢查是否有完全相同的名稱
  if (transaction %in% NameList) {
    return(transaction)
  }
  
  # 如果沒有完全匹配，則使用Levenshtein距離計算最近的匹配
  distances <- stringdist::stringdist(transaction, NameList, method = "lv")
  
  # 找到距離最小的位置，並返回對應的 NameList 中的名稱
  closest_position <- NameList[which.min(distances)]
  return(closest_position)
}