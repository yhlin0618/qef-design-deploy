save_list_elements <- function(my_list, save_path) {
  # 如果保存路径不存在，创建路径
  if (!dir.exists(save_path)) {
    dir.create(save_path, recursive = TRUE)
  }
  
  # 遍历列表中的每个元素并保存
  lapply(names(my_list), function(name) {
    # 获取列表元素
    data <- my_list[[name]]
    
    # 构造完整的文件路径，检查是否已经包含 .rds 后缀
    file_name <- ifelse(grepl("\\.rds$", name), name, paste0(name, ".rds"))
    file_path <- file.path(save_path, file_name)
    
    # 保存为 .rds 文件
    saveRDS(data, file_path)
    
    paste0("data: ",name," is saved")
  })
}
