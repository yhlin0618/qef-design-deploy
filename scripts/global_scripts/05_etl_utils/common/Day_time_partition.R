Day_time_partition <- function(x){
  if(is.na(x)|x=="NA"|x=="character(0)"){
    #stop(NA)
    return(NA)
  }else{
    # 6:00 AM to 8:00 AM: Early risers
    # 8:01 AM to 12:00 PM: Working hours
    # 12:01 PM to 3:00 PM: Lunch break
    # 3:01 PM to 5:00 PM: Afternoon tea time
    # 5:01 PM to 7:00 PM: Evening commute
    # 7:01 PM to 9:00 PM: Dinner time
    # 9:01 PM to 12:00 AM: Evening hours
    # 12:01 AM to 6:00 AM: Night owls
    parsed_time <- as.POSIXct(x, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
    
   # parsed_time <- strptime(x, format = "%Y-%m-%d %H:%M:%S")
    
  #  parsed_time <- strptime(result$Local_time[1], format = "%Y-%m-%d %H:%M:%S")
    # 使用 strftime 格式化时间为 "%H:%M:%S"
    time_of_day <-format(parsed_time, "%H:%M:%S")

    #time_of_day <- format(x, "%H:%M:%S")
    # 创建时间段开始的向量
    Partition_start <- c("00:00:00", "06:00:01", "08:00:01", "12:00:01",
                         "15:00:01", "17:00:01", "19:00:01", "21:00:01")
    # 创建时间段结束的向量
    Partition_end <- c("06:00:00", "08:00:00", "12:00:00", "15:00:00", 
                       "17:00:00", "19:00:00", "21:00:00", "23:59:59")
    
    # Partition_start+as.POSIXct("06:00:00")
    
    D_time <- c("Nightowls","Earlyrisers","Workinghours","Lunchbreak",
                "Afternoonteatime","Evening commute","Dinnertime","Eveninghours" )
    for (i in 1:length(Partition_start)) {
      is_within_range <- time_of_day >= Partition_start[i] & time_of_day <= Partition_end[i]
      if(is_within_range){
      #  print(D_time[1])
        return(D_time[i])
        break
      }
    }
  }
  
}

Day_time_partition_vec <- Vectorize(Day_time_partition)
