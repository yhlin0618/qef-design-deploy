# 設定資料夾路徑
Data_folder <- file.path("..","data")

# 檢查資料夾是否存在
if (!dir.exists(Data_folder)) {
  # 資料夾不存在，則創建它
  dir.create(Data_folder, recursive = TRUE)
  print(paste("資料夾創建於:", Data_folder))
} else {
  print(paste("資料夾已存在於:", Data_folder))
}


Product_List <- c("000_All",
                  "001_Electric_Can_Opener",
                  "002_Milk_Frother",
                  "003_Salt_and_Pepper_Grinder",
                  "004_Silicone_Spatula",
                  "005_Meat_Claw",
                  "006_Pastry_Brush",
                  "007_Bottle_Openers")

ProductLine_to_ProductLineID <- list("000_All"="0",
                                     "001_Electric_Can_Opener"="1",
                                     "002_Milk_Frother"="2",
                                     "003_Salt_and_Pepper_Grinder"="3",
                                     "004_Silicone_Spatula"="4",
                                     "005_Meat_Claw"="5",
                                     "006_Pastry_Brush"="6",
                                     "007_Bottle_Openers"="7")



Product_List_Chinese <- c("宏觀",
                  "電動開罐器",
                  "奶泡器",
                  "研磨罐",
                  "鍋鏟",
                  "撕肉爪",
                  "油刷",
                  "開瓶器")


Product_List_ToEnglish <- c(Electric_Can_Opener="001_Electric_Can_Opener",
                            Milk_Frother="002_Milk_Frother",
                            Salt_and_Pepper_Grinder="003_Salt_and_Pepper_Grinder",
                            Silicone_Spatula="004_Silicone_Spatula",
                            Meat_Claw="005_Meat_Claw",
                            Pastry_Brush="006_Pastry_Brush",
                            Bottle_Openers="007_Bottle_Openers")

Product_List_ToChinese <- c(Electric_Can_Opener="001_Electric_Can_Opener",
                            Milk_Frother="002_Milk_Frother",
                            Salt_and_Pepper_Grinder="003_Salt_and_Pepper_Grinder",
                            Silicone_Spatula="004_Silicone_Spatula",
                            Meat_Claw="005_Meat_Claw",
                            Pastry_Brush="006_Pastry_Brush",
                            Bottle_Openers="007_Bottle_Openers")

Product_List_NotAll<- Product_List[-1]

Used_Product_List <- Product_List_NotAll[c(1,2,3,4,5,6,7)]

Product_List_NotAll_Official_Website_Poisson <- setdiff(Product_List_NotAll,
                                                        c("003_Salt_and_Pepper_Grinder","005_Meat_Claw"))



IndextoASIN <- c(ASIN="product_Index",
                 Lineproduct.Price="品項價格",
                 Brand="品牌")

weekdays_names <- c(Sunday="星期日",
                    Monday="星期一",
                    Tuesday="星期二",
                    Wednesday="星期三",
                    Thursday="星期四",
                    Friday="星期五",
                    Saturday="星期六")

Not_In_Property <- c("sku","排名","asin","sku","brand","time","sales","product_name","lineproduct_price","排名","網址","銷售平台")

Chinesename <- c(Rating="顧客評論評分",
                 RatingNum="顧客評論則數",
                 width="產品寬度",
                 length="產品長度",
                 logprice="對數價格",
                 logprice2="對數價格平方")


#Revenue

source_vec <- c("amazon","officialwebsite")

timenowvec <- c("now","m1year","m1quarter","m1month")

timeSelect <- list(Month=paste("month",1:12,sep="_"),
                   Holiday=c("Christmas.Day","New.Year.s.Eve"),
                   Year=c("year"),
                   Daytime=c("Day_time_Afternoonteatime","Day_time_Dinnertime","Day_time_Earlyrisers",
                             "Day_time_Evening.commute","Day_time_Eveninghours","Day_time_Lunchbreak",
                             "Day_time_Nightowls","Day_time_Workinghours"),
                   Weekdays=c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"))

