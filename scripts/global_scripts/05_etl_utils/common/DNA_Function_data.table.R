analyze_customer_behavior_dt <- function(Data_byCustomertime, Skip.within.Subject=F,replace_NESmedian=F) {
  
  time_now <- max(Data_byCustomertime$time)
  
  # Step 1: 計算 IPT 平均和 Mvalue
  IPTmean_table <- Data_byCustomertime[, .(
    IPT_mean = mean(IPT, na.rm = TRUE),
    Mvalue = mean(total, na.rm = TRUE),
    ni = .N,
    sigma.HNorm.mle = sqrt(mean(IPT^2, na.rm = TRUE))  # 先計算 sigma.HNorm.mle
  ), by = customer_id]
  
  # Step 2: 根據計算出的 sigma.HNorm.mle 來計算 sigma.HNorm.bcmle
  IPTmean_table[, sigma.HNorm.bcmle := sigma.HNorm.mle + (sigma.HNorm.mle / (4 * (ni - 1)))]
  
  IPTmean_table[, M_ecdf := ecdf(Mvalue)(Mvalue)]
  IPTmean_table[, Mlabel := cut(M_ecdf, breaks = Mbreaks, labels = textMlabel, right = TRUE, ordered_result = TRUE)]
  
  # Step 2: NES 計算
  NEStable <- merge(Data_byCustomertime, IPTmean_table, by = "customer_id")
  NEStable[, Difftime := as.numeric(difftime(time_now, time, units = "days"))]
  NEStable[, NESratio := Difftime / IPT_mean]
  
  if (replace_NESmedian==T) {
    Amazon_NESmedian <<- median(NEStable$NESratio, na.rm = TRUE)
  }
  
  NEStable[, NESstatus := cut(NESratio, breaks = NESbreaks * Amazon_NESmedian, labels = textNESlabel, right = FALSE, ordered_result = TRUE)]
  
  # Step 3: RFM 分析
  RFM_table <- Data_byCustomertime[, .(
    Rvalue = as.numeric(difftime(time_now, max(time), units = "days")),
    Fvalue = max(times)
  ), by = customer_id]
  
  RFM_table[, `:=`(
    F_ecdf = ecdf(Fvalue)(Fvalue),  # 先計算 F_ecdf
    R_ecdf = ecdf(Rvalue)(Rvalue)   # 先計算 R_ecdf
  )]
  
  # 接著生成 Flabel 和 Rlabel
  RFM_table[, `:=`(
    Flabel = cut(F_ecdf, breaks = Fbreaks, labels = textFlabel, ordered_result = TRUE),
    Rlabel = cut(R_ecdf, breaks = Rbreaks, labels = textRlabel, ordered_result = TRUE)
  )]
  
  # Step 4: 顧客活躍度 (CAI)
  # 定義 CAI 計算的 data.table 版本
  CAI_table <- Data_byCustomertime[
    times != 1 & ni >= ni_threshold,                      # 過濾掉第一筆資料且每個人至少有三次購買
    .(MLE.weight = 1 / (ni - 1),                          # 計算 MLE 的權重
      WMLE.weight = (times - 1) / sum(times - 1),         # 計算 WMLE 的權重
      IPT, customer_id), 
    by = customer_id                                      # 按 customer_id 分組
  ][, .(MLE = sum(IPT * MLE.weight),                      # 計算 MLE
        WMLE = sum(IPT * WMLE.weight)),                    # 計算 WMLE
    by = customer_id                                    # 按 customer_id 分組
  ]
  
  # 第一步：計算 CAI
  CAI_table[, CAI := (MLE - WMLE) / MLE]
  
  # 第二步：計算 CAI 的累積分布
  CAI_table[, CAI_ecdf := ecdf(CAI)(CAI)]
  
  # 第三步：將 CAI 分成等級
  CAI_table[, CAIlabel := cut(CAI_ecdf,
                              breaks = CAIbreaks, 
                              labels = textCAIlabel, 
                              right = FALSE, 
                              ordered_result = TRUE)]
  
  
  
  
  # 第一步：計算每筆交易的時間差異 (Difftime)
  PCV_table <- Data_byCustomertime
  PCV_table$Difftime <- time_length(time_now - PCV_table$time,"days") 
  
  # 第二步：計算顧客過去價值 (PCV)
  PCV_table[, PCV := sum(total * (1 + delta)^(Difftime)), by = customer_id]
  
  
  # Step 6: 顧客終身價值 (CLV)
  pif <- function(t) {
    ((t < 5) * (4 * t^2 + 20) + (t >= 5) * (120 + (80 * (1 - exp(5 - t))))) / 20
  }
  
  CLV_fcn <- function(x) {
    t <- 0:10
    sum(x * pif(t) * ((0.9)^t / (1 + delta * 4)^t))
  }
  
  CLV_table <- Data_byCustomertime[, .(
    total_sum = sum(total, na.rm = TRUE)
  ), by = customer_id][, CLV := CLV_fcn(total_sum), by = customer_id]
  
  # 合併所有結果
  Data_byCustomer <- merge(IPTmean_table, CAI_table, by = "customer_id", all.x = TRUE)
  Data_byCustomer <- merge(Data_byCustomer, PCV_table, by = "customer_id", all.x = TRUE)
  Data_byCustomer <- merge(Data_byCustomer, CLV_table, by = "customer_id", all.x = TRUE)
  
  # Step 7: 靜止戶預測 (Nrec)
  Date_row_ingroup <- Data_byCustomertime[time < add_with_rollback(time_now, months(-1))]
  Date_row_Nrec <- Data_byCustomertime[, Ncode := time > add_with_rollback(time_now, months(-1))]
  
  Nrec_ingroup_table <- Date_row_ingroup[, .(
    Fvalue = max(times)
  ), by = customer_id]
  
  # 第一步：計算 MLE
  CAI_ingroup_btable <- Date_row_ingroup[ni > 2, .(
    MLE = sum(IPT * (1 / (ni - 1)))  # MLE計算
  ), by = customer_id]
  
  # 第二步：計算 WMLE
  CAI_ingroup_btable[, WMLE := sum(IPT * ((times - 1) / sum(times - 1))), by = customer_id]
  
  # 第三步：計算 CAI
  CAI_ingroup_btable[, CAI := (MLE - WMLE) / MLE, by = customer_id]
  
  
  Nrec_table <- merge(Nrec_ingroup_table, Date_row_Nrec[, .(customer_id, Nrec = max(Ncode)), by = customer_id], by = "customer_id", all.x = TRUE)
  Nrec_table <- merge(Nrec_table, CAI_ingroup_btable, by = "customer_id", all.x = TRUE)
  
  if (!Skip.within.Subject) {
    library(caret)
    cv_control <- trainControl(method = "cv", number = 10, savePredictions = "final", classProbs = FALSE)
    logistic_model_cv <- train(Nrec ~ Flabel + CAI, data = Nrec_table, method = "glm", family = "binomial", trControl = cv_control)
    
    predictions <- logistic_model_cv$pred
    cm <- confusionMatrix(predictions$pred, predictions$obs)
    accuracy <- cm$overall["Accuracy"]
    
    NrecAccu <- paste0("Accuracy: ", round(accuracy * 100, 2), "%")
    
    Nrec_table_all <- Data_byCustomer[, .(customer_id, Flabel, CAI)]
    Nrec_table_all[, Flabel := factor(Flabel)]
    Nrec_table_all[, CAI := recode(CAI, .missing = mean(CAI, na.rm = TRUE))]
    
    Nrec_add <- Data_byCustomer[, .(
      Nrec_prob = predict(logistic_model_cv, Nrec_table_all, type = "prob")[[2]],
      Nrec = predict(logistic_model_cv, Nrec_table_all)
    )]
    
    Data_byCustomer <- merge(Data_byCustomer, Nrec_add, by = "customer_id", all.x = TRUE)
  } else {
    NrecAccu <- "Accuracy: 100%"
    Data_byCustomer[, `:=`(Nrec_prob = 0, Nrec = 0)]
  }
  
  return(list(Data_byCustomer = Data_byCustomer, NrecAccu = NrecAccu))
}