DNA_Function_dplyr <- function(Data_byCustomertime, Skip.within.Subject=F,replace_NESmedian=F) {
  
  Data_byCustomertime <- as_tibble(Data_byCustomertime) %>% group_by(customer_id)
  
  Data_byCustomer <- Data_byCustomertime %>% 
    group_by(customer_id) %>% summarise()
  
  time_now <- max(Data_byCustomertime$time)
  
  #1 先算RFM
  #1.1. 平均間隔(IPT)與平均購物金額(M)
  IPTmean_table <- Data_byCustomertime %>%  
    group_by(customer_id)  %>% 
    summarise(IPT_mean=mean(IPT,na.rm = TRUE),                   #平均間隔時間
              Mvalue=mean(total,na.rm = TRUE),               #平均購物金額
              ni=n(),
              sigma.HNorm.mle=(mean(IPT^2,na.rm = TRUE)^0.5),
              sigma.HNorm.bcmle=sigma.HNorm.mle+(sigma.HNorm.mle/(4*(ni-1))),
              .groups="keep") %>%  #IPT只有ni-1個            
    ungroup() %>% mutate(M_ecdf = cume_dist(Mvalue),
                         Mlabel = cut(M_ecdf,
                                      Mbreaks,
                                      textMlabel,
                                      right=T,
                                      ordered_result = T)) 
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,IPTmean_table)
  
  #1.2 NES
  
  #挑選最近一次購物的資料 
  
  NEStable<- left_join(Data_byCustomertime,IPTmean_table,by="customer_id") %>% lazy_dt()%>% 
    slice(which.max(times)) %>% as_tibble()
  
  
  NEStable$IPT_mean <- as.difftime(NEStable$IPT_mean, units = "days")
  
  NEStable$Difftime <- difftime(time_now, NEStable$time, units = "days")
  
  NEStable$NESratio <- as.numeric(NEStable$Difftime)/as.numeric(NEStable$IPT_mean)
  
  if (replace_NESmedian==T){
    Amazon_NESmedian <<- median(NEStable$NESratio,na.rm = T)}
  
  
  NEStable$NESstatus <- fct_na_value_to_level(cut(NEStable$NESratio,
                                                  NESbreaks*Amazon_NESmedian,
                                                  textNESlabel,
                                                  right=F,
                                                  ordered_result = T),level = "N")
  
  NEStable$NESstatus <- ordered(NEStable$NESstatus,levels=c("N",textNESlabel))
  
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,NEStable)
  
  #table(Data_byCustomer$NESstatus)
  
  
  #1.2 近期購物情境(R)與購物頻率(F)
  RlabelandFlabel_table <- Data_byCustomertime%>% lazy_dt()  %>% 
    slice(which.max(times)) %>%        #挑選最近一次購物的資料 
    select(customer_id, times,time) %>%                                 #只把名字和次數拿出來
    ungroup() %>% as_tibble()
  
  RlabelandFlabel_table$Rvalue <- time_length(time_now - RlabelandFlabel_table$time,"days") 
  
  
  RlabelandFlabel_table2 <- RlabelandFlabel_table%>% lazy_dt() %>% 
    mutate(Fvalue = times,
           F_ecdf = cume_dist(Fvalue),
           Flabel0 = cut(F_ecdf,
                         c(0,0.2,0.8,1),
                         textFlabel,
                         right=F,
                         ordered_result = T),          #把購物頻率分成五等分
           Flabel = cut(Fvalue,
                        Fbreaks,
                        textFlabel,
                        ordered_result = T),          #把購物頻率分成五等分
           R_ecdf = cume_dist(Rvalue),
           Rlabel = cut(R_ecdf,
                        Rbreaks,
                        textRlabel,
                        right=F,
                        ordered_result = T)) %>%           #把最近購物情境分成五等分
    select(customer_id,Fvalue,F_ecdf,Flabel,Rvalue,R_ecdf,Rlabel) %>% as_tibble()
  
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,RlabelandFlabel_table2)
  
  #2. 顧客活躍度CAI
  
  CAI_table <- Data_byCustomertime  %>% 
    lazy_dt() %>% 
    mutate(MLE.weight=1/(ni-1),
           WMLE.weight=(times-1)/sum((times-1)))%>%     #產生WMLE的權重
    filter(times!=1)%>%                                 #需要把第一筆資料拿掉
    filter(ni>=ni_threshold) %>%  
    group_by(customer_id) %>%                     #每一個人都要三次
    summarise(MLE=sum(IPT*MLE.weight),
              WMLE=sum(IPT*WMLE.weight),
              .groups = "keep") %>% ungroup() %>% 
    mutate(CAI=(MLE - WMLE) / MLE,
           CAI_ecdf=cume_dist(CAI),
           CAIlabel=cut(CAI_ecdf,
                        CAIbreaks,
                        textCAIlabel,
                        right=F,
                        ordered_result = T)) %>% as_tibble()
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,CAI_table)
  

  #2. 顧客過去價值
  
  PCV_table <- Data_byCustomertime
  PCV_table$Difftime <- time_length(time_now - PCV_table$time,"days") 
  
  PCV_table <-  PCV_table %>%  
    group_by(customer_id) %>% 
    summarise(PCV=sum(total*(1+delta)^(Difftime)))
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,PCV_table)
  
  #8.顧客終身價值
  
  pif <- function(t){
    ((t<5)*(4*t^2+20)+(t>=5)*(120+(80*(1-exp(5-t)))))/20
  }
  
  CLV_fcn<- function(x){
    t=0:10
    sum(x*pif(0:10)*((0.9)^t/(1+delta*4)^t))
  }
  
  
  CLV_table <- Data_byCustomertime %>% 
    group_by(customer_id) %>% 
    summarise(total_sum=sum(total,na.rm = TRUE)) %>% 
    rowwise() %>% mutate(CLV=CLV_fcn(total_sum))   #代入CLV的函數
  
  #summerize的時候，只會保留group的變項，其他都會丟掉
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,CLV_table)
  
  # 3. 入店資歷與顧客新增率  & 客單價（NT）
  #顧客最早來的時間 time_First
  #最早來的時間到現在一共有多久 time_FirstTonow
  
  Data_byCustomerfirst <- Data_byCustomertime %>% 
    filter(times==1) %>% 
    rename(time_First=time,NT = total)%>% 
    select(customer_id,time_First,NT) 
  
  #N_table <- Data_row %>% slice(which.min(times)) %>% select(ID,total) %>% rename(NT=total)
  
  
  Data_byCustomerfirst$time_FirstTonow <- time_length(time_now - Data_byCustomerfirst$time_First,"days")  ##直接用時間相減很快，比用lubridate和mutate快多了
  
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,Data_byCustomerfirst)
  
  
  #5. 主力客單價
  
  E0T_table <- left_join(Data_byCustomertime,IPTmean_table,by="customer_id")
  
  E0T_table$IPT_mean <- as.difftime(E0T_table$IPT_mean, units = "days")
  
  E0T_table$Difftime <- difftime(time_now, E0T_table$time, units = "days")
  
  E0T_table$NESratio <- as.numeric(E0T_table$Difftime)/as.numeric(E0T_table$IPT_mean)
  
  E0T_table$NES <- cut(E0T_table$NESratio, NESbreaks,
                       textNESlabel,
                       right=F,
                       ordered_result = T) 
  
  E0T_table2 <- E0T_table  %>% filter(NES =="E0") %>%  #把他在E0的情況找出來
    group_by(customer_id) %>% summarise(E0T=mean(total,na.rm=T),.groups="keep") %>% #把主力客的平均算出來
    select(customer_id,E0T) #擷取資料
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,E0T_table2)
  
  # 12 交易穩定度
  
  CRI0<- IPTmean_table %>%  filter(ni>1)                                #只有ni>1才有IPT
  
  alpha <- mean(CRI0$ni)
  N <- nrow(CRI0)
  theta <- (sum((CRI0$IPT_mean)^-1))/(alpha*N)
  GE=1/((alpha-1)*theta)
  
  CRI_table <- CRI0 %>% 
    mutate(GE=GE,
           IE=IPT_mean,
           BE=(ni/(ni+alpha-1))*IE+((alpha-1)/(ni+alpha-1))*GE,
           CRI= abs(IE-BE)/abs(IE-GE),
           CRI_ecdf = cume_dist(CRI)) %>%
    select(customer_id,IE,BE,GE,CRI,CRI_ecdf)
  
  Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,CRI_table)
  
  
  # 13-1 靜止戶預測
  
  #找在「現在的日期的前一個日期」以前的資料
  Date_row_ingroup <- Data_byCustomertime[(Data_byCustomertime$time)< add_with_rollback(time_now, months(-1)),]
  
  Date_row_Nrec<- Data_byCustomertime
  Date_row_Nrec$Ncode = (Data_byCustomertime$time)> add_with_rollback(time_now, months(-1))
  
  Date_row_Nrec2<- Date_row_Nrec%>% 
    summarise(customer_id=first(customer_id),Nrec = max(Ncode))
  
  Nrec_ingroup_table <- Date_row_ingroup %>% 
    slice(which.max(times)) %>%        #挑選最近一次購物的資料 
    select(customer_id, times,time) %>%                                 #只把名字和次數拿出來
    ungroup()%>% mutate(Fvalue = times,
                        F_ecdf = cume_dist(Fvalue),          #把購物頻率分成五等分
                        Flabel = cut(Fvalue,
                                     Fbreaks,
                                     textFlabel,
                                     ordered_result = T)) %>%           #把最近購物情境分成五等分
    select(customer_id,Flabel)
  
  
  CAI_ingroup_btable <- Date_row_ingroup %>% mutate(MLE.weight=1/(ni-1),     #產生mle的權重
                                                    WMLE.weight=(times-1)/sum((times-1))) %>%         #產生WMLE的權重
    filter(times!=1)%>%                               #需要把第一筆資料拿掉
    filter(ni>2) %>%                                  #每一個人都要三次
    group_by(customer_id) %>% 
    summarise(MLE=sum(IPT*MLE.weight),
              WMLE=sum(IPT*WMLE.weight),
              CAI=(MLE - WMLE) / MLE,
              .groups="keep") 
  
  Nrec_table <- Date_row_ingroup %>% group_by(customer_id) %>% summarise(customer_id=first(customer_id))%>% 
    left_join(Nrec_ingroup_table,by="customer_id")%>% 
    left_join(CAI_ingroup_btable,by="customer_id") %>% 
    mutate(CAI=recode(CAI,.missing=mean(CAI_ingroup_btable$CAI))) %>%  # 如果有空缺就用平均的CAI代入
    left_join(Date_row_Nrec2,by="customer_id") %>% 
    select(-MLE, -WMLE) %>% 
    mutate(Nrec=factor(Nrec),Flabel=factor(Flabel))
  
  #library(caret)
  
  if (Skip.within.Subject==F){
    
    cv_control <- trainControl(method = "cv", number = 10, savePredictions = "final", classProbs = FALSE)
    
    logistic_model_cv <- train(Nrec ~ Flabel + CAI, data = Nrec_table, method = "glm", family = "binomial", trControl = cv_control)
    
    predictions <- logistic_model_cv$pred
    
    cm <- confusionMatrix(predictions$pred, predictions$obs)
    accuracy <- cm$overall["Accuracy"]
    
    NrecAccu <- paste0("Accuracy: ", round(accuracy * 100, 2), "%")
    
    # 13-2 靜止戶預測
    
    Nrec_table_all<- Data_byCustomer %>% select(customer_id,Flabel,CAI)%>% 
      mutate(Flabel=factor(Flabel)) %>% 
      mutate(CAI=recode(CAI,.missing=mean(Data_byCustomer$CAI,na.rm = T)))  # 如果有空缺就用平均的CAI代入
    
    
    Nrec_add<- Data_byCustomer %>% mutate(Nrec_prob=predict(logistic_model_cv, Nrec_table_all,type="prob")[[2]],
                                          Nrec=predict(logistic_model_cv, Nrec_table_all))
    
    Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,Nrec_add)
    
  } else{
    
    NrecAccu <- paste0("Accuracy: ", round(1 * 100, 2), "%")
    
    # 13-2 靜止戶預測
    
    Nrec_add<- Data_byCustomer %>% mutate(Nrec_prob=0,
                                          Nrec=0)
    
    Data_byCustomer <- left_join_remove_duplicate(Data_byCustomer,Nrec_add)
    
    
  }
  
  NrecAccu <- data.frame(NrecAccu=NrecAccu)

  return(list(Data_byCustomer = Data_byCustomer, NrecAccu = NrecAccu))
}





##Example
# library(data.table)
# library(tidyverse)
# source("./scripts/global_scripts/global_parameters.R")
# source("./scripts/global_scripts/global_path_settings.R")
# source("./scripts/global_scripts/left_join_remove_duplicate.R")
# Amazon_NESmedian <- 2.9
# exampledata <- readRDS(file.path(Data_folder,"Amazon_Sales_Data_byCustomertime_000_All_All_Full_Dta.rds"))
# DNA_Function_dplyr(exampledata)
