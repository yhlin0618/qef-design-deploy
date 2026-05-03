

#############################Global parameters###############################

max_nreviews_per_asin <- 30

####回溯時間區段設定

timenow_vec <- c("now","m1year","m1quarter","m1month")


state_dictionary <- data.frame(
  name = c("ALL","Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
           "Connecticut", "Delaware", "District of Columbia", "Florida", 
           "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", 
           "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", 
           "Massachusetts", "Michigan", "Minnesota", "Mississippi", 
           "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
           "New Jersey", "New Mexico", "New York", "North Carolina", 
           "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania","Puerto Rico",
           "Rhode Island", "South Carolina", "South Dakota", "Tennessee", 
           "Texas", "Utah", "Vermont", "Virginia", "Washington", 
           "West Virginia", "Wisconsin", "Wyoming"),
  abbreviation = c("ALL","AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", 
                   "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", 
                   "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", 
                   "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA","PR", "RI", 
                   "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", 
                   "WI", "WY")
)


len <- 120
P_size <- 4
t_size=5

##paramter settings of Customer DNA

#interest rate
r=1.085  #percentage
delta=(r/365)*0.01
retention=0.9

#CAI ##[1,2,3] = [漸趨靜止客戶,穩定消費客戶,漸趨活躍客戶]

ni_threshold = 4  
CAIbreaks = c(0,0.1,0.9,1)   
textCAIlabel = c("漸趨靜止客戶", "穩定消費客戶", "漸趨活躍客戶")

nFlabel = 3     ##[1,2,3] = [低頻買家（一次）,中頻買家（兩次）,高頻買家（>三次）]
nRlabel = 3     ##[1,2,3] = [長期未購買,中期未購買,最近買家] 20,80
nMlabel = 3     ##[1,2,3] = [低購買買家,中購買買家,高購買買家] 20,80

textFlabel = c("低頻買家（一次）","中頻買家（兩次）","高頻買家（>三次）")
textRlabel = c("長期未購買","中期未購買","最近買家")
textMlabel = c("低購買買家","中購買買家","高購買買家")

dq=0.0001
Fbreaks = c(0-dq,1.1,2.1,Inf)   ##這個是用次數切的
Rbreaks = c(0-dq,0.1-dq,0.9+dq,1+dq) 
Mbreaks = c(0-dq,0.1-dq,0.9+dq,1+dq)   


NESbreaks = c(0,1, 2, 2.5, Inf)
textNESlabel = c("E0","S1","S2","S3")


#NESratio median = 2.122416
#N: 新客
#E0  :  0*median < NESratio < 1*median
#S1  :  1*median < NESratio < 2*median
#S2  :  2*median < NESratio < 2.5*median
#S3  :  3*median < NESratio 
