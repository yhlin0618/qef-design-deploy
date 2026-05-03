SGD2=function(initial,data,rate=0.1,folds=20,Lambda=0,tolerance=0.0001){
  
  sign_past=sign(choice_score(data=data,parameter=initial,lambda=Lambda,alpha=0))
  Theta_old <- initial
  m=rep(1,length(initial))
  nr=rep(0,length(initial))
  grad <- choice_score(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  Theta_new <- Theta_old+(rate/(m+nr))*grad
  Theta_old <- Theta_new
  print(choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0))
  m=rep(2,length(initial))
  grad <- choice_score(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  Theta_new <- Theta_old+(rate/(m+nr))*grad
  Theta_old <- Theta_new
  print(choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0))

  tolik=choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  max=1000
  idx_loc <- which(colnames(data)== "idx")
  idx=length(unique(data[,idx_loc]))

  LEN <- c(1:idx)
  while(max>tolerance){
    sh_data <- data
    fld <- createFolds(LEN, k = folds, list = TRUE, returnTrain = FALSE)
    for( j in 1:folds){
      seleccted_idx <- fld[1][[1]]
      idicator <- sh_data[,idx_loc]%in%seleccted_idx
      sub_data <- sh_data[idicator,]
      sign_now=sign( choice_score(data=sub_data,parameter=Theta_old,lambda=Lambda,alpha=0))
      nr=nr+(sign_now!=sign_past)
      sign_past=sign_now
      grad <- choice_score(data=sub_data,parameter=Theta_old,lambda=Lambda,alpha=0)
      Theta_new <- Theta_old+(rate/(m+(nr)))*grad
      # max=max(abs(Theta_new-Theta_old))
      
      Theta_old <- Theta_new
      
    }
    #changing=(Likelihood_wider(Theta_new,data)-tolik)/abs(tolik)
    tolik=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
    print(choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0))
    if(tolik>0){
      print("Error occured! logLikelihood is positive.")
      Res=list()
      Res$Parameter=Theta_new
      Res$nr=nr
      Res$Likelihood=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
      Res$max=max
      return(Res)
    }
    max= choice_score(data=sub_data,parameter=Theta_new,lambda=Lambda,alpha=0) %>% abs() %>% max
   # print(max)
  }
  #print(max)
  Res=list()
  Res$Parameter=Theta_new
  Res$nr=nr
  Res$Likelihood=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
  Res$max=max
  return(Res)
}
