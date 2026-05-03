SGD2=function(initial,data,rate=0.1,folds=20,Lambda=0,tolerance=0.0001,harmony_series=TRUE,
              save_path=file.path(Data_folder,paste0(as.character( Sys.info()["nodename"]),"_Theta_newest.rds"))){
  
  sign_past=sign(choice_score(data=data,parameter=initial,lambda=Lambda,alpha=0))
  Theta_old <- initial
  m=rep(1,length(initial))
  nr=rep(0,length(initial))
  
  last_save_time <- Sys.time()  # Initialize last save time
  
  save_theta <- function(theta) {
    saveRDS(theta, file = save_path)
  }
  
  check_and_save <- function(theta) {
    current_time <- Sys.time()
    if (as.numeric(difftime(current_time, last_save_time, units = "mins")) >= 1) {
      save_theta(theta)
      last_save_time <<- current_time  # Update the last save time
    }
  }
  
  grad <- choice_score(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  Theta_new <- Theta_old+(rate/(m+nr))*grad
  Theta_old <- Theta_new
  
  check_and_save(Theta_old)  # Save if 1 minute passed
  
  print(choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0))
  m=rep(2,length(initial))
  grad <- choice_score(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  Theta_new <- Theta_old+(rate/(m+nr))*grad
  Theta_old <- Theta_new
  
  check_and_save(Theta_old)  # Save if 1 minute passed
  
  print(choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0))

  tolik=choice_llik(data=data,parameter=Theta_old,lambda=Lambda,alpha=0)
  max=1000
  idx_loc <- which(colnames(data)== "idx")
  idx=length(unique(data[,idx_loc]))

  LEN <- c(1:idx)
  while(max>tolerance){
    sh_data <- data
    lik_old <- tolik
    fld <- createFolds(LEN, k = folds, list = TRUE, returnTrain = FALSE)
    for( j in 1:folds){
      seleccted_idx <- fld[1][[1]]
      idicator <- sh_data[,idx_loc]%in%seleccted_idx
      sub_data <- sh_data[idicator,]
      sign_now=sign( choice_score(data=sub_data,parameter=Theta_old,lambda=Lambda,alpha=0))
      nr=nr+(sign_now!=sign_past)
     # nr <- 1
      sign_past=sign_now
      grad <- choice_score(data=sub_data,parameter=Theta_old,lambda=Lambda,alpha=0)
      if(harmony_series){
        Theta_new <- Theta_old+(rate/(m+(nr)))*grad
      }else{
        Theta_new <- Theta_old+rate*grad
      }

      # max=max(abs(Theta_new-Theta_old))
      
      Theta_old <- Theta_new
      
    }
    
    check_and_save(Theta_old)  # Save if 1 minute passed
    
    #changing=(Likelihood_wider(Theta_new,data)-tolik)/abs(tolik)
    tolik=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
    print(tolik)


    max= choice_score(data=sub_data,parameter=Theta_new,lambda=Lambda,alpha=0) %>% abs() %>% max
    if(lik_old>tolik){
    #  break
      # Res=list()
      # Res$Parameter=Theta_new
      # Res$nr=nr
      # Res$Likelihood=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
      # Res$max=max
      # return(Res)
    }
     # print(max)
  }
  #print(max)
  Res=list()
  Res$Parameter=Theta_new
  Res$nr=nr
  Res$Likelihood=choice_llik(data=data,parameter=Theta_new,lambda=Lambda,alpha=0)
  Res$max=max
  save_theta(Theta_old)  # Save if 1 minute passed
  return(Res)
}
