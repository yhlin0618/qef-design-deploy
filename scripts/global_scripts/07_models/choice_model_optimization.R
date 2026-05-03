library(maxLik)
choice_model_MLE <- function(data,Ini,Lambda=0,printlevel=1){
  f <- function(theta){
    L <- choice_llik(data,theta,lambda=Lambda)
    return(L)
  }
  g <- function(theta){
    # G <- score_function(theta,data)
    G <- as.vector(choice_score(data,theta,lambda=Lambda))
    return(G)
  }

  M_f <- maxLik(logLik=f,grad=g,start=Ini,  control=list(printLevel=printlevel))
 # M_f <- maxLik(logLik=f,grad=g,start=M_f$estimate,tol=.Machine$double.eps)
  return(M_f)
}

#est_par <- choice_model_MLE(data=data_mat,Ini=par)

#data.table(Effect=rownames(choice_score(data_mat,est_par$estimate)),Estimate =est_par$estimate)

