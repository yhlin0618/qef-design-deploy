Design_gen <- function(data,remove_name,remain_name=NULL){
  unit_equation = "克:盎司:公斤:Kilograms=1:28.3:1000:1000;毫升:公升=1:1000"
  data_trans <- select(data,-any_of(remove_name)) %>% 
    convert_units_to_smallest(., unit_equation)  %>% generate_dummy_matrix(., "ASIN",remain_name=remain_name) 
  ## preallocate for the interaction processing]
  data_out <- data_trans
  return(data_out)
}
