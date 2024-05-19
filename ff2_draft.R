library(tidyverse)
library(arrow)
library(haven)

#See how outstanding loans evolve for a single loan
getwd()
years = c(2011:2011)
months = c("01", "02", "03", "04", "05", "06", 
           "07", "08", "09", "10", "11", "12")
aux_count = 0
for (y in years){
  for (m in months){
    filename = paste0("SCR_", y, m, ".dta")
    df = read_dta(filename)
    
    df = df %>% 
      filter(loan_id == 15489)
    
    if(aux_count == 0){
      final = df
      aux_count = 1
    } else{ 
      final = rbind(final, df)
      }
  }
}
