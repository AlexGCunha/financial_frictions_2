#------------------------------------------------------------------------------------------------
#This code will:
#- Work on inspections original dataset to simplify information
#- It will generate a df with 3 columns: cnpj, date and inspections
#- It only needs my original file to run
#------------------------------------------------------------------------------------------------

library(tidyverse)
library(haven)
library(arrow)

setwd("C:/Users/xande/OneDrive/Documentos/Doutorado/RA/Financial_Frictions/thaline data")
df = read_stata("firm_inspections.dta")

df <- df %>% 
  filter(firm==1, !is.na(cnpj8)) %>% 
  mutate(inspected = 1,
         year_decision = format(date_decision, "%Y"),
         year_decision = as.numeric(year_decision), 
         n_informal = round(fine/200)) %>% 
  select(cnpj8, year_decision, inspected, n_informal) %>% 
  mutate_at(c("year_decision", "inspected", "n_informal"), as.integer)


#save
setwd("C:/Users/xande/OneDrive/Documentos/Doutorado/RA/Financial_Frictions/Data")
write_parquet(df,"inspected_firms.parquet")
