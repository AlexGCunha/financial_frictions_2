#####################################################################
#This code will:
# - Take a sample of RAIS firms and construct some metrics, per year
# - Make the same thing for inspected firms
# - It needs ff2_inspected_firms_local and its output file to be sent 
#################D####################################################
options(file.download.method="wininet")
repository = "http://artifactory.bcnet.bcb.gov.br/artifactory/cran-remote/"

if (!require("dplyr")) install.packages("splitstackshape", repos = repository)
if (!require("haven")) install.packages("splitstackshape", repos = repository)
if (!require("readxl")) install.packages("splitstackshape", repos = repository)
if (!require("arrow")) install.packages("splitstackshape", repos = repository)
if (!require("stringr")) install.packages("splitstackshape", repos = repository)
if (!require("tibble")) install.packages("splitstackshape", repos = repository)
if (!require("tidyr")) install.packages("splitstackshape", repos = repository)
if (!require("ggplot2")) install.packages("splitstackshape", repos = repository)
if (!require("readr")) install.packages("splitstackshape", repos = repository)
if (!require("purrr")) install.packages("splitstackshape", repos = repository)

library(tidyverse)
library(haven)
library(readxl) 
library(arrow)

data_path = "Z:/Bernardus/Cunha_Santos_Doornik/Dta_files"
rais_path = "Z:/DATA/Dta_files/RAIS"
output_path = "Z:/Bernardus/Cunha_Santos_Doornik/Output_check"

############################################################
#Create sample of firms from RAIS files and inspected firms
############################################################
a = Sys.time()
years = c(2005:2018)
sample_firms = c()
for (y in years){
  #Open RAIS file
  setwd(rais_path)
  filename = paste0("RAIS_",y,".dta")
  rais = read_dta(filename)
  
  #Select only 10% of unique firm ids
  firms = rais %>% 
    mutate(cnpj8 = as.numeric(cnpj8)) %>% 
    select(cnpj8) %>% 
    unique() %>% 
    pull() 
  
  nfirms = round(0.1 * length(firms))
  firms = sample(firms, nfirms, replace = FALSE)
  
  #Bind to other years samples
  sample_firms = c(sample_firms, firms)
  gc()
  print(y)
    
}
rm(rais, nfirms, firms)
gc()

#Read inspected firms data
setwd(data_path)
inspected = read_parquet("inspected_firms.parquet")
inspected_firms = inspected %>% 
  select(cnpj8) %>% 
  pull() %>% 
  unique()

#add to our sample of firms and remove duplicates
sample_firms = c(sample_firms, inspected_firms)
sample_firms = unique(sample_firms)

b = Sys.time()
print("Time to generate sample of firms:")
print(b-a)

############################################################
#Construct metrics for these firms
############################################################
a = Sys.time()
#Useful mode function
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

aux_count = 0
for (y in years){
  setwd(rais_path)
  filename = paste0("RAIS_",y,".dta")
  rais = read_dta(filename)
  
  #Filter companies in our sample
  rais = rais %>%
    mutate(cnpj8 = as.numeric(cnpj8)) %>%
    filter(cnpj8 %in% sample_firms)

  #Modifications in some variables
  rais = rais %>% 
    mutate(wage = str_replace(wage_contr, ",", ".")) %>% 
    mutate_at(c('municipality',"wage","ind_cnae95"),as.integer) %>%
    #filter only individuals working in December
    filter(is.na(quit_reason) | quit_reason == "00") %>%
    group_by(cnpj8) %>% 
    summarise(munic_ibge = Mode(municipality),
              n_formal = length(unique(cpf)),
              wage_bill = sum(wage),
              cnae = Mode(ind_cnae95),
              ) %>% 
    ungroup() %>% 
    mutate(year = as.integer(y), 
           cnae2 = as.character(cnae),
           cnae2 = as.integer(substr(cnae2,1,2)),
           )
  
  #Bind
  if(aux_count == 0){
    df = rais
    aux_count = 1
  } else {
    df = rbind(df, rais)
    print(y)
  }
}

#Add inspection data for inspected firms
df = df %>% 
  left_join(inspected, by = c("cnpj8"), na_matches = "never")

#Note that the number of informal is a proxy for the year
#prior to the inspections, so lets impute NA for other years
df = df %>% 
  mutate(year_prior_inspection = year_decision - 1) %>% 
  mutate(n_informal = ifelse(year == year_prior_inspection, n_informal, NA_integer_))
  

b = Sys.time()
print("Time to create Rais database:")
print(b-a)

#Save
setwd(data_path)
write_parquet("ff2_rais.parquet")
rm(list = ls())
gc()

