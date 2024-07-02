#####################################################################
#This code will:
# - Take a sample of RAIS firms and construct some metrics, per year
# - Make the same thing for inspected firms
# - It needs ff2_inspected_firms_local and its output file to be sent 
#####################################################################
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
rais_teradata_path = "Z:/DATA/Dta_files/RAIS_TERADATA"
output_path = "Z:/Bernardus/Cunha_Santos_Doornik/Output_check"

# local_rais = 'C:/Users/xande/OneDrive/Documentos/Doutorado/RA/Household Debt/Data'
# local_teradata = 'C:/Users/xande/OneDrive/Documentos/Doutorado/RA/UBI-Informality/SD/Data/rais_teradata'
# local_data = 'C:/Users/xande/OneDrive/Documentos/Doutorado/RA/Financial_Frictions/Data'

years = c(2005:2020)

############################################################
#Create sample of firms from RAIS files and inspected firms
############################################################
# a = Sys.time()
# sample_firms = c()
# for (y in years){
#   #Open RAIS file
#   if(y <= 2016){
#     setwd(rais_path)
#     filename = paste0("RAIS_",y,".dta")
#     rais = read_dta(filename)
#   } else {
#     setwd(rais_teradata_path)
#     filename = paste0("RAIS_TERADATA_",y,"12.dta")
#     rais = read_dta(filename)
#   }
#   
#   
#   #Select only 10% of unique firm ids
#   firms = rais %>% 
#     mutate(cnpj8 = as.numeric(cnpj8)) %>% 
#     select(cnpj8) %>% 
#     unique() %>% 
#     pull() 
#   
#   nfirms = round(0.1 * length(firms))
#   firms = sample(firms, nfirms, replace = FALSE)
#   
#   #Bind to other years samples
#   sample_firms = c(sample_firms, firms)
#   gc()
#   print(y)
#     
# }
# rm(rais, nfirms, firms)
# gc()
# 
# #Read inspected firms data
# setwd(data_path)
# inspected = read_parquet("inspected_firms.parquet")
# inspected_firms = inspected %>% 
#   select(cnpj8) %>% 
#   pull() %>% 
#   unique()
# 
# #add to our sample of firms and remove duplicates
# sample_firms = c(sample_firms, inspected_firms)
# sample_firms = unique(sample_firms)
# 
# b = Sys.time()
# message = paste0("Time to generate sample of firms: ", 
#                  difftime(b, a, units = "mins"), "minutes")
# print(message)

############################################################
#Construct metrics for these firms
############################################################
a = Sys.time()
#open cnae conversion file to be used later
setwd(data_path)
conv = read_csv("cnae_conversao.csv")

#Useful mode function
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

aux_count = 0
for (y in years){
  #Open RAIS file
  if(y <= 2016){
    setwd(rais_path)
    filename = paste0("RAIS_",y,".dta")
    rais = read_dta(filename)
  } else {
    setwd(rais_teradata_path)
    filename = paste0("RAIS_TERADATA_",y,"12.dta")
    rais = read_dta(filename)
    
  }
  
  #Filter companies in our sample
  rais = rais %>%
    # filter(cnpj8 %in% sample_firms) %>% 
    mutate(cnpj8 = as.numeric(cnpj8))
  
  #Print a summary for the year 2016
  if(y == 2016){
    print(str(rais))
    print(summary(rais))
  }
    
  #convert cnae 2.0 to cnae 1.0 from 2016 onward
  if(y >= 2016 & !('ind_cnae95' %in% colnames(rais))){
    rais = rais %>% 
      #its on numeric format, but the 0 before the number is important for
      # cnae classification, so lets inpute that where it's missing
      mutate(ind_sub_cnae20 = as.character(ind_sub_cnae20),
             nchar_cnae = nchar(ind_sub_cnae20)) %>%
      mutate(ind_sub_cnae20 = case_when(
        nchar_cnae == 7 ~ ind_sub_cnae20,
        T ~ paste0("0", ind_sub_cnae20))
      ) %>% 
      mutate(ind_cnae20 = substr(ind_sub_cnae20,1,5))
    
    #get cnae 1.0 (cnae95) values
    rais = rais %>% 
      left_join(conv, by = "ind_cnae20", na_matches = "never")
    
    #print(colnames(rais))
  }

  #Modifications in some variables
  rais = rais %>% 
    mutate(wage = str_replace(wage_contr, ",", "."),
           wage = as.integer(wage)) %>% 
    mutate_at(c('municipality',"wage","ind_cnae95"),as.integer) %>%
    #filter only individuals working in December
    mutate(quit_reason = as.character(quit_reason)) %>% 
    filter(is.na(quit_reason) | quit_reason == "00" | quit_reason == "-1") %>%
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
  mutate(n_informal = ifelse(year == year_prior_inspection, n_informal, 
                             NA_integer_))
  

b = Sys.time()
message = paste0("Time to create RAIS database: ", 
                 difftime(b, a, units = "mins"), "minutes")
print(message)

#Print total formal employment per year
for (y in years){
  formal = df %>% 
    filter(year == y) %>% 
    summarise(value = sum(n_formal)) %>% 
    select(value) %>% 
    pull()
  
  print(paste0("Formal employment in ", y, ": ", formal))
}

#Save
setwd(data_path)
write_parquet(df, "ff2_rais.parquet")
rm(list = ls())
gc()

