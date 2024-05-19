#####################################################################
#This code will:
# - Create loan metrics for companies in our sample
# - It needs ff2_prep_rais 
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
scr_path = "Z:/DATA/Dta_files/SCR"

#First, open rais sampled data to filter companies we want
setwd(data_path)
rais = read_parquet("ff2_rais.parquet")
sample_firms = rais %>% 
  select(cnpj8) %>% 
  unique() %>% 
  pull()

############################################################
#Construct loan rates based on new loans
############################################################
#We will look only at new contracts, but since we dont know if
#every new contract appears first in the database in the exact month
#it starts, we will do the following: 
#We construct a single file  with all the observations, for every period for
#all companies in our sample and then we look a the first month it appeared
#and take that information

years = c(2005:2018)
months = c("01", "02", "03", "04", "05", "06", 
           "07", "08", "09", "10", "11", "12")
for (y in years){
  count_aux = 0
  setwd(scr_path)
  for (m in months){
    filename <- paste("SCR_",y,m,".dta",sep="")
    df = read_dta(filename) %>% 
      filter(firm_id %in% sample_firms) %>% 
      rename(cnpj8 = firm_id)
    
    #cleaning
    df = df %>% 
      #Only prefixed contracts
      filter(loan_index_rate == 11) %>% 
      select(!loan_index_rate) %>% 
      #Drop contracts where start date is 1901
      mutate_at(c("loan_start_date","loan_end_date",
                  "firm_bank_start_date"),as.Date) %>% 
      mutate_at(c("loan_start_date","loan_end_date",
                  "firm_bank_start_date"),
                ~case_when(.<=as.Date("1jan1901",format='%d%b%Y')~as.Date(NA),
                           TRUE~.)) %>% 
      filter(!is.na(loan_start_date), !is.na(loan_end_date),
             !is.na(firm_bank_start_date)) %>% 
      select(!c("loan_start_date","loan_end_date",
                "firm_bank_start_date")) %>% 
      #Drop earmarked loans
      filter(loan_resource >= 100 & loan_resource < 200) %>% 
      #Take only the first appearence of each loan on this file
      arrange(loan_start_date) %>% 
      group_by(loan_id) %>% 
      summarise_all(first) %>% 
      ungroup()

    
    
    
    if(count_aux==0){
      df_a <- df
      count_aux <- count_aux + 1
    }
    else{
      df_a <- rbind(df_a, df)
      count_aux <- count_aux + 1
    }
  }
  #Saving year dataframe
  setwd(data_path)
  archive_name <- paste("SCR_",y,".parquet",sep="")
  write_parquet(df_a,archive_name)
  print(y)
  
  
}
summary(df_a)

rm(df,df_a)
gc()