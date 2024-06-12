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
#rais = read_parquet("ff2_rais_local.parquet")
rais = read_parquet("ff2_rais.parquet")
sample_firms = rais %>% 
  select(cnpj8) %>% 
  unique() %>% 
  pull()

#Open cdi interest rates dataset to be used later
#Merge with cdi dataset to create spread measure
setwd(data_path)
rates <- read_excel('cdi.xlsx',sheet='month') %>% 
  select(c("time_id", "taxa")) %>% 
  mutate(time_id = as.character(time_id),
         taxa = taxa/100) %>% 
  rename(yearmonth = time_id,
         cdi = taxa)


############################################################
#Construct loan rates based on new loans
############################################################
#We will look only at new contracts, but since we dont know if
#every new contract appears first in the database in the exact month
#it starts, we will do the following: 
#We construct a single file  with all the observations, for every period for
#all companies in our sample and then we look at the first month it appeared
#and take that information
#At the same time, we will construct another df with end-of-period info (dfend)

years = c(2005:2018)
months = c("01", "02", "03", "04", "05", "06", 
           "07", "08", "09", "10", "11", "12")
dfend = c()

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
      #Drop earmarked loans
      filter(loan_resource >= 100 & loan_resource < 200) %>% 
      #Fill empty non performing loans and outstanding loans with 0
      mutate(across(c("loan_outstanding", "loan_arrears_90_180_days",
                      "loan_arrears_over_180_days", "loan_losses"), 
                    ~ replace_na(.,0))) %>% 
      #transform loan base rate to decimal format
      mutate(loan_base_rate = loan_base_rate/100)
    
    #Save end of period (eop) information in December
    if(m == "12"){
      eop = df %>% 
        group_by(cnpj8) %>% 
        summarise(loan_outstanding = sum(loan_outstanding),
                  npl = sum(loan_arrears_90_180_days, 
                            loan_arrears_over_180_days, loan_losses)) %>% 
        ungroup() %>% 
        mutate(year = y)
      
      #Bind
      dfend = rbind(dfend, eop)
      rm(eop)
    }
    
    
    df = df %>% 
      #Create relationship metric
      mutate(rel_duration = difftime(loan_start_date, firm_bank_start_date,
                                     units = "days")) %>% 
      #Create Maturity metric
      mutate(maturity = difftime(loan_end_date, loan_start_date,
                                 units = "days")) %>%  
      #Create rating metric
      mutate(rating = case_when(loan_rating =="AA"~10,
                                loan_rating =="A"~9,
                                loan_rating =="B"~8,
                                loan_rating =="C"~7,
                                loan_rating =="D"~6,
                                loan_rating =="E"~5,
                                loan_rating =="F"~4,
                                loan_rating =="G"~3,
                                loan_rating =="H"~2,
                                T ~ NA))
    
    #Create date column and select info we want
    df = df %>% 
      mutate(year = y,
             yearmonth = paste0(year, m)) %>% 
      select(year, yearmonth, cnpj8, bank_id, loan_id, contract_value, 
             loan_outstanding, loan_base_rate, rel_duration, maturity, rating,
             loan_start_date)
    
    #Take only the first appearence of each loan on this file
    #just to guarantee we dont have duplicates in the same month
    df = df %>% 
      arrange(loan_start_date) %>% 
      group_by(loan_id, cnpj8, bank_id) %>% 
      summarise(across(everything(),first)) %>%
      ungroup() %>% 
      select(!c("loan_start_date"))
    
      
    
    if(count_aux==0){
      df_a <- df
      count_aux <- count_aux + 1
    }
    else{
      df_a <- rbind(df_a, df)
      count_aux <- count_aux + 1
    }
  }
  #Save just the first obs of each contract on this year
  df_a = df_a %>% 
    arrange(yearmonth) %>% 
    group_by(cnpj8, bank_id, loan_id) %>% 
    summarise(across(everything(),first)) %>%
    ungroup()
  
  #Saving year dataframe
  setwd(data_path)
  archive_name <- paste("SCR_",y,".parquet",sep="")
  write_parquet(df_a,archive_name)
  print(y)
  
  
}

rm(df,df_a)
gc()

#Bind yearly files
df = c()
for (y in years){
  setwd(data_path)
  archive_name <- paste0("SCR_",y,".parquet")
  df_a = read_parquet(archive_name)
  df = rbind(df, df_a)
}

rm(df_a)
gc()

#Merge with rates dataset to calculate spreads
df = df %>% 
  left_join(rates, by = "yearmonth", na_matches = "never")
rm(rates)

df = df %>% 
  mutate(spread = (1+loan_base_rate)/(1+cdi)-1)

#take the first observation of each loan
df = df %>% 
  arrange(yearmonth) %>% 
  group_by(loan_id, cnpj8, bank_id) %>% 
  summarise(across(everything(),first)) %>% 
  ungroup() %>% 
  select(!c("yearmonth"))

#aggregate by year and firm, use contract value as weights
df = df %>% 
  group_by(year, cnpj8) %>% 
  summarise(across(c("loan_base_rate", "spread",
                     "rel_duration", "maturity", "rating"),
                   ~weighted.mean(., contract_value, na.rm=T))) %>% 
  ungroup()

#Merge df with dfend and fill missing data of eop with 0
df = df %>% 
  left_join(dfend, by = c("year", "cnpj8"), na_matches = "never") %>% 
  mutate_at(c("loan_outstanding", "npl"), ~ replace_na(., 0))

rm(dfend)


#Merge with rais dataset
rais = rais %>% 
  left_join(df, by = c("cnpj8", "year"), na_matches = "never")

rm(df)

setwd(data_path)
write_parquet(rais, "ff2_after_scr.parquet")

print(str(rais))
print(summary(rais))

rm(rais)
gc()


