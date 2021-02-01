library(shiny)
library(shinydashboard)
library(DT)
library(plotly)
library(dplyr)
library(tidyr)
library(scales)
library(ggiraph)
library(reshape2)
library(data.table)

# Set working directory
setwd(stringr::str_replace(getwd(),"/shiny",""))
addResourcePath("workingdirectory", getwd())

# Fixing the labels
all_years <- list("Entire study period" = "all", 
               "Index year 2010" = "2010",
               "Index year 2011" = "2011",
               "Index year 2012" = "2012",
               "Index year 2013" =  "2013",
               "Index year 2014" = "2014", 
               "Index year 2015" = "2015",
               "Index year 2016" = "2016",
               "Index year 2017" = "2017")

all_populations <- list("Asthma > 18"= "asthma",
                        "COPD > 40" = "copd",
                        "ACO > 40" = "aco",
                        "Asthma 6-17" = "asthma6plus",
                        "Asthma < 5" = "asthma6min")

included_databases <- list("IPCI" = "IPCI",
                           "CPRD" = "CPRD",
                           "CCAE" = "ccae",
                           "MDCD" = "mdcd",
                           "MDCR" = "mdcr",
                           "AUSOM" = "AUSOM")
                    
characterization <- list()
stepupdown <- list()

for (d in included_databases) {
  try(characterization[[d]] <- read.csv(paste0(stringr::str_replace(getwd(),"/shiny",""), "/output/", d, "/characterization/characterization.csv")))
  
  stepupdown_d <- list()
  
  for (p in all_populations) {
    try(stepupdown_d[[p]] <- read.csv(paste0(stringr::str_replace(getwd(),"/shiny",""), "/output/", d, "/", p, "/",d , "_", p, "_augmentswitch.csv"))) 
    
    try(stepupdown[[d]] <- stepupdown_d)
  }
}

writeLines("Data Loaded")

