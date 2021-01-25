library(shiny)
library(shinydashboard)
library(DT)
library(plotly)
library(dplyr)
library(tidyr)
library(scales)
library(ggiraph)
library(reshape2)

# Set working directory
setwd(stringr::str_replace(getwd(),"/shiny",""))
addResourcePath("workingdirectory", getwd())

# Fixing the labels
all_years <- c("all", 
               "2010",
               "2011",
               "2012",
               "2013",
               "2014", 
               "2015",
               "2016",
               "2017")

all_populations <- list("Asthma > 18"= "asthma",
                        "COPD > 40" = "copd",
                        "ACO > 40" = "aco",
                        "Asthma 6-17" = "asthma6plus",
                        "Asthma < 5" = "asthma6min")

included_databases <- list("IPCI" = "IPCI")
                    
characterization <- list()
for (d in included_databases) {
  characterization[[d]] <- read.csv(paste0(stringr::str_replace(getwd(),"/shiny",""), "/output/", included_databases[[d]], "/characterization/characterization.csv"))
}

writeLines("Data Loaded")

