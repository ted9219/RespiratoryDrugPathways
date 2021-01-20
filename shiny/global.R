library(shiny)
library(shinydashboard)
library(DT)
library(plotly)
library(dplyr)
library(tidyr)
library(scales)
library(ggiraph)

source("PlotsAndTables.R")

# Fixing the labels
# indications <- sort(as.list(unique(table1a %>% filter(variable=="indication") %>% select(value)))$value)
# indications <- append(indications,"All")
# formulations <- sort(as.list(unique(table1a %>% filter(variable=="formulation") %>% select(value)))$value)
# formulations <- append(formulations,"All")
# ingredients <- sort(unique(table1a$ingredient))
# analyses <- data.frame(analysisId=c(1,2,3,4,5,6,7,8),analysisName=c('Drug Exposure (days)','PDD/DDD Ratio','Cumulative DDD','Cumulative Dose (mg)','Cumulative annual dose (mg/PY)','Indications','Renal Impairment','Observation Period'))

# Sort selectors
# databases <- database[order(database$databaseId),]
# analyses <- analyses[order(analyses$analysisId), ]

addResourcePath("workingdirectory", stringr::str_replace(getwd(),"/shiny",""))

writeLines("Data Loaded")

