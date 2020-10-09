
# Load packages (if necessary: install)
library(SqlRender)
library(dplyr)
library(ggplot2)
library(scales)
library(gridExtra)
library(data.table)
library(stringr)
library(glue)
library(DatabaseConnector)
library(readr)
library(networkD3)
library(tidyr)

# Change to project folder
setwd('todo')

source(paste(getwd(), '/R/CreateCohort.R', sep = ""), echo=TRUE)
source(paste(getwd(), '/R/Helper.R', sep = ""), echo=TRUE)
source(paste(getwd(), '/R/Main.R', sep = ""), echo=TRUE)

# ------------------------------------------------------------------------
# Settings and database credentials
# ------------------------------------------------------------------------
user <- 'todo'
password <- 'todo'
cdmDatabaseSchemaList <- 'todo'
cohortSchema <- 'todo'
oracleTempSchema <- NULL
databaseList <- 'todo'
fftempdir <- paste0(getwd(),"/temp")

dbms <- 'todo'
server <- 'todo'
port <- 'todo'
outputFolder <- paste0(getwd(),"/output")
options(fftempdir = fftempdir)

# Connect to the server
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = password,
                                                                port = port)

connection <- DatabaseConnector::connect(dbms = dbms,connectionDetails = connectionDetails)

# ------------------------------------------------------------------------ 
# Hard-coded settings
# ------------------------------------------------------------------------

## Analysis Settings
debugSqlFile <- "resp_drug_study.dsql"
cohortTable <- "resp_drug_study_cohorts"

runCreateCohorts <- FALSE
runCohortCharacterization <- FALSE
runCheckCohorts <- FALSE
runTreatmentPathways <- FALSE
exportResults <- FALSE

study_settings <- data.frame(readr::read_csv("inst/Settings/study_settings.csv", col_types = readr::cols()))

# ------------------------------------------------------------------------ 
# Run the study
# ------------------------------------------------------------------------

for (sourceId in 1:length(cdmDatabaseSchemaList)) {
  cdmDatabaseSchema <- cdmDatabaseSchemaList[sourceId]
  cohortDatabaseSchema <- cohortSchema
  databaseName <- databaseList[sourceId]
  databaseId <- databaseName
  databaseDescription <- databaseName
  
  print(paste("Executing against", databaseName))
  
  outputFolderDB <- paste0(outputFolder, "/", databaseName)
  
  execute(
    connection = connection,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    oracleTempSchema = oracleTempSchema,
    outputFolder = outputFolderDB,
    databaseId = databaseId,
    databaseName = databaseName,
    runCreateCohorts = runCreateCohorts,
    runCohortCharacterization = runCohortCharacterization,
    runCheckCohorts = runCheckCohorts,
    runTreatmentPathways = runTreatmentPathways,
    exportResults = exportResults,
    debugSqlFile = debugSqlFile,
    study_settings = study_settings
  )
  }


