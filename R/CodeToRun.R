
# Load packages
library(SqlRender)
library(dplyr)
library(highcharter)
library(ggplot2)
library(scales)
library(gridExtra)
library(data.table)
library(stringr)
library(glue)
library(DatabaseConnector)

source('~/RespiratoryDrugStudies/R/CreateCohort.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/Helper.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/Main.R', echo=TRUE)

# ------------------------------------------------------------------------
# Settings and database credentials
# ------------------------------------------------------------------------
user <- "amarkus"
password <- "amarkus"
cdmDatabaseSchemaList <- 'cdm'
cohortSchemaList <- 'results'
oracleTempSchema <- NULL
databaseList <- 'IPCI'
fftempdir <- paste0(getwd(),"/temp")

dbms <- "postgresql"
server <- "Res-Srv-Lin-01/IPCI-HI-LARIOUS"
port <- 5432
outputFolder <- paste0(getwd(),"/output")
options(fftempdir = fftempdir)

if (length(cdmDatabaseSchemaList) != length(cohortSchemaList) || length(cohortSchemaList) != length(databaseList)) {
  stop("The CDM, results and database lists match in length")
}

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
runTreatmentPathways <- TRUE
debug <- FALSE # Use this when you'd like to emit the SQL for debugging 
exportResults <- FALSE

study_settings <- data.frame(readr::read_csv("inst/Settings/study_settings.csv", col_types = readr::cols()))
study_settings <- study_settings[,1:2]

# ------------------------------------------------------------------------ 
# Run the study
# ------------------------------------------------------------------------

for (sourceId in 1:length(cdmDatabaseSchemaList)) {
  cdmDatabaseSchema <- cdmDatabaseSchemaList[sourceId]
  cohortDatabaseSchema <- cohortSchemaList[sourceId]
  databaseName <- databaseList[sourceId]
  databaseId <- databaseName
  databaseDescription <- databaseName
  
  print(paste("Executing against", databaseName))
  
  execute(
    connection = connection,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    oracleTempSchema = oracleTempSchema,
    outputFolder = outputFolder,
    databaseId = databaseId,
    databaseName = databaseName,
    runCreateCohorts = runCreateCohorts,
    runCohortCharacterization = runCohortCharacterization,
    runCheckCohorts = runCheckCohorts,
    runTreatmentPathways = runTreatmentPathways,
    debug = debug,
    exportResults = exportResults,
    debugSqlFile = debugSqlFile,
    study_settings = study_settings
  )
  }


