
# Load packages
library(SqlRender)
library(dplyr)
library(highcharter)
library(ggplot2)
library(scales)
library(gridExtra)
library(data.table)
library(stringr)

source('~/RespiratoryDrugStudies/R/TreatmentPathway.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/CreateCohort.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/Helper.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/Main.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/PullCohort.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/UsagePatternGraph.R', echo=TRUE)
source('~/RespiratoryDrugStudies/R/CycleIncidencePlot.R', echo=TRUE)

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
addIndex <- TRUE  # Use this for PostgreSQL and other dialects that support creating indices
runSunburstPlot <- TRUE
runIncidencePrevalance <- FALSE
runTreatmentPathways <- FALSE
debug <- FALSE # Use this when you'd like to emit the SQL for debugging 
exportResults <- FALSE

minCellCount <- 5

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
    runSunburstPlot = runSunburstPlot,
    addIndex = addIndex,
    runIncidencePrevalance = runIncidencePrevalance,
    runTreatmentPathways = runTreatmentPathways,
    debug = debug,
    exportResults = exportResults,
    debugSqlFile = debugSqlFile,
    minCellCount = minCellCount
  )
  }


