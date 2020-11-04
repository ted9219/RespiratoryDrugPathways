
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
runTreatmentPathways <- FALSE
outputResults <- FALSE

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
    runTreatmentPathways = runTreatmentPathways,
    outputResults = outputResults,
    study_settings = study_settings
  )
  }


