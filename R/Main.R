
execute <- function(connection = NULL,
                    connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema = cdmDatabaseSchema,
                    cohortTable = "cohort",
                    oracleTempSchema = cohortDatabaseSchema,
                    outputFolder,
                    databaseId = "Unknown",
                    databaseName = "Unknown",
                    runCreateCohorts = TRUE,
                    runCohortCharacterization = FALSE,
                    runCheckCohorts = TRUE,
                    runSunburstPlot,
                    runIncidencePrevalance = FALSE,
                    runTreatmentPathways = FALSE,
                    exportResults = TRUE,
                    addIndex = FALSE,
                    selfManageTempTables = TRUE,
                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                    cdmDrugExposureSchema = cdmDatabaseSchema,
                    drugExposureTable = "drug_exposure",
                    cdmObservationPeriodSchema = cdmDatabaseSchema,
                    observationPeriodTable = "observation_period",
                    cdmPersonSchema = cdmDatabaseSchema,
                    personTable = "person",
                    minCellCount = 5,
                    debug = FALSE,
                    debugSqlFile = "") {
  
  # Input checks
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  if (!is.null(getOption("fftempdir")) && !file.exists(getOption("fftempdir"))) {
    warning("fftempdir '", getOption("fftempdir"), "' not found. Attempting to create folder")
    dir.create(getOption("fftempdir"), recursive = TRUE)
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  # Create cohorts: either predefined in ATLAS or using custom concept sets created in SQL inserted into template
  if (runCreateCohorts) {
    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    # on.exit(ParallelLogger::unregisterLogger("DEFAULT"))
    
    ParallelLogger::logInfo("Creating cohorts")
    createCohorts(connection = connection,
                  connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                  cohortTable = cohortTable,
                  # addIndex = addIndex,
                  oracleTempSchema = oracleTempSchema,
                  outputFolder = outputFolder)
  }
  
  if (runCohortCharacterization) {
    
    
    
  }
  
  if (runCheckCohorts) {
    
    # Number of concepts in each cohort
    # Number of overlapping concepts between cohorts
    # Total exposure counts in database (in a specified period)
    # Validate cohorts with concept information (description + dose form etc.)
    
  }
  
  if (runSunburstPlot) {
    pathToCsv <- "output/cohort.csv"
    cohortIds <- readr::read_csv(pathToCsv, col_types = readr::cols())
    targetCohortId <- cohortIds[cohortIds$cohortType == "target", ]$cohortId
    
    select <- "mono"
    outcomeCohortIds <- cohortIds[cohortIds$cohortType == "outcome" & grepl(select, cohortIds$cohortName), ]$cohortId
    
    cohortIds$cohortName <- paste("'", cohortIds$cohortName, "'")
    labels <-  apply(cohortIds[,1:2],1, paste, collapse = ",")
    labels <- paste("(", paste(str_replace_all(labels, select, ""), collapse = "),("), ")")
    
    studyFile <- renderStudySpecificSql2(minCellCount, cdmDatabaseSchema, cohortDatabaseSchema, targetCohortId, outcomeCohortIds, cohortTable, labels)
    
    # TODO: check sometimes an "regular expression is invalid UTF-8" error/warning that does not cause problems but appears
    conn <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(conn,SqlRender::readSql(studyFile))
    
    extractAndWriteToFile(conn, tableName = "summary", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, "txpath")
    extractAndWriteToFile(conn, tableName = "person_cnt", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, "txpath")
    extractAndWriteToFile(conn, tableName = "seq_cnt", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, "txpath")
   
    transformFile(tableName = "seq_cnt", studyName = "txpath", max_layer = 7)
    
  
     
  }
  
  
  if (runIncidencePrevalance) {
    
    # # TODO: change this code
    # ParallelLogger::logInfo("Creating All Tables")
    # createAllTables(connection = connection,
    #                 connectionDetails = connectionDetails,
    #                 cdmDatabaseSchema = cdmDatabaseSchema,
    #                 cohortDatabaseSchema = cohortDatabaseSchema,
    #                 oracleTempSchema = oracleTempSchema,
    #                 debug = debug,
    #                 outputFolder = outputFolder,
    #                 debugSqlFile = debugSqlFile, 
    #                 minCellCount = minCellCount,
    #                 databaseId,
    #                 databaseName)
    # 
    # ParallelLogger::logInfo("Gathering prevalence proportion")
    # getProportion <- function(row, proportionType) {
    #   data <- getProportionByType(connection = connection,
    #                               connectionDetails = connectionDetails,
    #                               cdmDatabaseSchema = cdmDatabaseSchema,
    #                               cohortDatabaseSchema = cohortDatabaseSchema,
    #                               proportionType = proportionType,
    #                               ingredient = row$cohortId)
    #   if (nrow(data) > 0) {
    #     data$cohortId <- row$cohortId
    #   }
    #   return(data)
    # }
    # prevalenceData <- lapply(split(cohortsOfInterest, cohortsOfInterest$cohortId), getProportion, proportionType = "prevalence")
    # prevalenceData <- do.call(rbind, prevalenceData)
    # if (nrow(prevalenceData) > 0) {
    #   prevalenceData$databaseId <- databaseId
    #   prevalenceData <- enforceMinCellValue(prevalenceData, "cohortCount", minCellCount)
    #   prevalenceData <- enforceMinCellValue(prevalenceData, "proportion", minCellCount/prevalenceData$numPersons)
    # }
    # write.csv(prevalenceData, file.path(outputFolder, "prevalence_proportion.csv"))
    # 
    # # Incidence
    # ParallelLogger::logInfo("Gathering incidence proportion")
    # incidenceData <- lapply(split(cohortsOfInterest, cohortsOfInterest$cohortId), getProportion, proportionType = "incidence")
    # incidenceData <- do.call(rbind, incidenceData)
    # if (nrow(incidenceData) > 0) {
    #   incidenceData$databaseId <- databaseId
    #   incidenceData <- enforceMinCellValue(incidenceData, "cohortCount", minCellCount)
    #   incidenceData <- enforceMinCellValue(incidenceData, "proportion", minCellCount/incidenceData$numPersons)
    # }
    # write.csv(incidenceData, file.path(outputFolder, "incidence_proportion.csv"))
    # 
    
  }
  
  
  if (runTreatmentPathways) {
    
    pathToCsv <- "output/cohort.csv"
    cohortIds <- readr::read_csv(pathToCsv, col_types = readr::cols())
    targetCohortId <- cohortIds[cohortIds$cohortType == "target", ]$cohortId
    outcomeCohortIds <- cohortIds[cohortIds$cohortType == "outcome", ]$cohortId
    
    outputFileTitle <- 'output_tp'
    
    # Treatment Pathway
    fromYear <- 1998
    toYear <- 2018
    collapseDates <- 0
    treatmentLine <- 5 # Treatment line number for visualize in graph
    minimumRegimenChange <- 0 # Target patients for at least 1 regimen change
    identicalSeriesCriteria <- 30 # Regard as a same treatment when gap dates between each cycle less than 30 days
    minSubject <- 0 # under 0 patients are removed from plot
    
    ParallelLogger::logInfo("Drawing annual regimen usage graph...")
    usageGraph<-usagePatternGraph(connectionDetails,
                                  cohortDatabaseSchema,
                                  cohortTable,
                                  outputFolder,
                                  outputFileTitle,
                                  cohortIds,
                                  targetCohortId,
                                  outcomeCohortIds,
                                  identicalSeriesCriteria,
                                  fromYear,
                                  toYear)
    
    ParallelLogger::logInfo("Drawing a flow chart of the treatment pathway...")
    treatmentPathway<- treatmentPathway(connectionDetails,
                                        cohortDatabaseSchema,
                                        cohortTable,
                                        outputFolder,
                                        outputFileTitle,
                                        cohortIds,
                                        targetCohortId,
                                        outcomeCohortIds,
                                        minimumRegimenChange,
                                        treatmentLine,
                                        collapseDates,
                                        minSubject,
                                        identicalSeriesCriteria)
    
    ParallelLogger::logInfo("Drawing incidence of the adverse event in each cycle...")
    cycleIncidencePlot <- cycleIncidencePlot(connectionDetails,
                                             cohortDatabaseSchema,
                                             cohortTable,
                                             outputFolder,
                                             outputFileTitle,
                                             cohortIds,
                                             targetCohortId,
                                             outcomeCohortIds,
                                             restrictInitialSeries = TRUE,
                                             restricInitialEvent =TRUE,
                                             identicalSeriesCriteria,
                                             eventPeriod = 30,
                                             minSubject)
    
    # TODO: change this path etc.
    pathToRmd <- system.file("rmd","Treatment_PatternsLocalVer.Rmd",package = "CancerTxPathway")
    rmarkdown::render(pathToRmd,"flex_dashboard",output_dir = outputFolder,output_file = paste0(outputFileTitle,'.','html'),
                      params = list(outputFolder = outputFolder,
                                    outputFileTitle = outputFileTitle,
                                    maximumCycleNumber = maximumCycleNumber, minSubject = minSubject),clean = TRUE)
  }
  
  if (exportResults) {
    exportResults(outputFolder,databaseId)
  }
  
  invisible(NULL)
}
