
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
    
    # Select cohorts included
    pathToCsv <- "output/cohort.csv"
    cohortIds <- readr::read_csv(pathToCsv, col_types = readr::cols())
    targetCohortId <- cohortIds[cohortIds$cohortType == "target", ]$cohortId
    
    select <- "mono"
    outcomeCohortIds <- cohortIds[cohortIds$cohortType == "outcome" & grepl(select, cohortIds$cohortName), ]$cohortId
    
    # Analyis settings
    studyName = "txpath"
    minEraDuration <- 7 # 7
    eraCollapseSize <- 40
    combinationWindow <- 30
    firstTreatment <- FALSE # Select to only include first encounter of each outcome cohort

    # Result settings
    maxPathLength <- 7 # Maximum number of treatment layers
    minCellCount <- 5 # Minimum number of people in treatment path
    
    # Load cohorts and pre-processing in SQL
    sql <- loadRenderTranslateSql(sql = "CreateTreatmentSequence.sql",
                                  oracleTempSchema = oracleTempSchema,
                                  resultsSchema=cohortDatabaseSchema,
                                  studyName=studyName, 
                                  targetCohortId=targetCohortId,
                                  outcomeCohortIds=outcomeCohortIds,
                                  cohortTable=cohortTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
    sql <- loadRenderTranslateSql(sql = "SELECT * FROM @resultsSchema.dbo.@studyName_@tableName",
                                  oracleTempSchema = oracleTempSchema,
                                  resultsSchema=cohortDatabaseSchema,
                                  studyName=studyName, 
                                  tableName="drug_seq")
    all_data <- DatabaseConnector::querySql(connection, sql)
    
    # Apply analysis settings
    data <- as.data.table(all_data)
    print(paste0("Original: ", nrow(data)))

    # -- minEraDuration
    # filter out rows with duration_era < minEraDuration
    data <- data[DURATION_ERA >= minEraDuration,]
    print(paste0("After minEraDuration: ", nrow(data)))

    # -- eraCollapseSize
    # order data by person_id, drug_concept_id, drug_start_date, drug_end_date
    data <- data[order(PERSON_ID, DRUG_CONCEPT_ID,DRUG_START_DATE, DRUG_END_DATE),]
    
    # find all rows with gap_same < eraCollapseSize
    rows <- which(data$GAP_SAME < eraCollapseSize)
    
    # for all rows, modify the row preceding, loop backwards in case more than one collapse
    for (r in rev(rows)) {
      data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_END_DATE]
    }
    
    # remove all rows with  gap_same < eraCollapseSize
    data <- data[!rows,]
    data[,GAP_SAME:=NULL]
    print(paste0("After eraCollapseSize: ", nrow(data)))
    
    # re-calculate duration_era
    data[,DURATION_ERA:=difftime(DRUG_END_DATE , DRUG_START_DATE, units = "days")]
    
    # -- combinationWindow
    data$DRUG_CONCEPT_ID <- as.character(data$DRUG_CONCEPT_ID)
    
    # order data by person_id, drug_start_date, drug_end_date
    data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
    
    # calculate gap with previous treatment
    data[,GAP_PREVIOUS:=difftime(DRUG_START_DATE, shift(DRUG_END_DATE, type = "lag"), units = "days"), by = PERSON_ID]
    data$GAP_PREVIOUS <- as.integer(data$GAP_PREVIOUS)
    
    # find all rows with gap_previous < 0
    data[data$GAP_PREVIOUS < 0, SELECT_INDEX:=which(data$GAP_PREVIOUS < 0)]
    
    # select one row per iteration for each person
    rows <- data[!is.na(SELECT_INDEX),head(.SD,1), by=PERSON_ID]$SELECT_INDEX
    data[,SELECT_INDEX:=NULL]
    
    # while rows exist:
    while(!(length(rows)==0)) {
      print(length(rows))
      
      for (r in rows) {
        # switch
        if (-data$GAP_PREVIOUS[r] < combinationWindow) {
          data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
        }
        
        # combination
        else if (-data$GAP_PREVIOUS[r] >= combinationWindow) {
          if (data[r - 1, DRUG_END_DATE] <= data[r, DRUG_END_DATE]) {
            # add combination as new row
            new_row <- data[r,]
            new_row[, "DRUG_END_DATE"]  <- data[r - 1, DRUG_END_DATE]
            new_row[, "DRUG_CONCEPT_ID"] <- paste0(data[r - 1, "DRUG_CONCEPT_ID"], "+", data[r, "DRUG_CONCEPT_ID"])
            
            data <- rbindlist(list(data, new_row))
            
            # adjust current rows
            temp <- data[r-1,DRUG_END_DATE]
            data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
            data[r,"DRUG_START_DATE"] <- temp
          }
          
          else if (data[r - 1, DRUG_END_DATE] > data[r, DRUG_END_DATE]) {
            # adjust row for combination
            data[r,"DRUG_CONCEPT_ID"] <- paste0(data[r - 1, "DRUG_CONCEPT_ID"], "+", data[r, "DRUG_CONCEPT_ID"])
        
            
            # split row in two by adding new row 
            data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
            
            new_row <- data[r - 1,]
            new_row[, "DRUG_START_DATE"]  <- data[r, DRUG_END_DATE]

            data <- rbindlist(list(data, new_row))
          }
        }
      }
      
      # re-calculate duration_era
      data[,DURATION_ERA:=difftime(DRUG_END_DATE, DRUG_START_DATE, units = "days")]
      
      # -- minEraDuration
      # filter out rows with duration_era < minEraDuration
      data <- data[DURATION_ERA >= minEraDuration,]

      # order data by person_id, drug_start_date, drug_end_date
      data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
      
      # calculate gap with previous treatment
      data[,GAP_PREVIOUS:=difftime(DRUG_START_DATE, shift(DRUG_END_DATE, type = "lag"), units = "days"), by = PERSON_ID]
      data$GAP_PREVIOUS <- as.integer(data$GAP_PREVIOUS)
      
      # find all rows with gap_previous < 0
      data[data$GAP_PREVIOUS < 0, SELECT_INDEX:=which(data$GAP_PREVIOUS < 0)]
      
      # select one row per iteration for each person
      rows <- data[!is.na(SELECT_INDEX),head(.SD,1), by=PERSON_ID]$SELECT_INDEX
      data[,SELECT_INDEX:=NULL]
      
      print(paste0("After iteration combinationWindow: ", nrow(data)))
    }
    
    # add drug_seq
    data[, DRUG_SEQ:=seq_len(.N), by= .(PERSON_ID)]
  
    # -- firstTreatment
    if (firstTreatment == TRUE) {
      data <- data[, head(.SD,1), by=.(PERSON_ID, DRUG_CONCEPT_ID)]
    }
   
    # Add labels
    # cohortIds$cohortName <- paste("'", cohortIds$cohortName, "'")
    data <- merge(data, cohortIds, by.x = "DRUG_CONCEPT_ID", by.y = "cohortId")
  
    # Get results
    extractAndWriteToFile(conn, tableName = "summary", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, studyName = studyName, dbms = "postgresql")
    extractAndWriteToFile(conn, tableName = "person_cnt", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, studyName = studyName, dbms = "postgresql")
    extractAndWriteToFile(conn, tableName = "seq_cnt", cdmSchema = cdmDatabaseSchema , resultsSchema = cohortDatabaseSchema, studyName = studyName, dbms = "postgresql")
   
    # Process results to input in sunburst plot
    transformFile(tableName = "seq_cnt", studyName = studyName, maxPathLength = maxPathLength, minCellCount = minCellCount)
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
