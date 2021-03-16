
# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of RespiratoryDrugPathways
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the study
#'
#' @details
#' This function will create the exposure and outcome cohorts following the definitions included in
#' this package.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/)
#' @param databaseId         
#' @param databaseName         
#' @param runCreateCohorts         
#' @param runCohortCharacterization         
#' @param runTreatmentPathways         
#' @param outputResults         
#' @param study_settings         
#' @export

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
                    runTreatmentPathways = FALSE,
                    outputResults = TRUE,
                    study_settings = study_settings) {
  # Input checks
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::logInfo(paste0("Running package version ", packageVersion("RespiratoryDrugPathways")))
  
  # Target/outcome cohorts of interest are extracted from the database (defined using ATLAS or custom concept sets created in SQL inserted into cohort template) 
  if (runCreateCohorts) {
    ParallelLogger::logInfo("runCreateCohorts TRUE")
    createCohorts(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  oracleTempSchema = oracleTempSchema,
                  outputFolder = outputFolder)
  }
  
  # Characterization of study/target population
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("runCohortCharacterization TRUE")
    
    if (!file.exists(paste0(outputFolder, "/characterization")))
      dir.create(paste0(outputFolder, "/characterization"), recursive = TRUE)
    
    # For all different target populations
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    targetCohortIds <- unique(as.numeric(study_settings[study_settings$param == "targetCohortId",settings]))
    minCellCount <- max(as.integer(study_settings[study_settings$param == "minCellCount",settings])) # Minimum number of subjects in the target cohort for a given eent in order to be counted in the pathway
    
    cohortCounts <- getCohortCounts(connection = connection,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTable = cohortTable, 
                                    cohortIds = targetCohortIds)
    
    cohortCounts <- cohortCounts %>% 
      dplyr::mutate(databaseId = !!databaseId)
    
    characteristics <- getCohortCharacteristics(connection = connection,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                oracleTempSchema = oracleTempSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortIds = targetCohortIds,
                                                covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAge = TRUE, useDemographicsGender = TRUE, useDemographicsTimeInCohort = TRUE, useDemographicsPostObservationTime = TRUE, useConditionGroupEraAnyTimePrior = TRUE, useCharlsonIndex = TRUE))
    
    exportCharacterization(characteristics = characteristics,
                           databaseId = databaseId,
                           incremental = FALSE,
                           covariateValueFileName = file.path(paste0(outputFolder, "/characterization"), "covariate_value.csv"),
                           covariateRefFileName = file.path(paste0(outputFolder, "/characterization"), "covariate_ref.csv"),
                           analysisRefFileName = file.path(paste0(outputFolder, "/characterization"), "analysis_ref.csv"),
                           counts = cohortCounts,
                           minCellCount = minCellCount)
    
    # Selection of standard results
    settings_characterization <- read.csv("inst/Settings/characterization_settings.csv", stringsAsFactors = FALSE)
    standard_characterization <- read.csv(paste0(outputFolder, "/characterization/covariate_value.csv"), stringsAsFactors = FALSE)
    standard_characterization <- merge(settings_characterization[,c("covariate_id", "covariate_name")], standard_characterization, by = "covariate_id")
    
    # Add cohort counts
    standard_characterization <- rbind(standard_characterization, cbind(covariate_id = "Custom", covariate_name = "Number of persons", cohort_id = cohortCounts$cohortId, mean = cohortCounts$cohortEntries, sd = NA, database_id = databaseId))
    
    # Add custom characterization
    ParallelLogger::logInfo("Adding custom cohorts in characterization")
    custom <- settings_characterization[settings_characterization$covariate_id == "Custom", ]
    
    custom_characterization <- data.frame()
    
    for (t in cohortCounts$cohortId) {
      for (c in 1:nrow(custom)) {
        
        # Get concept sets
        concept_set <- custom$concept_set[c]
        
        # Find all occurences before index date in SQL (including outside observation period)
        sql <- loadRenderTranslateSql(sql = "CustomCharacterization.sql",
                                      dbms = connectionDetails$dbms,
                                      oracleTempSchema = oracleTempSchema,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      resultsSchema=cohortDatabaseSchema,
                                      databaseName=databaseName,
                                      targetCohortId=t,
                                      characterizationConceptSet=concept_set,
                                      cohortTable=cohortTable)
        DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
        
        sql <- loadRenderTranslateSql(sql = "SELECT * FROM @resultsSchema.@tableName",
                                      dbms = connectionDetails$dbms,
                                      oracleTempSchema = oracleTempSchema,
                                      resultsSchema=cohortDatabaseSchema,
                                      tableName=paste0(databaseName, "_characterization"))
        result <- DatabaseConnector::querySql(connection, sql)
        
        # Bind to results
        custom_characterization <- rbind(custom_characterization, cbind(covariate_id = "Custom", covariate_name = custom$covariate_name[c], cohort_id = t, mean = result, sd = NA, database_id = databaseId))
        
      }
    }
    
    # Combine result and save
    colnames(custom_characterization) <- tolower(colnames(custom_characterization))
    all_characterization <- rbind(standard_characterization, custom_characterization)
    write.csv(all_characterization, paste0(outputFolder, "/characterization/characterization.csv"), row.names = FALSE)
    
  }
  
  # Treatment pathways are constructed
  if (runTreatmentPathways) {
    ParallelLogger::logInfo("runTreatmentPathways TRUE")
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      ParallelLogger::logInfo(paste0("Constructing treatment pathways: ", studyName))
      
      # Select cohorts included
      targetCohortId <- study_settings[study_settings$param == "targetCohortId",s]
      eventCohortIds <- study_settings[study_settings$param == "eventCohortIds",s]
      
      # Analysis settings
      includeTreatmentsPriorToIndex <- study_settings[study_settings$param == "includeTreatmentsPriorToIndex",s] # Number of days prior to the index date of the target cohort that event cohorts are allowed to start
      minEraDuration <-  as.integer(study_settings[study_settings$param == "minEraDuration",s]) # Minimum time an event era should last to be included in analysis
      splitEventCohorts <-  study_settings[study_settings$param == "splitEventCohorts",s] # Specify event cohort to split in acute (< 30 days) and therapy (>= 30 days)
      eraCollapseSize <-  as.integer(study_settings[study_settings$param == "eraCollapseSize",s]) # Window of time between which two eras of the same event cohort are collapsed into one era
      combinationWindow <-  as.integer(study_settings[study_settings$param == "combinationWindow",s]) # Window of time two event cohorts need to overlap to be considered a combination treatment
      minStepDuration <-  as.integer(study_settings[study_settings$param == "minStepDuration",s]) # Minimum time an event era before or after a generated combination treatment should last to be included in analysis
      filterTreatments <-  study_settings[study_settings$param == "filterTreatments",s] # Select first occurrence of / changes between / all event cohorts
        
      # Load cohorts and pre-processing in SQL
      sql <- loadRenderTranslateSql(sql = "CreateTreatmentSequence.sql",
                                    dbms = connectionDetails$dbms,
                                    oracleTempSchema = oracleTempSchema,
                                    resultsSchema=cohortDatabaseSchema,
                                    studyName=studyName,
                                    databaseName=databaseName,
                                    targetCohortId=targetCohortId,
                                    eventCohortIds=eventCohortIds,
                                    includeTreatmentsPriorToIndex=includeTreatmentsPriorToIndex,
                                    cohortTable=cohortTable)
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
      
      sql <- loadRenderTranslateSql(sql = "SELECT * FROM @resultsSchema.@databaseName_@studyName_@tableName",
                                    dbms = connectionDetails$dbms,
                                    oracleTempSchema = oracleTempSchema,
                                    resultsSchema=cohortDatabaseSchema,
                                    studyName=studyName,
                                    databaseName=databaseName,
                                    tableName="drug_seq")
      all_data <- DatabaseConnector::querySql(connection, sql)
      
      # Apply analysis settings
      ParallelLogger::logInfo("Construct combinations, this may take a while for larger datasets.")
      data <- as.data.table(all_data)
      writeLines(paste0("Original: ", nrow(data)))
      
      data <- doEraDuration(data, minEraDuration)
      if (splitEventCohorts != "") {data <- doSplitEventCohorts(data, splitEventCohorts, outputFolder)}
      data <- doEraCollapse(data, eraCollapseSize)
      
      time1 <- Sys.time()
      data <- doCombinationWindow(data, combinationWindow, minStepDuration)
      time2 <- Sys.time()
      ParallelLogger::logInfo(paste0("Time needed to execute combination window ", difftime(time2, time1, units = "mins")))
      
      # Order the combinations
      ParallelLogger::logInfo("Order the combinations.")
      combi <- grep("+", data$DRUG_CONCEPT_ID, fixed=TRUE)
      concept_ids <- strsplit(data$DRUG_CONCEPT_ID[combi], split="+", fixed=TRUE)
      data$DRUG_CONCEPT_ID[combi] <- sapply(concept_ids, function(x) paste(sort(x), collapse = "+"))
      
      data <- doFilterTreatments(data, filterTreatments)
      
      # Add drug_seq
      ParallelLogger::logInfo("Adding drug sequence number.")
      data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
      data[, DRUG_SEQ:=seq_len(.N), by= .(PERSON_ID)]
      
      # Add concept_name
      ParallelLogger::logInfo("Adding concept names.")
      data <- addLabels(data, outputFolder)
      
      # Order the combinations
      ParallelLogger::logInfo("Ordering the combinations.")
      combi <- grep("+", data$DRUG_CONCEPT_NAME, fixed=TRUE)
      concept_names <- strsplit(data$CONCEPT_NAME[combi], split="+", fixed=TRUE)
      data$CONCEPT_NAME[combi] <- sapply(concept_names, function(x) paste(sort(x), collapse = "+"))
      
      # Move table back to SQL
      ParallelLogger::logInfo("Move data back to SQL and final processing, this may take longer for larger datasets.")
      time3 <- Sys.time()
      DatabaseConnector::insertTable(connection = connection,
                                     tableName = paste0(cohortDatabaseSchema,".", databaseName, "_", studyName, "_drug_seq_processed"),
                                     data = data,
                                     dropTableIfExists = TRUE,
                                     createTable = TRUE,
                                     tempTable = FALSE)
      
      # Post-processing in SQL
      sql <- loadRenderTranslateSql(sql = "SummarizeTreatmentSequence.sql",
                                    dbms = connectionDetails$dbms,
                                    oracleTempSchema = oracleTempSchema,
                                    resultsSchema=cohortDatabaseSchema,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    studyName=studyName,
                                    databaseName=databaseName)
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
      time4 <- Sys.time()
      
      ParallelLogger::logInfo(paste0("Time needed to move data back to SQL and final processing ", difftime(time4, time3, units = "mins")))
    }
  }
  
  # Output is generated
  if (outputResults) {
    ParallelLogger::logInfo("outputResults TRUE")
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      if (!file.exists(paste0(outputFolder, "/", studyName)))
        dir.create(paste0(outputFolder, "/",studyName), recursive = TRUE)
      
      # Select cohorts included
      targetCohortId <- study_settings[study_settings$param == "targetCohortId",s]
      eventCohortIds <- study_settings[study_settings$param == "eventCohortIds",s]
      
      # Result settings
      maxPathLength <-  as.integer(study_settings[study_settings$param == "maxPathLength",s]) # Maximum number of steps included in treatment pathway (max 5)
      minCellCount <-  as.integer(study_settings[study_settings$param == "minCellCount",s]) # Minimum number of persons with a specific treatment pathway for the pathway to be included in analysis
      minCellMethod  <-  study_settings[study_settings$param == "minCellMethod",s] # Select to completely remove / sequentially adjust (by removing last step as often as necessary) treatment pathways below minCellCount
      groupCombinations  <-  study_settings[study_settings$param == "groupCombinations",s] # Select to group all non-fixed combinations in one category 'other’ in the sunburst plot
      addNoPaths  <-  study_settings[study_settings$param == "addNoPaths",s] # Select to include untreated persons without treatment pathway in the sunburst plot
      
      path = paste0(outputFolder, "/",studyName, "/", databaseName, "_", studyName)
      
      # Numbers study population
      extractAndWriteToFile(connection, tableName = "summary_cnt", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName, path = path, dbms = connectionDetails$dbms)
      
      # Transform results for output
      transformed_data <- transformTreatmentSequence(studyName = studyName, databaseName = databaseName, path = path, maxPathLength = maxPathLength, minCellCount = minCellCount)
      
      if (!is.null(transformed_data)) {
        file_noyear <- as.data.table(transformed_data[[1]])
        file_withyear <- as.data.table(transformed_data[[2]])
        
        # Compute percentage of people treated with each outcome cohort separately and in the form of combination treatments
        outputPercentageGroupTreated(data = file_noyear, eventCohortIds = eventCohortIds, groupCombinations = TRUE, outputFolder = outputFolder, outputFile = paste(path,"_percentage_groups_treated_noyear.csv",sep=''))
        outputPercentageGroupTreated(data = file_withyear, eventCohortIds = eventCohortIds, groupCombinations = TRUE, outputFolder = outputFolder, outputFile = paste(path,"_percentage_groups_treated_withyear.csv",sep=''))
        
        # Compute step-up/down of asthma/COPD drugs
        outputStepUpDown(file_noyear = file_noyear, path = path, targetCohortId = targetCohortId)
        
        # Duration of era's
        transformDuration(connection = connection, outputFolder = outputFolder, cohortDatabaseSchema = cohortDatabaseSchema, dbms = dbms, studyName = studyName, databaseName = databaseName, path = path, maxPathLength = maxPathLength, groupCombinations = TRUE, minCellCount = minCellCount)
      
        
        
        # Save (censored) results file_noyear and file_year
        saveTreatmentSequence(file_noyear = file_noyear, file_withyear = file_withyear, groupCombinations = groupCombinations, minCellCount = minCellCount, minCellMethod = minCellMethod)
        
        file_noyear <- as.data.table(read.csv(paste(path,"_file_noyear.csv",sep=''), stringsAsFactors = FALSE))
        file_withyear <- as.data.table(read.csv(paste(path,"_file_withyear.csv",sep=''), stringsAsFactors = FALSE))
        
        # Treatment pathways sankey diagram
        createSankeyDiagram(data = file_noyear, databaseName = databaseName, studyName = studyName)
        
        # Treatment pathways sunburst plot 
        outputSunburstPlot(data = file_noyear, databaseName = databaseName, eventCohortIds = eventCohortIds, studyName = studyName, outputFolder=outputFolder, path=path, groupCombinations=groupCombinations, addNoPaths=addNoPaths, maxPathLength=maxPathLength, createInput=TRUE, createPlot=TRUE)
        outputSunburstPlot(data = file_withyear, databaseName = databaseName, eventCohortIds = eventCohortIds, studyName = studyName, outputFolder=outputFolder, path=path, groupCombinations=groupCombinations, addNoPaths=addNoPaths, maxPathLength=maxPathLength, createInput=TRUE, createPlot=TRUE)
      

        }
    }
  }
  
  invisible(NULL)
}