
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
  
  # Target/outcome cohorts of interest are extracted from the database (defined using ATLAS or custom concept sets created in SQL inserted into cohort template) 
  if (runCreateCohorts) {
    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    
    ParallelLogger::logInfo("Creating cohorts")
    createCohorts(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  oracleTempSchema = oracleTempSchema,
                  outputFolder = outputFolder)
  }
  
  # Characterization of study/target population
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("Characterization")
    
    # For all different target populations
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    targetCohortIds <- unique(lapply(study_settings[study_settings$param == "targetCohortId",settings], function(x) {x}))
    
    cohortCounts <- CohortDiagnostics::getCohortCounts(connection = connection,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       cohortTable = cohortTable, 
                                                       cohortIds = targetCohortIds)
    
    cohortCounts <- cohortCounts %>% 
      dplyr::mutate(databaseId = !!databaseId)
    
    writeToCsv(data = cohortCounts, 
               fileName = file.path(paste0(outputFolder, "/characterization"), "cohort_count.csv"), 
               incremental = FALSE, 
               cohortId = subset$cohortId)
    
    # todo: test for multiple cohorts
    characteristics <- CohortDiagnostics::getCohortCharacteristics(connection = connection,
                                                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                                                   oracleTempSchema = oracleTempSchema,
                                                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                                                   cohortTable = cohortTable,
                                                                   cohortIds = targetCohortIds,
                                                                   covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAge = TRUE, useDemographicsGender = TRUE, useDemographicsTimeInCohort = TRUE, useDemographicsPostObservationTime = TRUE, useConditionGroupEraAnyTimePrior = TRUE, useCharlsonIndex = TRUE))
    
    CohortDiagnostics::exportCharacterization(characteristics = characteristics,
                                              databaseId = databaseId,
                                              incremental = FALSE,
                                              covariateValueFileName = file.path(paste0(outputFolder, "/characterization"), "covariate_value.csv"),
                                              covariateRefFileName = file.path(paste0(outputFolder, "/characterization"), "covariate_ref.csv"),
                                              analysisRefFileName = file.path(paste0(outputFolder, "/characterization"), "analysis_ref.csv"),
                                              counts = cohortCounts,
                                              minCellCount = 5)
    # Selection results
    # todo: add custom covariates
    all_characterization <- data.frame(readr::read_csv(paste0(outputFolder, "/characterization/covariate_value.csv"), col_types = readr::cols()))
    final_characterization <- data.frame(readr::read_csv("inst/Settings/characterization_settings.csv", col_types = readr::cols()))
    final_characterization <- merge(final_characterization, all_characterization, by = "covariate_id", all.x = TRUE)
    
  }
  # Treatment pathways are constructed
  if (runTreatmentPathways) {
    ParallelLogger::logInfo("Constructing treatment pathways")
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      if (!file.exists(paste0(outputFolder, "/", studyName)))
        dir.create(paste0(outputFolder, "/",studyName), recursive = TRUE)
      
      # Select cohorts included
      targetCohortId <- study_settings[study_settings$param == "targetCohortId",s]
      outcomeCohortIds <- study_settings[study_settings$param == "outcomeCohortIds",s]
      
      # Analyis settings
      minEraDuration <-  as.integer(study_settings[study_settings$param == "minEraDuration",s]) # Minimum time an era should last to be included in analysis
      minStepDuration <-  as.integer(study_settings[study_settings$param == "minStepDuration",s]) # Minimum time a step (split drug era) before or after a combination treatment should last to be included in analysis
      eraCollapseSize <-  as.integer(study_settings[study_settings$param == "eraCollapseSize",s]) # Window of time between two same evnt cohorts that are considered one era
      combinationWindow <-  as.integer(study_settings[study_settings$param == "combinationWindow",s]) # Window of time when two event cohorts need to overlap to be considered a combination
      sequentialRepetition <-  study_settings[study_settings$param == "sequentialRepetition",s] # Select to only remove sequential occurences of each outcome cohort
      firstTreatment <-  study_settings[study_settings$param == "firstTreatment",s] # Select to only include first occurrence of each outcome cohort
      
      # Load cohorts and pre-processing in SQL
      sql <- loadRenderTranslateSql(sql = "CreateTreatmentSequence.sql",
                                    dbms = connectionDetails$dbms,
                                    oracleTempSchema = oracleTempSchema,
                                    resultsSchema=cohortDatabaseSchema,
                                    studyName=studyName,
                                    databaseName=databaseName,
                                    targetCohortId=targetCohortId,
                                    outcomeCohortIds=outcomeCohortIds,
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
      data <- as.data.table(all_data)
      writeLines(paste0("Original: ", nrow(data)))
      
      data <- doEraDuration(data, minEraDuration)
      data <- doEraCollapse(data, eraCollapseSize)
      data <- doCombinationWindow(data, combinationWindow, minStepDuration)
      if (sequentialRepetition) {data <- doSequentialRepetition(data)}
      if (firstTreatment) {data <- doFirstTreatment(data)}
      
      # Add drug_seq
      data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
      data[, DRUG_SEQ:=seq_len(.N), by= .(PERSON_ID)]
      
      # Order the combinations
      concept_ids <- strsplit(data$DRUG_CONCEPT_ID, split="+", fixed=TRUE)
      data$DRUG_CONCEPT_ID <- sapply(concept_ids, function(x) paste(sort(x), collapse = "+"))
      
      # Add concept_name
      data <- addLabels(data, outputFolder)
      
      # Move table back to SQL
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
    }
  }
  
  # Output is generated
  if (outputResults) {
    ParallelLogger::logInfo("Generating output")
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      # Result settings
      maxPathLength <-  as.integer(study_settings[study_settings$param == "maxPathLength",s]) # Maximum number of steps in a given pathway to be included in the sunburst plot
      minCellCount <-  as.integer(study_settings[study_settings$param == "minCellCount",s]) # Minimum number of subjects in the target cohort for a given eent in order to be counted in the pathway
      addNoPaths  <-  study_settings[study_settings$param == "addNoPaths",s] # Select to add subjects without path to sunburst plot
      otherCombinations  <-  study_settings[study_settings$param == "otherCombinations",s] # Select to group all non-fixed combinations in one category 'other combinations'
      
      path = paste0(outputFolder, "/",studyName, "/", databaseName, "_", studyName)
      
      # Get results
      extractAndWriteToFile(connection, tableName = "summary", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName, outputFolder = outputFolder, path = path, dbms = connectionDetails$dbms)
      extractAndWriteToFile(connection, tableName = "person_cnt", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName, outputFolder = outputFolder, path = path, dbms = connectionDetails$dbms)
      extractAndWriteToFile(connection, tableName = "drug_seq_summary", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName,outputFolder = outputFolder, path = path, dbms = connectionDetails$dbms)
      extractAndWriteToFile(connection, tableName = "duration_cnt", resultsSchema = cohortDatabaseSchema, studyName = studyName,databaseName = databaseName, outputFolder = outputFolder, path = path, dbms = connectionDetails$dbms)
      
      # Process results to outputs
      generateOutput(studyName = studyName, databaseName = databaseName, outputFolder = outputFolder, path = path, maxPathLength = maxPathLength, minCellCount = minCellCount, addNoPaths = addNoPaths, otherCombinations = otherCombinations)
      
      # Create sunburst plots
      createSunburstPlot(studyName = studyName,  outputFolder = outputFolder, path=path)
      
    }
  }
  
  DatabaseConnector::disconnect(connection)
  invisible(NULL)
}
