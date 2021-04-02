
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

execute <- function(OMOP_CDM = TRUE,
                    connection = NULL,
                    connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema = cdmDatabaseSchema,
                    cohortTable = "cohort",
                    oracleTempSchema = cohortDatabaseSchema,
                    outputFolder,
                    databaseId = "Unknown",
                    databaseName = "Unknown",
                    cohortLocation = "inst/Settings/input_cohorts.csv",
                    runCreateCohorts = TRUE,
                    runCohortCharacterization = FALSE,
                    runTreatmentPathways = FALSE,
                    outputResults = TRUE,
                    study_settings = study_settings) {
  # Input checks
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  ParallelLogger::clearLoggers()
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::logInfo(print(paste0("Running package version ", packageVersion("RespiratoryDrugPathways"))))
  
  # Target/outcome cohorts of interest are extracted from the database (defined using ATLAS or custom concept sets created in SQL inserted into cohort template) 
  if (runCreateCohorts) {
    if (OMOP_CDM) {
      ParallelLogger::logInfo(print("runCreateCohorts OMOP-CDM TRUE"))
      createCohorts(connectionDetails = connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    cohortDatabaseSchema = cohortDatabaseSchema,
                    cohortTable = cohortTable,
                    oracleTempSchema = oracleTempSchema,
                    outputFolder = outputFolder)
    } else {
      ParallelLogger::logInfo("runCreateCohorts Other TRUE")
      importCohorts(cohortLocation = cohortLocation, outputFolder = outputFolder)
    }
  }
  
  # Characterization of study/target population
  if (runCohortCharacterization & OMOP_CDM) {
    ParallelLogger::logInfo(print("runCohortCharacterization TRUE"))
    
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
    ParallelLogger::logInfo(print("runTreatmentPathways TRUE"))
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      if (!file.exists(paste0(getwd(),"/temp/", databaseName, "/", studyName)))
        dir.create(paste0(getwd(),"/temp/",  databaseName, "/", studyName), recursive = TRUE)
      
      if (!file.exists(paste0(outputFolder, "/", studyName)))
        dir.create(paste0(outputFolder, "/",studyName), recursive = TRUE)
      
      ParallelLogger::logInfo(print(paste0("Constructing treatment pathways: ", studyName)))
      
      # Select cohorts included
      targetCohortId <- study_settings[study_settings$param == "targetCohortId",s]
      eventCohortIds <- study_settings[study_settings$param == "eventCohortIds",s]
      
      # Analysis settings
      includeTreatmentsPriorToIndex <- as.integer(study_settings[study_settings$param == "includeTreatmentsPriorToIndex",s]) # Number of days prior to the index date of the target cohort that event cohorts are allowed to start
      minEraDuration <-  as.integer(study_settings[study_settings$param == "minEraDuration",s]) # Minimum time an event era should last to be included in analysis
      splitEventCohorts <-  study_settings[study_settings$param == "splitEventCohorts",s] # Specify event cohort to split in acute (< 30 days) and therapy (>= 30 days)
      eraCollapseSize <-  as.integer(study_settings[study_settings$param == "eraCollapseSize",s]) # Window of time between which two eras of the same event cohort are collapsed into one era
      combinationWindow <-  as.integer(study_settings[study_settings$param == "combinationWindow",s]) # Window of time two event cohorts need to overlap to be considered a combination treatment
      minStepDuration <-  as.integer(study_settings[study_settings$param == "minStepDuration",s]) # Minimum time an event era before or after a generated combination treatment should last to be included in analysis
      filterTreatments <-  study_settings[study_settings$param == "filterTreatments",s] # Select first occurrence of / changes between / all event cohorts
      
      # Load cohorts
      if (OMOP_CDM) {
        # Get cohorts from database and save as csv
        sql <- loadRenderTranslateSql(sql = "SELECT * FROM @resultsSchema.@cohortTable",
                                      dbms = connectionDetails$dbms,
                                      oracleTempSchema = oracleTempSchema,
                                      resultsSchema=cohortDatabaseSchema,
                                      studyName=studyName,
                                      databaseName=databaseName,
                                      cohortTable=cohortTable)
        all_data <- as.data.table(DatabaseConnector::querySql(connection, sql))
        colnames(all_data) <- c("cohort_id", "person_id", "start_date", "end_date")   
        
        # write.csv(all_data, paste0(getwd(),"/temp/",  databaseName, "/", databaseName, "_extracted_cohorts.csv"), row.names = FALSE) 
      } else {
        
        # Load cohorts in from file
        # Required columns: cohort_id, person_id, start_date, end_date
        all_data <- data.table(read.csv(paste0(getwd(),"/temp/",  databaseName, "/extracted_cohorts.csv"), sep=","))
      }
      
      # Select target cohort
      select_people <- all_data$person_id[all_data$cohort_id == targetCohortId]
      data <- all_data[all_data$person_id %in% select_people, ]
      
      # Add index year column based on start date target cohort
      targetCohort <- data[data$cohort_id %in% targetCohortId,,]
      targetCohort$index_year <- stringr::str_match(targetCohort$start_date, "20\\d{2}")
      
      # Number of persons in target cohort (in total + per year)
      counts_targetcohort <- rollup(targetCohort, .N, by = c("index_year"))
      counts_targetcohort$index_year <- paste0("Number of persons in target cohort ", counts_targetcohort$index_year)
      
      # Select event cohorts for target cohort and merge with start/end date and index year
      eventCohorts <- data[data$cohort_id %in% unlist(strsplit(eventCohortIds, split = ",")),,]
      data <- merge(x = eventCohorts, y = targetCohort, by = c("person_id"), all.x = TRUE)
      
      # Only keep event cohorts after target cohort start date
      data <- data[data$start_date.y - as.difftime(includeTreatmentsPriorToIndex, unit="days") <= start_date.x & data$start_date.x < data$end_date.y,]
      
      # Remove unnecessary columns
      data <- data[,c("person_id","index_year", "cohort_id.x", "start_date.x", "end_date.x")]
      colnames(data) <- c("person_id","index_year", "event_cohort_id", "event_start_date", "event_end_date")
      
      # Calculate duration and gap same
      data[,duration_era:=difftime(event_end_date, event_start_date, units = "days")]
      
      data <- data[order(event_start_date, event_end_date),]
      data[,lag_variable:=shift(event_end_date, type = "lag"), by=c("person_id", "event_cohort_id")]
      data[,gap_same:=difftime(event_start_date, lag_variable, units = "days"),]
      data$lag_variable <- NULL
      
      # Apply analysis settings
      ParallelLogger::logInfo("Construct combinations, this may take a while for larger datasets.")
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
      combi <- grep("+", data$event_cohort_id, fixed=TRUE)
      concept_ids <- strsplit(data$event_cohort_id[combi], split="+", fixed=TRUE)
      data$event_cohort_id[combi] <- sapply(concept_ids, function(x) paste(sort(x), collapse = "+"))
      
      data <- doFilterTreatments(data, filterTreatments)
      
      # Add drug_seq
      ParallelLogger::logInfo("Adding drug sequence number.")
      data <- data[order(person_id, event_start_date, event_end_date),]
      data[, drug_seq:=seq_len(.N), by= .(person_id)]
      
      # Add concept_name
      ParallelLogger::logInfo("Adding concept names.")
      data <- addLabels(data, outputFolder)
      
      # Order the combinations
      ParallelLogger::logInfo("Ordering the combinations.")
      combi <- grep("+", data$concept_name, fixed=TRUE)
      concept_names <- strsplit(data$concept_name[combi], split="+", fixed=TRUE)
      data$concept_name[combi] <- sapply(concept_names, function(x) paste(sort(x), collapse = "+"))
      
      # Reformat and save counts target cohort/treatment pathways 
      data$concept_name <- unlist(data$concept_name)
      write.csv(data, paste0(getwd(),"/temp/",  databaseName, "/", studyName, "/", databaseName, "_", studyName, "_drug_seq_processed.csv"), row.names = FALSE) 
      
      # Group based on treatments and rename columns
      data <- as.data.table(reshape2::dcast(data = data, person_id + index_year ~ drug_seq, value.var = "concept_name"))
      colnames(data)[3:ncol(data)] <- paste0("event_cohort_name", colnames(data)[3:ncol(data)])
      
      layers <- c(colnames(data))[3:min(7,ncol(data))] # max first 5
      data <- data[, .(freq=length((person_id))), by = c(layers, "index_year")]
      write.csv(data, paste0(getwd(),"/temp/",  databaseName, "/", studyName, "/", databaseName, "_", studyName, "_paths.csv"), row.names = FALSE) 
      
      # Number of pathways (in total + per year)
      counts_pathways <- rollup(data, sum(freq), by = c("index_year"))
      counts_pathways$index_year <- paste0("Number of pathways (before minCellCount) in  ", counts_pathways$index_year)
      
      # Calculate number of persons in target cohort / with pathways, in total / per year
      colnames(counts_pathways) <- colnames(counts_targetcohort)
      counts <- rbind(counts_targetcohort, counts_pathways)
      
      write.csv(counts, paste0(outputFolder, "/",studyName, "/", databaseName, "_", studyName, "_summary_cnt.csv"), row.names = FALSE)
    }
  }
  
  # Output is generated
  if (outputResults) {
    ParallelLogger::logInfo(print("outputResults TRUE"))
    
    # For all different study settings
    settings <- colnames(study_settings)[grepl("analysis", colnames(study_settings))]
    
    for (s in settings) {
      studyName <- study_settings[study_settings$param == "studyName",s]
      
      ParallelLogger::logInfo(print(paste0("Creating output: ", studyName)))
      
      # Select cohorts included
      targetCohortId <- study_settings[study_settings$param == "targetCohortId",s]
      eventCohortIds <- study_settings[study_settings$param == "eventCohortIds",s]
      
      # Result settings
      maxPathLength <-  as.integer(study_settings[study_settings$param == "maxPathLength",s]) # Maximum number of steps included in treatment pathway (max 5)
      minCellCount <-  as.integer(study_settings[study_settings$param == "minCellCount",s]) # Minimum number of persons with a specific treatment pathway for the pathway to be included in analysis
      minCellMethod  <-  study_settings[study_settings$param == "minCellMethod",s] # Select to completely remove / sequentially adjust (by removing last step as often as necessary) treatment pathways below minCellCount
      groupCombinations  <-  study_settings[study_settings$param == "groupCombinations",s] # Select to group all non-fixed combinations in one category 'other’ in the sunburst plot
      addNoPaths  <-  study_settings[study_settings$param == "addNoPaths",s] # Select to include untreated persons without treatment pathway in the sunburst plot
      
      path <- paste0(outputFolder, "/",studyName, "/", databaseName, "_", studyName)
      temp_path <- paste0(getwd(),"/temp/",  databaseName, "/", studyName, "/", databaseName, "_", studyName)
      
      # Transform results for output
      transformed_data <- transformTreatmentSequence(studyName = studyName,  path = path, temp_path = temp_path, maxPathLength = maxPathLength, minCellCount = minCellCount)
      
      if (!is.null(transformed_data)) {
        file_noyear <- as.data.table(transformed_data[[1]])
        file_withyear <- as.data.table(transformed_data[[2]])
        
        # Compute percentage of people treated with each outcome cohort separately and in the form of combination treatments
        outputPercentageGroupTreated(data = file_noyear, eventCohortIds = eventCohortIds, groupCombinations = TRUE, outputFolder = outputFolder, outputFile = paste(path,"_percentage_groups_treated_noyear.csv",sep=''))
        outputPercentageGroupTreated(data = file_withyear, eventCohortIds = eventCohortIds, groupCombinations = TRUE, outputFolder = outputFolder, outputFile = paste(path,"_percentage_groups_treated_withyear.csv",sep=''))
        
        # Compute step-up/down of asthma/COPD drugs
        outputStepUpDown(file_noyear = file_noyear, path = path, targetCohortId = targetCohortId)
        
        # Duration of era's
        transformDuration(outputFolder = outputFolder, studyName = studyName, databaseName = databaseName, path = path, temp_path = temp_path, maxPathLength = maxPathLength, groupCombinations = TRUE, minCellCount = minCellCount)
        
        # Save (censored) results file_noyear and file_year
        saveTreatmentSequence(file_noyear = file_noyear, file_withyear = file_withyear, path = path, groupCombinations = groupCombinations, minCellCount = minCellCount, minCellMethod = minCellMethod)
        
        file_noyear <- as.data.table(read.csv(paste(path,"_file_noyear.csv",sep=''), stringsAsFactors = FALSE))
        file_withyear <- as.data.table(read.csv(paste(path,"_file_withyear.csv",sep=''), stringsAsFactors = FALSE))
        
        # Treatment pathways sankey diagram
        createSankeyDiagram(data = file_noyear, databaseName = databaseName, studyName = studyName)
        
        # Treatment pathways sunburst plot 
        outputSunburstPlot(data = file_noyear, databaseName = databaseName, eventCohortIds = eventCohortIds, studyName = studyName, outputFolder=outputFolder, path=path, addNoPaths=addNoPaths, maxPathLength=maxPathLength, createInput=TRUE, createPlot=TRUE)
        outputSunburstPlot(data = file_withyear, databaseName = databaseName, eventCohortIds = eventCohortIds, studyName = studyName, outputFolder=outputFolder, path=path, addNoPaths=addNoPaths, maxPathLength=maxPathLength, createInput=TRUE, createPlot=TRUE)
        
      }
    }
    
    # Zip output folder
    zipName <- file.path(getwd(), paste0(databaseName, ".zip"))
    OhdsiSharing::compressFolder(file.path(outputFolder), zipName)
    
  }
  
  invisible(NULL)
}