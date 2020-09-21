createCohorts <- function(createCohortTable = TRUE,
                          connection,
                         connectionDetails,
                         oracleTempSchema = NULL,
                         cdmDatabaseSchema,
                         cohortDatabaseSchema,
                         vocabularyDatabaseSchema = cdmDatabaseSchema,
                         cohortTable,
                         outputFolder,
                         createCustomCohorts = FALSE
){
  
  # Load cohorts to create
  pathToCsv <- "inst/settings/cohorts_to_create.csv"
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  # cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortId %in% c(34, 35, 36, 37),]
  write.csv(cohortsToCreate, file.path(outputFolder, "cohort.csv"), row.names = FALSE)
  
  # Create study cohort table structure
  if(createCohortTable){
    ParallelLogger::logInfo("Creating table for the cohorts")
    sql <- loadRenderTranslateSql(sql = "CreateCohortTable.sql",
                                  oracleTempSchema = oracleTempSchema,
                                  cohort_database_schema = cohortDatabaseSchema,
                                  cohort_table = cohortTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  # In case of custom definitions: create concept sets
  if (createCustomCohorts) {
    ParallelLogger::logInfo("Creating concept sets for the custom cohorts")
    sql <- loadRenderTranslateSql(sql = "CreateDrugClassesSkratch.sql",
                                  oracleTempSchema = oracleTempSchema,
                                  cohort_database_schema = cohortDatabaseSchema)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    
  }
  
  # Load custom definitions
  sql <- loadRenderTranslateSql(sql = "SELECT * FROM @cohort_database_schema.drug_classes",
                                oracleTempSchema = oracleTempSchema,
                                cohort_database_schema = cohortDatabaseSchema)
  custom_definitions <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  
  # TODO: add check if concept set "closed" with } -> complete
  
  # Instantiate cohorts
  ParallelLogger::logInfo("Insert cohort of interest into the cohort table")
  
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste0("Creating cohort:", cohortsToCreate$cohortName[i], " ", cohortsToCreate$cohortDefinition[i]))
    
    if (cohortsToCreate$cohortDefinition[i] == "ATLAS") {
      sql <- loadRenderTranslateSql(sql = paste0(cohortsToCreate$cohortName[i], ".sql"),
                                    oracleTempSchema = oracleTempSchema,
                                    cdm_database_schema = cdmDatabaseSchema,
                                    vocabulary_database_schema = vocabularyDatabaseSchema,
                                    target_database_schema = cohortDatabaseSchema,
                                    target_cohort_table = cohortTable,
                                    target_cohort_id = cohortsToCreate$cohortId[i])
      DatabaseConnector::executeSql(connection, sql)
      
    } else if (cohortsToCreate$cohortDefinition[i] == "Custom") {
      
      # Load in concept sets (later: change to -> generate sql to form concept sets)
      concept_set <- custom_definitions[custom_definitions$name == cohortsToCreate$cohortName[i],"conceptSet"]
      concept_set <- paste0("(", substr(concept_set, 2, nchar(concept_set)-1), ")")
      
      if (is.null(concept_set))
      {
        warning("Concept set is empty")
      }

      # Insert concept set in SQL template to create cohort
      sql <- loadRenderTranslateSql(sql = "CohortTemplate.sql",
                                    oracleTempSchema = oracleTempSchema,
                                    cdm_database_schema = cdmDatabaseSchema,
                                    vocabulary_database_schema = vocabularyDatabaseSchema,
                                    target_database_schema = cohortDatabaseSchema,
                                    target_cohort_table = cohortTable,
                                    target_cohort_id = cohortsToCreate$cohortId[i],
                                    concept_set = concept_set)
      DatabaseConnector::executeSql(connection, sql)
      
    } else {
      warning("Cohort definition not implemented, specify ATLAS or Custom")
    }

  }
  
  # TODO: test this
  # Check number of subjects per cohort
  ParallelLogger::logInfo("Counting cohorts")
  sql <- loadRenderTranslateSql(sql = "CohortCounts.sql",
                                oracleTempSchema = oracleTempSchema,
                                resultsSchema=cohortDatabaseSchema,
                                cohort_table = cohortTable)
  counts <- DatabaseConnector::querySql(connection, sql)
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))
  write.csv(counts, file.path(outputFolder, "cohort_counts.csv"), row.names = FALSE)
  
  DatabaseConnector::disconnect(connection)
}

