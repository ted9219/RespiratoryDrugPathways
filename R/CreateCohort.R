createCohorts <- function(createCohortTable = TRUE,
                          connection,
                         connectionDetails,
                         oracleTempSchema = NULL,
                         cdmDatabaseSchema,
                         cohortDatabaseSchema,
                         vocabularyDatabaseSchema = cdmDatabaseSchema,
                         cohortTable,
                         outputFolder){

  # Load cohorts to create
  pathToCsv <- "inst/settings/cohorts_to_create.csv"
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortId %in% c(1,13,14,20,21,24,25,29,32,33,38,40,42,43,44,45),]
  write.csv(cohortsToCreate, file.path(outputFolder, "cohort.csv"), row.names = FALSE)

  # Create study cohort table structure
  if(createCohortTable){
    ParallelLogger::logInfo("Creating table for the cohorts")
    sql <- loadRenderTranslateSql(sql = "CreateCohortTable.sql",
                                  dbms = connectionDetails$dbms,
                                  oracleTempSchema = oracleTempSchema,
                                  cohort_database_schema = cohortDatabaseSchema,
                                  cohort_table = cohortTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }

  # In case of custom definitions: load custom definitions
  pathToCsv <- "inst/settings/drug_classes.csv"
  custom_definitions <- readr::read_csv(pathToCsv, col_types = readr::cols())

  # Instantiate cohorts
  ParallelLogger::logInfo("Insert cohort of interest into the cohort table")

  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste0("Creating cohort:", cohortsToCreate$cohortName[i], " ", cohortsToCreate$cohortDefinition[i]))

    if (cohortsToCreate$cohortDefinition[i] == "ATLAS") {
      sql <- loadRenderTranslateSql(sql = paste0(cohortsToCreate$cohortName[i], ".sql"),
                                    dbms = connectionDetails$dbms,
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
                                    dbms = connectionDetails$dbms,
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

  # Check number of subjects per cohort
  ParallelLogger::logInfo("Counting cohorts")
  sql <- loadRenderTranslateSql(sql = "CohortCounts.sql",
                                dbms = connectionDetails$dbms,
                                oracleTempSchema = oracleTempSchema,
                                resultsSchema=cohortDatabaseSchema,
                                cohortTable = cohortTable)
  counts <- DatabaseConnector::querySql(connection, sql)
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))
  write.csv(counts, file.path(outputFolder, "cohort_counts.csv"), row.names = FALSE)

  # DatabaseConnector::disconnect(connection)
}

