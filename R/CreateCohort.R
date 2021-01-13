
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

#' Create the exposure and outcome cohorts
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
#' @export

createCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          cohortTable,
                          oracleTempSchema = NULL,
                          outputFolder){
  
  connection <- DatabaseConnector::connect(connectionDetails)
  
  # Load cohorts to create
  # (One can add ATLAS cohorts to package using populatePackageCohorts())
  pathToCsv <- "inst/Settings/cohorts_to_create.csv"
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  write.csv(cohortsToCreate, file.path(outputFolder, "cohort.csv"), row.names = FALSE)
  
  # Create study cohort table structure
  ParallelLogger::logInfo("Creating table for the cohorts")
  sql <- loadRenderTranslateSql(sql = "CreateCohortTable.sql",
                                dbms = connectionDetails$dbms,
                                oracleTempSchema = oracleTempSchema,
                                cohort_database_schema = cohortDatabaseSchema,
                                cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # In case of custom definitions: load custom definitions
  pathToCsv <- "inst/Settings/drug_classes.csv"
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
  
  DatabaseConnector::disconnect(connection)
}

