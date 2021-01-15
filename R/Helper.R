loadRenderTranslateSql <- function(sql,
                                   oracleTempSchema = oracleTempSchema,
                                   dbms = "postgresql",
                                   # cdm_version = cdmVersion,
                                   warnOnMissingParameters = TRUE,
                                   output = FALSE,
                                   outputFile,
                                   ...) {
  if (grepl('.sql', sql)) {
    pathToSql <- paste("inst/SQL/", sql, sep ="")
    parameterizedSql <- readChar(pathToSql, file.info(pathToSql)$size)[1]
  } else {
    parameterizedSql <- sql
  }
  
  renderedSql <- SqlRender::render(sql = parameterizedSql, warnOnMissingParameters = warnOnMissingParameters, ...)
  renderedSql <- SqlRender::translate(sql = renderedSql, targetDialect = dbms, oracleTempSchema = oracleTempSchema)
  
  if (output == TRUE) {
    SqlRender::writeSql(renderedSql,outputFile)
    writeLines(paste("Created file '",outputFile,"'",sep=""))
  }
  
  return(renderedSql)
}

extractFile <- function(connection, tableName, resultsSchema, studyName, databaseName, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.@databaseName_@studyName_@tableName"
  renderedSql <- SqlRender::render(parameterizedSql, resultsSchema=resultsSchema, studyName=studyName, databaseName=databaseName, tableName=tableName)
  translatedSql <- SqlRender::translate(renderedSql, targetDialect = dbms)
  data <- DatabaseConnector::querySql(connection, translatedSql)
}


extractAndWriteToFile <- function(connection, tableName, resultsSchema, studyName, databaseName, path, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.@databaseName_@studyName_@tableName"
  renderedSql <- SqlRender::render(parameterizedSql, resultsSchema=resultsSchema, studyName=studyName, databaseName=databaseName, tableName=tableName)
  translatedSql <- SqlRender::translate(renderedSql, targetDialect = dbms)
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste(path,"_",tableName,".csv",sep='')
  write.csv(data,file=outputFile, row.names = FALSE)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

populatePackageCohorts <- function(targetCohortIds,
                                   targetCohortNames,
                                   outcomeIds,
                                   outcomeNames,
                                   baseUrl = 'https://...'){
  
  # insert the target and outcome cohorts:
  cohortsToCreate <- data.frame(cohortId = c(targetCohortIds, outcomeIds),
                                atlasId = c(targetCohortIds, outcomeIds),
                                name = c(targetCohortNames, outcomeNames),
                                type = c(rep('target', length(targetCohortIds)), rep('outcome',length(outcomeIds)))
  )
  
  write.csv(cohortsToCreate, file.path("./inst/Settings",'CohortsToCreate.csv' ), row.names = FALSE)
  
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Inserting cohort:", cohortsToCreate$name[i]))
    OhdsiRTools::insertCohortDefinitionInPackage(definitionId = cohortsToCreate$atlasId[i], 
                                                 name = cohortsToCreate$name[i], 
                                                 baseUrl = baseUrl, 
                                                 generateStats = F)
  }
  
}

writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    params$data = data
    params$fileName = fileName
    do.call(saveIncremental, params)
    ParallelLogger::logDebug("appending records to ", fileName)
  } else {
    if (file.exists(fileName)) {
      ParallelLogger::logDebug("Overwriting and replacing previous ",fileName, " with new.")
    } else {
      ParallelLogger::logDebug("creating ",fileName)
    }
    readr::write_excel_csv(x = data, 
                           path = fileName, 
                           na = "", 
                           append = FALSE,
                           delim = ",")
  }
}

# recursive function to remove name from all levels of list
stripname <- function(x, name) {
  thisdepth <- depth(x)
  if (thisdepth == 0) {
    return(x)
  } else if (length(nameIndex <- which(names(x) == name))) {
    temp <- names(x)[names(x) == name]
    x[[temp]] <- unname(x[[temp]])
  }
  return(lapply(x, stripname, name))
}

# function to find depth of a list element
depth <- function(this, thisdepth=0){
  if (!is.list(this)) {
    return(thisdepth)
  } else{
    return(max(unlist(lapply(this,depth,thisdepth=thisdepth+1))))    
  }
}

# function to save sunburst plots (TODO: fix it, not working )
screenshotHTML <- function() {
  library(webshot)
  
  file_list <- list.files(path="plots/", pattern = ".html")

  for (f in file_list) {
    
    # Open all html files in path + take screenshot
    URL <- paste0("file://", getwd(), "/plots/", f)
    webshot(URL, file = paste0("plots/screenshot", sub(".html", "", f), ".png"), delay = 1, debug = TRUE)
    

  }
}

# functions from CohortDiagnostics
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortCounts.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = cohortIds)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE) %>% 
    tidyr::tibble()
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Counting cohorts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
  
}

getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     cohortDatabaseSchema = cdmDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortIds,
                                     cdmVersion = 5,
                                     covariateSettings,
                                     batchSize = 100) {
  startTime <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  results <- Andromeda::andromeda()
  for (start in seq(1, length(cohortIds), by = batchSize)) {
    end <- min(start + batchSize - 1, length(cohortIds))
    if (length(cohortIds) > batchSize) {
      ParallelLogger::logInfo(sprintf("Batch characterization. Processing cohorts %s through %s",
                                      start,
                                      end))
    }
    featureExtractionOutput <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                                     oracleTempSchema = oracleTempSchema,
                                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                                                     cdmVersion = cdmVersion,
                                                                     cohortTable = cohortTable,
                                                                     cohortId = cohortIds[start:end],
                                                                     covariateSettings = covariateSettings,
                                                                     aggregated = TRUE)
    
    populationSize <- attr(x = featureExtractionOutput, which = "metaData")$populationSize
    populationSize <- dplyr::tibble(cohortId = names(populationSize),
                                    populationSize = populationSize)
    
    if (!"analysisRef" %in% names(results)) {
      results$analysisRef <- featureExtractionOutput$analysisRef
    }
    if (!"covariateRef" %in% names(results)) {
      results$covariateRef <- featureExtractionOutput$covariateRef 
    } else {
      covariateIds <- results$covariateRef %>%
        dplyr::select(.data$covariateId) 
      Andromeda::appendToTable(results$covariateRef, featureExtractionOutput$covariateRef %>% 
                                 dplyr::anti_join(covariateIds, by = "covariateId", copy = TRUE))
    }
    if ("timeRef" %in% names(featureExtractionOutput) && !"timeRef" %in% names(results)) {
      results$timeRef <- featureExtractionOutput$timeRef
    }
    
    if ("covariates" %in% names(featureExtractionOutput) && 
        dplyr::pull(dplyr::count(featureExtractionOutput$covariates)) > 0) {
      
      covariates <- featureExtractionOutput$covariates %>% 
        dplyr::rename(cohortId = .data$cohortDefinitionId) %>% 
        dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>% 
        dplyr::mutate(sd = sqrt(((populationSize * .data$sumValue) + .data$sumValue)/(populationSize^2))) %>% 
        dplyr::rename(mean = .data$averageValue) %>% 
        dplyr::select(-.data$sumValue, -.data$populationSize)
      
      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$timeId, .data$covariateId, .data$mean, .data$sd)
      } else {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$covariateId, .data$mean, .data$sd)
      }
      if ("covariates" %in% names(results)) {
        Andromeda::appendToTable(results$covariates, covariates) 
      } else {
        results$covariates <- covariates
      }
    }
    
    if ("covariatesContinuous" %in% names(featureExtractionOutput) && 
        dplyr::pull(dplyr::count(featureExtractionOutput$covariatesContinuous)) > 0) {
      covariates <- featureExtractionOutput$covariatesContinuous %>% 
        dplyr::rename(mean = .data$averageValue, 
                      sd = .data$standardDeviation, 
                      cohortId = .data$cohortDefinitionId)
      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$timeId, .data$covariateId, .data$mean, .data$sd)
      } else {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$covariateId, .data$mean, .data$sd)
      }
      if ("covariates" %in% names(results)) {
        Andromeda::appendToTable(results$covariates, covariates) 
      } else {
        results$covariates <- covariates
      }
    }
  }
  
  delta <- Sys.time() - startTime
  ParallelLogger::logInfo("Cohort characterization took ", signif(delta, 3), " ", attr(delta, "units"))
  return(results)
}

exportCharacterization <- function(characteristics,
                                   databaseId,
                                   incremental,
                                   covariateValueFileName,
                                   covariateRefFileName,
                                   analysisRefFileName,
                                   timeRefFileName = NULL,
                                   counts,
                                   minCellCount) {
  if (!"covariates" %in% names(characteristics)) {
    warning("No characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariates <- characteristics$covariates %>% 
      dplyr::filter(mean >= 0.0001) %>% 
      dplyr::mutate(databaseId = !!databaseId) %>% 
      dplyr::left_join(counts, by = c("cohortId", "databaseId"), copy = TRUE) %>%
      dplyr::mutate(mean = dplyr::case_when(.data$mean != 0 & .data$mean < minCellCount / .data$cohortEntries ~ -minCellCount / .data$cohortEntries, 
                                            TRUE ~ .data$mean)) %>%
      dplyr::mutate(sd = dplyr::case_when(.data$mean >= 0 ~ sd)) %>% 
      dplyr::mutate(mean = round(.data$mean, digits = 4),
                    sd = round(.data$sd, digits = 4)) %>%
      dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
    
    if (dplyr::pull(dplyr::count(characteristics$filteredCovariates)) > 0) {
      covariateRef <- dplyr::collect(characteristics$covariateRef)
      writeToCsv(data = covariateRef,
                 fileName = covariateRefFileName,
                 incremental = incremental,
                 covariateId = covariateRef$covariateId)
      analysisRef <- dplyr::collect(characteristics$analysisRef)
      writeToCsv(data = analysisRef,
                 fileName = analysisRefFileName,
                 incremental = incremental,
                 analysisId = analysisRef$analysisId)
      if (!is.null(timeRefFileName)) {
        timeRef <- dplyr::collect(characteristics$timeRef)
        writeToCsv(data = timeRef,
                   fileName = timeRefFileName,
                   incremental = incremental,
                   analysisId = timeRef$timeId)
      }
      writeCovariateDataAndromedaToCsv(data = characteristics$filteredCovariates, 
                                       fileName = covariateValueFileName, 
                                       incremental = incremental)
    }
  } 
}


