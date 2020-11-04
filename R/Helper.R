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

  renderedSql <- render(sql = parameterizedSql, warnOnMissingParameters = warnOnMissingParameters, ...)
  renderedSql <- translate(sql = renderedSql, targetDialect = dbms, oracleTempSchema = oracleTempSchema)

  if (output == TRUE) {
    SqlRender::writeSql(renderedSql,outputFile)
    writeLines(paste("Created file '",outputFile,"'",sep=""))
  }

  return(renderedSql)
}

extractAndWriteToFile <- function(connection, tableName, resultsSchema, studyName, databaseName, outputFolder, path, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.@databaseName_@studyName_@tableName"
  renderedSql <- render(parameterizedSql, resultsSchema=resultsSchema, studyName=studyName, databaseName=databaseName, tableName=tableName)
  translatedSql <- translate(renderedSql, targetDialect = dbms)
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste(path,"_",tableName,".csv",sep='')
  write.csv(data,file=outputFile)
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
  
  write.csv(cohortsToCreate, file.path("./inst/settings",'CohortsToCreate.csv' ), row.names = F)
  
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Inserting cohort:", cohortsToCreate$name[i]))
    OhdsiRTools::insertCohortDefinitionInPackage(definitionId = cohortsToCreate$atlasId[i], 
                                                 name = cohortsToCreate$name[i], 
                                                 baseUrl = baseUrl, 
                                                 generateStats = F)
  }
  
}

