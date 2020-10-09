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

extractAndWriteToFile <- function(connection, tableName, resultsSchema, studyName, outputFolder, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.@studyName_@tableName"
  renderedSql <- render(parameterizedSql, resultsSchema=resultsSchema, studyName=studyName, tableName=tableName)
  translatedSql <- translate(renderedSql, targetDialect = dbms)
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste(outputFolder, "/",studyName, "/", studyName,"_",tableName,".csv",sep='')
  write.csv(data,file=outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

