loadRenderTranslateSql <- function(sqlFilename,
                                   dbms = "postgresql",
                                   ...,
                                   cdm_version = cdmVersion,
                                   oracleTempSchema,
                                   warnOnMissingParameters = TRUE) {
  pathToSql <- paste("inst/SQL/", sqlFilename, sep ="")
  
  parameterizedSql <- readChar(pathToSql, file.info(pathToSql)$size)
  
  renderedSql <- render(sql = parameterizedSql[1], warnOnMissingParameters = warnOnMissingParameters, ...)
  
  renderedSql <- translate(sql = renderedSql, targetDialect = dbms, oracleTempSchema = oracleTempSchema)
  
  return(renderedSql)
}


renderStudySpecificSql <- function(minCellCount, cdmDatabaseSchema, cohortDatabaseSchema, targetCohortId, outcomeCohortIds, cohortTable, labels, dbms = "postgresql", studyName, minEraDuration,combinationWindow, eraCollapseSize, firstTreatment){
  inputFile <- "inst/SQL/ConstructTxPaths.sql"
  outputFile <- paste("output/TxPath_autoTranslate_", dbms,".sql",sep="")
  
  # TODO: remove old Sql in functions -> use new ones
  parameterizedSql <- SqlRender::readSql(inputFile)
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmDatabaseSchema, resultsSchema=cohortDatabaseSchema,  targetCohortId = targetCohortId, outcomeCohortIds = outcomeCohortIds, cohortTable = cohortTable, labels = labels, studyName = studyName, minEraDuration = minEraDuration, combinationWindow = combinationWindow, eraCollapseSize = eraCollapseSize, firstTreatment = firstTreatment)$sql
  
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  SqlRender::writeSql(translatedSql,outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
  return(outputFile)
}

extractAndWriteToFile <- function(connection, tableName, cdmSchema, resultsSchema, studyName, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.dbo.@studyName_@sourceName_@tableName"
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmSchema, resultsSchema=resultsSchema, studyName=studyName, sourceName="source", tableName=tableName)$sql
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  write.csv(data,file=outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

transformFile <- function(tableName,studyName, maxPathLength) {
  
  inputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  outputFile <- paste("output/transformed_",studyName,"_",tableName,".csv",sep='') 
  
  file <- as.data.table(read.csv(inputFile))
  group <- as.vector(colnames(file)[!grepl("X|INDEX_YEAR|NUM_PERSONS|CONCEPT_ID", colnames(file))])
  group <- group[1:maxPathLength]
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=group]
  
  transformed_file <- apply(file_noyear[,..group],1, paste, collapse = "-")
  # transformed_file <- str_replace_all(transformed_file, "-1-", "") # TODO: check what is -1/Other
  transformed_file <- str_replace_all(transformed_file, "-NA", "")
  
  # TODO: Add final layer "NONE" to each so that there is a difference in maxlayer/final treatment
  
  transformed_file <- data.frame(path=str_replace_all(transformed_file, " ", ""), freq=file_noyear$freq)
  
  # The order of the resulting file is important for functioning and good interpretation
  write.table(transformed_file[order(-transformed_file$freq, transformed_file$path),],file=outputFile, sep = ",", row.names = FALSE, col.names = FALSE)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
   
}



