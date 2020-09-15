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

extractAndWriteToFile <- function(connection, tableName, cdmSchema, resultsSchema, studyName, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.dbo.@studyName_@tableName" 
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmSchema, resultsSchema=resultsSchema, studyName=studyName, tableName=tableName)$sql
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  write.csv(data,file=outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

transformFile <- function(tableName,studyName, maxPathLength, minCellCount) {
  
  inputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  outputFile <- paste("output/transformed_",studyName,"_",tableName,".csv",sep='') 
  
  file <- as.data.table(read.csv(inputFile))
  group <- as.vector(colnames(file)[!grepl("X|INDEX_YEAR|NUM_PERSONS|CONCEPT_ID", colnames(file))])
  group <- group[1:maxPathLength]
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=group]
  
  # todo: not remove minCellCount but aggregate pathway to other path
  file_noyear <- file_noyear[freq >= minCellCount,]
  
  transformed_file <- apply(file_noyear[,..group],1, paste, collapse = "-")
  transformed_file <- str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=file_noyear$freq)
  
  # The order of the resulting file is important for functioning and good interpretation
  write.table(transformed_file[order(-transformed_file$freq, transformed_file$path),],file=outputFile, sep = ",", row.names = FALSE, col.names = FALSE)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
   
}



