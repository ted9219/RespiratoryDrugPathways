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


renderStudySpecificSql <- function(studyName, minCellCount, cdmSchema, resultsSchema, dbms = "postgresql"){
  if (studyName == "HTN12mo"){
    TxList <- '21600381,21601461,21601560,21601664,21601744,21601782'
    DxList <- '316866'
    ExcludeDxList <- '444094'
  } else if (studyName == "DM12mo"){
    TxList <- '21600712,21500148'
    DxList <- '201820'
    ExcludeDxList <- '444094,35506621'
  } else if (studyName == "Dep12mo"){
    TxList <- '21604686, 21500526'
    DxList <- '440383'
    ExcludeDxList <- '444094,432876,435783'
  }
  
  inputFile <- "inst/SQL/TxPathParameterized.sql"
  
  outputFile <- paste("TxPath_autoTranslate_", dbms,"_", studyName, ".sql",sep="")
  
  parameterizedSql <- SqlRender::readSql(inputFile)
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmSchema, resultsSchema=resultsSchema, studyName = studyName, txlist=TxList, dxlist=DxList, excludedxlist=ExcludeDxList, smallcellcount = minCellCount)$sql
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  SqlRender::writeSql(translatedSql,outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
  return(outputFile)
}


renderStudySpecificSql2 <- function(minCellCount, cdmDatabaseSchema, cohortDatabaseSchema, targetCohortId, outcomeCohortIds, cohortTable, labels, dbms = "postgresql", studyName = "txpath"){
  inputFile <- "inst/SQL/ConstructTxPaths.sql"
  outputFile <- paste("output/TxPath_autoTranslate_", dbms,".sql",sep="")
  
  parameterizedSql <- SqlRender::readSql(inputFile)
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmDatabaseSchema, resultsSchema=cohortDatabaseSchema,  targetCohortId = targetCohortId, outcomeCohortIds = outcomeCohortIds, cohortTable = cohortTable, labels = labels, smallcellcount = minCellCount, studyName = studyName)$sql
  
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  SqlRender::writeSql(translatedSql,outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
  return(outputFile)
}



extractAndWriteToFile <- function(connection, tableName, cdmSchema, resultsSchema, studyName, dbms){
  sourceName <- "source"
  dbms = "postgresql"
  parameterizedSql <- "SELECT * FROM @resultsSchema.dbo.@studyName_@sourceName_@tableName"
  renderedSql <- SqlRender::renderSql(parameterizedSql, cdmSchema=cdmSchema, resultsSchema=resultsSchema, studyName=studyName, sourceName=sourceName, tableName=tableName)$sql
  translatedSql <- SqlRender::translateSql(renderedSql, targetDialect = dbms)$sql
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  write.csv(data,file=outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

transformFile <- function(tableName,studyName, max_layer) {
  
  inputFile <- paste("output/",studyName,"_",tableName,".csv",sep='') 
  outputFile <- paste("output/transformed_",studyName,"_",tableName,".csv",sep='') 
  
  file <- as.data.table(read.csv(inputFile))
  group <- as.vector(colnames(file)[!grepl("X|INDEX_YEAR|NUM_PERSONS|CONCEPT_ID", colnames(file))])
  group <- group[1:max_layer]
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=group]
  
  transformed_file <- apply(file_noyear[,..group],1, paste, collapse = "-")
  # transformed_file <- str_replace_all(transformed_file, "-1-", "") # TODO: check what is -1/Other
  transformed_file <- str_replace_all(transformed_file, "-NA", "")
  
  # Add final layer "NONE" to each so that there is a difference in maxlayer/final treatment
  
  transformed_file <- data.frame(path=str_replace_all(transformed_file, " ", ""), freq=file_noyear$freq)
  
  # The order of the resulting file is important for functioning and good interpretation
  write.table(transformed_file[order(-transformed_file$freq, transformed_file$path),],file=outputFile, sep = ",", row.names = FALSE, col.names = FALSE)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
   
}



email <- function(from,
                  to = "rijduke@gmail.com",
                  subject = "OHDSI Study 2 Results",
                  dataDescription,
                  sourceName = "source_name",
                  folder = getwd(),
                  compress = TRUE) {
  
  if (missing(from)) stop("Must provide return address")
  if (missing(dataDescription)) stop("Must provide a data description")
  
  suffix <- c("_person_cnt.csv", "_seq_cnt.csv", "_summary.csv")
  prefix <- c("Dep12mo_", "HTN12mo_", "DM12mo_")
  
  files <- unlist(lapply(prefix, paste, 
                         paste(sourceName, suffix, sep =""), 
                         sep =""))
  absolutePaths <- paste(folder, files, sep="/")
  
  if (compress) {
    
    sapply(absolutePaths, function(name) {
      newName = paste(name, ".gz", sep="")
      tmp <- read.csv(file = name)			
      newFile <- gzfile(newName, "w")
      write.csv(tmp, newFile)
      writeLines(paste("Compressed to file '",newName,"'",sep=""))	
      close(newFile)
    })
    absolutePaths <- paste(absolutePaths, ".gz", sep="")		
  }
  
  result <- mailR::send.mail(from = from,
                             to = to,
                             subject = subject,
                             body = paste("\n", dataDescription, "\n",
                                          sep = ""),
                             smtp = list(host.name = "aspmx.l.google.com",
                                         port = 25),
                             attach.files = absolutePaths,						
                             authenticate = FALSE,
                             send = TRUE)
  if (result$isSendPartial()) {
    stop("Error in sending email")
  } else {
    writeLines("Emailed the following files:\n")
    writeLines(paste(absolutePaths, collapse="\n"))
    writeLines(paste("\nto:", to))
  }
}



