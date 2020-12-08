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
  
  write.csv(cohortsToCreate, file.path("./inst/settings",'CohortsToCreate.csv' ), row.names = FALSE)
  
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


# TODO: check this function, screenshot not working yet
screenshotHTML <- function(path) {
  library(webshot)
  
  file_list <- list.files(path="plots/", pattern = ".html")

  for (f in file_list) { # f <- file_list[2]
    
    # Open all html files in path + take screenshot
    URL <- paste0("file://", getwd(), "/plots/", f)
    
    webshot(URL, file = paste0("plots/screenshot", sub(".html", "", f), ".png"), delay = 1, debug = TRUE)
    
  }
}

