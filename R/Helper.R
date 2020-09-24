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

extractAndWriteToFile <- function(connection, tableName, resultsSchema, studyName, dbms){
  parameterizedSql <- "SELECT * FROM @resultsSchema.@studyName_@tableName"
  renderedSql <- render(parameterizedSql, resultsSchema=resultsSchema, studyName=studyName, tableName=tableName)
  translatedSql <- translate(renderedSql, targetDialect = dbms)
  data <- DatabaseConnector::querySql(connection, translatedSql)
  outputFile <- paste(outputFolder, "/",studyName, "/", studyName,"_",tableName,".csv",sep='')
  write.csv(data,file=outputFile)
  writeLines(paste("Created file '",outputFile,"'",sep=""))
}

transformFile <- function(tableName,studyName, maxPathLength, minCellCount, addNoPaths) {

  inputFile <- paste(outputFolder, "/",studyName, "/", studyName,"_",tableName,".csv",sep='')
  outputFile <- paste(outputFolder, "/",studyName, "/", studyName,"_transformed_",tableName,".csv",sep='')

  file <- as.data.table(read.csv(inputFile))
  group <- as.vector(colnames(file)[!grepl("X|INDEX_YEAR|NUM_PERSONS|CONCEPT_ID", colnames(file))])
  group <- group[1:maxPathLength]
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=group]

  # todo: not remove minCellCount but aggregate pathway to other path
  writeLines(paste("Remove ", sum(file_noyear$freq < minCellCount), " paths with too low frequency"))
  file_noyear <- file_noyear[freq >= minCellCount,]

  transformed_file <- apply(file_noyear[,..group],1, paste, collapse = "-")
  transformed_file <- str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=file_noyear$freq, stringsAsFactors = FALSE)

  summary_counts <- read.csv(paste(outputFolder, "/",studyName, "/", studyName,"_summary.csv",sep=''), stringsAsFactors = FALSE)
  summary_counts <- rbind(summary_counts, c(4,   'Number of pathways final (after minCellCount)', sum(transformed_file$freq)  ))
  write.table(summary_counts,file=paste(outputFolder, "/",studyName, "/", studyName,"_summary.csv",sep=''), sep = ",", row.names = FALSE, col.names = TRUE)

  if (addNoPaths) {
    noPath <- as.integer(summary_counts[summary_counts$COUNT_TYPE == "Number of persons in target cohort", "NUM_PERSONS"]) - sum(transformed_file$freq)
    transformed_file <- rbind(transformed_file, c("End", noPath))
  }

  # The order of the resulting file is important for functioning and good interpretation
  transformed_file$path <- as.factor(transformed_file$path)
  transformed_file$freq <- as.integer(transformed_file$freq)
  write.table(transformed_file[order(-transformed_file$freq, transformed_file$path),],file=outputFile, sep = ",", row.names = FALSE, col.names = FALSE)
  writeLines(paste("Created file '",outputFile,"'",sep=""))

}

doEraDuration <- function(data, minEraDuration) {
  # filter out rows with duration_era < minEraDuration
  data <- data[DURATION_ERA >= minEraDuration,]
  writeLines(paste0("After minEraDuration: ", nrow(data)))

  return(data)
}

doEraCollapse <- function(data, eraCollapseSize) {
  # order data by person_id, drug_concept_id, drug_start_date, drug_end_date
  data <- data[order(PERSON_ID, DRUG_CONCEPT_ID,DRUG_START_DATE, DRUG_END_DATE),]

  # find all rows with gap_same < eraCollapseSize
  rows <- which(data$GAP_SAME < eraCollapseSize)

  # for all rows, modify the row preceding, loop backwards in case more than one collapse
  for (r in rev(rows)) {
    data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_END_DATE]
  }

  # remove all rows with  gap_same < eraCollapseSize
  data <- data[!rows,]
  data[,GAP_SAME:=NULL]

  # re-calculate duration_era
  data[,DURATION_ERA:=difftime(DRUG_END_DATE , DRUG_START_DATE, units = "days")]

  writeLines(paste0("After eraCollapseSize: ", nrow(data)))
  return(data)
}

doCombinationWindow <- function(data, combinationWindow, minEraDuration) {
  data$DRUG_CONCEPT_ID <- as.character(data$DRUG_CONCEPT_ID)

  output <- selectRowsCombinationWindow(data)
  data <- output[[1]]
  rows <- output[[2]]

  # while rows exist:
  while(!(length(rows)==0)) {
    writeLines(as.character(length(rows)))

    for (r in rows) {
      # define switch
      if (-data$GAP_PREVIOUS[r] < combinationWindow) {
        data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
      }

      # define combination
      else if (-data$GAP_PREVIOUS[r] >= combinationWindow) {
        if (data[r - 1, DRUG_END_DATE] <= data[r, DRUG_END_DATE]) {
          # add combination as new row
          new_row <- data[r,]
          new_row[, "DRUG_END_DATE"]  <- data[r - 1, DRUG_END_DATE]
          new_row[, "DRUG_CONCEPT_ID"] <- paste0(data[r - 1, "DRUG_CONCEPT_ID"], "+", data[r, "DRUG_CONCEPT_ID"])

          data <- rbindlist(list(data, new_row))

          # adjust current rows
          temp <- data[r-1,DRUG_END_DATE]
          data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
          data[r,"DRUG_START_DATE"] <- temp
        }

        else if (data[r - 1, DRUG_END_DATE] > data[r, DRUG_END_DATE]) {
          # adjust row for combination
          data[r,"DRUG_CONCEPT_ID"] <- paste0(data[r - 1, "DRUG_CONCEPT_ID"], "+", data[r, "DRUG_CONCEPT_ID"])

          # split row in two by adding new row
          data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]

          new_row <- data[r - 1,]
          new_row[, "DRUG_START_DATE"]  <- data[r, DRUG_END_DATE]

          data <- rbindlist(list(data, new_row))
        }
      }
    }

    # re-calculate duration_era
    data[,DURATION_ERA:=difftime(DRUG_END_DATE, DRUG_START_DATE, units = "days")]

    data <- doEraDuration(data, minEraDuration)

    output <- selectRowsCombinationWindow(data)
    data <- output[[1]]
    rows <- output[[2]]

    writeLines(paste0("After iteration combinationWindow: ", nrow(data)))
  }

  data[,GAP_PREVIOUS:=NULL]

  return(data)
}

selectRowsCombinationWindow <- function(data) {
  # order data by person_id, drug_start_date, drug_end_date
  data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]

  # calculate gap with previous treatment
  data[,GAP_PREVIOUS:=difftime(DRUG_START_DATE, shift(DRUG_END_DATE, type = "lag"), units = "days"), by = PERSON_ID]
  data$GAP_PREVIOUS <- as.integer(data$GAP_PREVIOUS)

  # find all rows with gap_previous < 0
  data[data$GAP_PREVIOUS < 0, SELECT_INDEX:=which(data$GAP_PREVIOUS < 0)]

  # select one row per iteration for each person
  rows <- data[!is.na(SELECT_INDEX),head(.SD,1), by=PERSON_ID]$SELECT_INDEX
  data[,SELECT_INDEX:=NULL]

  return(list(data,rows))
}

doSequentialRepetition <- function(data) {
  # order data by person_id, drug_start_date, drug_end_date
  data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
  data[,ID_PREVIOUS:=shift(DRUG_CONCEPT_ID, type = "lag"), by = PERSON_ID]

  # find all rows for which previous treatment is same
  rows <- which(data$DRUG_CONCEPT_ID == data$ID_PREVIOUS)

  # for all rows, modify the row preceding, loop backwards in case more than one collapse
  for (r in rev(rows)) {
    data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_END_DATE]

    # sum duration_era
    data[r - 1,"DURATION_ERA"] <-  data[r - 1,DURATION_ERA] +  data[r,DURATION_ERA]
  }

  # remove all rows with same sequential treatments
  data <- data[!rows,]
  data[,ID_PREVIOUS:=NULL]
  writeLines(paste0("After collapseSameSequential: ", nrow(data)))

  return(data)
}

doFirstTreatment <- function(data) {
  data <- data[, head(.SD,1), by=.(PERSON_ID, DRUG_CONCEPT_ID)]
}

addLabels <- function(data) {
  cohortIds <- readr::read_csv("output/cohort.csv", col_types = readr::cols())
  labels <- data.frame(DRUG_CONCEPT_ID = as.character(cohortIds$cohortId), CONCEPT_NAME = str_replace_all(cohortIds$cohortName, c(" mono| combi| all"), ""), stringsAsFactors = FALSE)
  data <- merge(data, labels, all.x = TRUE)

  data$CONCEPT_NAME[is.na(data$CONCEPT_NAME)] <- sapply(data$DRUG_CONCEPT_ID[is.na(data$CONCEPT_NAME)], function(x) {
    # revert search to look for longest concept_ids first
    for (l in nrow(labels):1)
    {
      x <- gsub(labels$DRUG_CONCEPT_ID[l], labels$CONCEPT_NAME[l], x)
    }

    return(x)
  })

  return(data)
}


