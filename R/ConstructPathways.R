doEraDuration <- function(data, minEraDuration) {
  # filter out rows with duration_era < minEraDuration
  data <- data[DURATION_ERA >= minEraDuration,]
  writeLines(paste0("After minEraDuration: ", nrow(data)))
  
  return(data)
}

doStepDuration <- function(data, minStepDuration) {
  # filter out rows with duration_era < minStepDuration for selected steps
  data <- data[(is.na(check_duration) | DURATION_ERA >= minStepDuration),]
  writeLines(paste0("After minStepDuration: ", nrow(data)))
  
  return(data)
}

doAcuteVSTherapy <- function(data, splitAcuteVsTherapy, outputFolder) {
  
  # load in labels cohorts
  labels <- data.table(readr::read_csv(paste(outputFolder, "/cohort.csv",sep=''), col_types = list("c", "c", "c", "c")))
  
  for (c in splitAcuteVsTherapy) {
    # label as acute
    data[DRUG_CONCEPT_ID == c & DURATION_ERA < 30, "DRUG_CONCEPT_ID"] <- as.integer(paste0(c,1))
    
    # label as therapy
    data[DRUG_CONCEPT_ID == c & DURATION_ERA >= 30, "DRUG_CONCEPT_ID"] <- as.integer(paste0(c,2))
    
    # add new labels
    original <- labels[cohortId == as.integer(c),]
    
    new1 <- original
    new1$cohortId <- as.integer(paste0(c,1))
    new1$cohortName <- paste0(new1$cohortName, " (acute)")
    
    new2 <- original
    new2$cohortId <- as.integer(paste0(c,2))
    new2$cohortName <- paste0(new2$cohortName, " (therapy)")
    
    labels <- labels[cohortId != as.integer(c),]
    labels <- rbind(labels, new1, new2)
    
  }
  
  # save new labels cohorts
  write.csv(labels, file.path(outputFolder, "cohort.csv"), row.names = FALSE)
 
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

doCombinationWindow <- function(data, combinationWindow, minStepDuration) {
  data$DRUG_CONCEPT_ID <- as.character(data$DRUG_CONCEPT_ID)
  
  data <- selectRowsCombinationWindow(data)
  
  # while rows exist:
  while(sum(data$SELECTED_ROWS)!=0) {
    
    # which have gap previous shorter than combination window OR min(current duration era, previous duration era) -> add column switch
    data[SELECTED_ROWS == 1 & (-GAP_PREVIOUS < combinationWindow  & !(-GAP_PREVIOUS == DURATION_ERA | -GAP_PREVIOUS == shift(DURATION_ERA, type = "lag"))), switch:=1]
    
    # for rows selected not in column switch -> if data[r - 1, DRUG_END_DATE] <= data[r, DRUG_END_DATE] -> add column combination first received, first stopped
    data[SELECTED_ROWS == 1 & is.na(switch) & shift(DRUG_END_DATE, type = "lag") <= DRUG_END_DATE, combination_FRFS:=1]
    
    # for rows selected not in column switch -> if data[r - 1, DRUG_END_DATE] > data[r, DRUG_END_DATE] -> add column combination last received, first stopped
    data[SELECTED_ROWS == 1 & is.na(switch) & shift(DRUG_END_DATE, type = "lag") > DRUG_END_DATE, combination_LRFS:=1]
    
    writeLines(paste0("Total of ", sum(data$SELECTED_ROWS), " selected rows: ", sum(!is.na(data$switch)) , " switches, ", sum(!is.na(data$combination_FRFS)), " combinations FRFS and ", sum(!is.na(data$combination_LRFS)), " combinations LRFS"))
    if (sum(!is.na(data$switch)) + sum(!is.na(data$combination_FRFS)) +  sum(!is.na(data$combination_LRFS)) != sum(data$SELECTED_ROWS)) {
      warning(paste0(sum(data$SELECTED_ROWS), ' does not equal total sum ', sum(!is.na(data$switch)) +  sum(!is.na(data$combination_FRFS)) +  sum(!is.na(data$combination_LRFS))))
    }
    
    # do transformations for each of the three newly added columns
    # construct helpers
    data[,DRUG_START_DATE_next:=shift(DRUG_START_DATE, type = "lead"),by=PERSON_ID]
    data[,DRUG_END_DATE_previous:=shift(DRUG_END_DATE, type = "lag"),by=PERSON_ID]
    data[,DRUG_END_DATE_next:=shift(DRUG_END_DATE, type = "lead"),by=PERSON_ID]
    data[,DRUG_CONCEPT_ID_previous:=shift(DRUG_CONCEPT_ID, type = "lag"),by=PERSON_ID]
    
    # case: switch
    # change end data of previous row -> no minStepDuration
    data[shift(switch, type = "lead")==1,DRUG_END_DATE:=DRUG_START_DATE_next]
    
    # case: combination_FRFS
    # add a new row with start date (r) and end date (r-1) as combination (copy current row + change end date + update concept id) -> no minStepDuration
    add_rows_FRFS <- data[combination_FRFS==1,]
    add_rows_FRFS[,DRUG_END_DATE:=DRUG_END_DATE_previous]
    add_rows_FRFS[,DRUG_CONCEPT_ID:=paste0(DRUG_CONCEPT_ID, "+", DRUG_CONCEPT_ID_previous)]
    
    # change end date of previous row -> check minStepDuration
    data[shift(combination_FRFS, type = "lead")==1,c("DRUG_END_DATE","check_duration"):=list(DRUG_START_DATE_next, 1)]
    
    # change start date of current row -> check minStepDuration 
    data[combination_FRFS==1,c("DRUG_START_DATE", "check_duration"):=list(DRUG_END_DATE_previous,1)]
    
    # case: combination_LRFS
    # change current row to combination -> no minStepDuration
    data[combination_LRFS==1,DRUG_CONCEPT_ID:=paste0(DRUG_CONCEPT_ID, "+", DRUG_CONCEPT_ID_previous)]
    
    # add a new row with end date (r) and end date (r-1) to split drug era (copy previous row + change end date) -> check minStepDuration 
    add_rows_LRFS <- data[shift(combination_LRFS, type = "lead")==1,]
    add_rows_LRFS[,c("DRUG_START_DATE", "check_duration"):=list(DRUG_END_DATE_next,1)]
    
    # change end date of previous row -> check minStepDuration 
    data[shift(combination_LRFS, type = "lead")==1,c("DRUG_END_DATE", "check_duration"):=list(DRUG_START_DATE_next,1)]
    
    # combine all rows and remove helper columns
    data <- rbind(data, add_rows_FRFS, fill=TRUE)
    data <- rbind(data, add_rows_LRFS)
    
    # re-calculate duration_era
    data[,DURATION_ERA:=difftime(DRUG_END_DATE, DRUG_START_DATE, units = "days")]
    
    data <- doStepDuration(data, minStepDuration)
    
    data <- data[,c("PERSON_ID", "INDEX_YEAR", "DRUG_CONCEPT_ID", "DRUG_START_DATE", "DRUG_END_DATE", "DURATION_ERA")]
    
    data <- selectRowsCombinationWindow(data)
    
    writeLines(paste0("After iteration combinationWindow: ", nrow(data)))
    gc()
  }
  
  ParallelLogger::logInfo("Done with combinationWindow")
  
  data[,GAP_PREVIOUS:=NULL]
  data[,SELECTED_ROWS:=NULL]
  
  return(data)
}

selectRowsCombinationWindow <- function(data) {
  # order data by person_id, drug_start_date, drug_end_date
  data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
  
  # calculate gap with previous treatment
  data[,GAP_PREVIOUS:=difftime(DRUG_START_DATE, shift(DRUG_END_DATE, type = "lag"), units = "days"), by = PERSON_ID]
  data$GAP_PREVIOUS <- as.integer(data$GAP_PREVIOUS)
  
  # find all rows with gap_previous < 0
  data[data$GAP_PREVIOUS < 0, ALL_ROWS:=which(data$GAP_PREVIOUS < 0)]
  
  # select one row per iteration for each person
  rows <- data[!is.na(ALL_ROWS),head(.SD,1), by=PERSON_ID]$ALL_ROWS
  
  data[rows,SELECTED_ROWS:=1]
  data[!rows,SELECTED_ROWS:=0]
  data[,ALL_ROWS:=NULL]
  
  return(data)
}

doSequentialRepetition <- function(data) {
  # order data by person_id, drug_start_date, drug_end_date
  data <- data[order(PERSON_ID, DRUG_START_DATE, DRUG_END_DATE),]
  
  # group all rows per person for which previous treatment is same
  data <- data[, group:=rleid(PERSON_ID,DRUG_CONCEPT_ID)]
  
  # remove all rows with same sequential treatments
  data <- data[,.(DRUG_START_DATE=min(DRUG_START_DATE), DRUG_END_DATE=max(DRUG_END_DATE), DURATION_ERA=sum(DURATION_ERA)), by = .(PERSON_ID,INDEX_YEAR,DRUG_CONCEPT_ID,group)]

  data[,group:=NULL]
  writeLines(paste0("After collapseSameSequential: ", nrow(data)))
  
  return(data)
}

doFirstTreatment <- function(data) {
  data <- data[, head(.SD,1), by=.(PERSON_ID, DRUG_CONCEPT_ID)]
  writeLines(paste0("After doFirstTreatment: ", nrow(data)))
  
  return(data)
}

addLabels <- function(data, outputFolder) {
  labels <- data.frame(readr::read_csv(paste(outputFolder, "/cohort.csv",sep=''), col_types = list("c", "c", "c", "c")))
  labels <- labels[labels$cohortType == "outcome",c("cohortId", "cohortName")]
  colnames(labels) <- c("DRUG_CONCEPT_ID", "CONCEPT_NAME")

  data <- merge(data, labels, all.x = TRUE, by = "DRUG_CONCEPT_ID")
  
  data$CONCEPT_NAME[is.na(data$CONCEPT_NAME)] <- sapply(data$DRUG_CONCEPT_ID[is.na(data$CONCEPT_NAME)], function(x) {
    
    # revert search to look for longest concept_ids first
    for (l in nrow(labels):1)
    {
      
      # if treatment occurs twice in a combination (as monotherapy and in fixed-combination) -> remove monotherapy occurence
      if (any(grep(labels$CONCEPT_NAME[l], x))) {
        x <- gsub(labels$DRUG_CONCEPT_ID[l], "", x)
      } else {
        x <- gsub(labels$DRUG_CONCEPT_ID[l], labels$CONCEPT_NAME[l], x)
      }
    }
    
    return(x)
  })
  
  # Filter out + at beginning/end or repetitions
  data$CONCEPT_NAME <- gsub("\\++", "+", data$CONCEPT_NAME)
  data$CONCEPT_NAME <- gsub("^\\+", "", data$CONCEPT_NAME)
  data$CONCEPT_NAME <- gsub("\\+$", "", data$CONCEPT_NAME)
 
  return(data)
}
