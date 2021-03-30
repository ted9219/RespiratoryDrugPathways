doEraDuration <- function(data, minEraDuration) {
  # filter out rows with duration_era < minEraDuration
  data <- data[duration_era >= minEraDuration,]
  ParallelLogger::logInfo(print(paste0("After minEraDuration: ", nrow(data))))
  
  return(data)
}

doStepDuration <- function(data, minStepDuration) {
  # filter out rows with duration_era < minStepDuration for selected steps
  data <- data[(is.na(check_duration) | duration_era >= minStepDuration),]
  ParallelLogger::logInfo(print(paste0("After minStepDuration: ", nrow(data))))
  
  return(data)
}

doSplitEventCohorts <- function(data, splitEventCohorts, outputFolder) {
  
  # load in labels cohorts
  labels <- data.table(readr::read_csv(paste(outputFolder, "/cohort.csv",sep=''), col_types = list("c", "c", "c", "c")))
  
  for (c in splitEventCohorts) {
    # label as acute
    data[event_cohort_id == c & duration_era < 30, "event_cohort_id"] <- as.integer(paste0(c,1))
    
    # label as therapy
    data[event_cohort_id == c & duration_era >= 30, "event_cohort_id"] <- as.integer(paste0(c,2))
    
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
  # order data by person_id, event_cohort_id,start_date, end_date
  data <- data[order(person_id, event_cohort_id,event_start_date, event_end_date),]
  
  # find all rows with gap_same < eraCollapseSize
  rows <- which(data$gap_same < eraCollapseSize)
  
  # for all rows, modify the row preceding, loop backwards in case more than one collapse
  for (r in rev(rows)) {
    data[r - 1,"event_end_date"] <- data[r,event_end_date]
  }
  
  # remove all rows with  gap_same < eraCollapseSize
  data <- data[!rows,]
  data[,gap_same:=NULL]
  
  # re-calculate duration_era
  data[,duration_era:=difftime(event_end_date , event_start_date, units = "days")]
  
  ParallelLogger::logInfo(print(paste0("After eraCollapseSize: ", nrow(data))))
  return(data)
}

doCombinationWindow <- function(data, combinationWindow, minStepDuration) {
  data$event_cohort_id <- as.character(data$event_cohort_id)
  
  data <- selectRowsCombinationWindow(data)
  
  # while rows exist:
  iterations <- 1
  while(sum(data$SELECTED_ROWS)!=0) {
    
    # which have gap previous shorter than combination window OR min(current duration era, previous duration era) -> add column switch
    data[SELECTED_ROWS == 1 & (-GAP_PREVIOUS < combinationWindow  & !(-GAP_PREVIOUS == duration_era | -GAP_PREVIOUS == shift(duration_era, type = "lag"))), switch:=1]
    
    # for rows selected not in column switch -> if data[r - 1, event_end_date] <= data[r, event_end_date] -> add column combination first received, first stopped
    data[SELECTED_ROWS == 1 & is.na(switch) & shift(event_end_date, type = "lag") <= event_end_date, combination_FRFS:=1]
    
    # for rows selected not in column switch -> if data[r - 1, event_end_date] > data[r, event_end_date] -> add column combination last received, first stopped
    data[SELECTED_ROWS == 1 & is.na(switch) & shift(event_end_date, type = "lag") > event_end_date, combination_LRFS:=1]
    
    ParallelLogger::logInfo(print(paste0("Iteration ", iterations, " modifying  ", sum(data$SELECTED_ROWS), " selected rows out of ", nrow(data), ": ", sum(!is.na(data$switch)) , " switches, ", sum(!is.na(data$combination_FRFS)), " combinations FRFS and ", sum(!is.na(data$combination_LRFS)), " combinations LRFS")))
    if (sum(!is.na(data$switch)) + sum(!is.na(data$combination_FRFS)) +  sum(!is.na(data$combination_LRFS)) != sum(data$SELECTED_ROWS)) {
      warning(paste0(sum(data$SELECTED_ROWS), ' does not equal total sum ', sum(!is.na(data$switch)) +  sum(!is.na(data$combination_FRFS)) +  sum(!is.na(data$combination_LRFS))))
    }
    
    # do transformations for each of the three newly added columns
    # construct helpers
    data[,event_start_date_next:=shift(event_start_date, type = "lead"),by=person_id]
    data[,event_end_date_previous:=shift(event_end_date, type = "lag"),by=person_id]
    data[,event_end_date_next:=shift(event_end_date, type = "lead"),by=person_id]
    data[,event_cohort_id_previous:=shift(event_cohort_id, type = "lag"),by=person_id]
    
    # case: switch
    # change end data of previous row -> no minStepDuration
    data[shift(switch, type = "lead")==1,event_end_date:=event_start_date_next]
    
    # case: combination_FRFS
    # add a new row with start date (r) and end date (r-1) as combination (copy current row + change end date + update concept id) -> no minStepDuration
    add_rows_FRFS <- data[combination_FRFS==1,]
    add_rows_FRFS[,event_end_date:=event_end_date_previous]
    add_rows_FRFS[,event_cohort_id:=paste0(event_cohort_id, "+", event_cohort_id_previous)]
    
    # change end date of previous row -> check minStepDuration
    data[shift(combination_FRFS, type = "lead")==1,c("event_end_date","check_duration"):=list(event_start_date_next, 1)]
    
    # change start date of current row -> check minStepDuration 
    data[combination_FRFS==1,c("event_start_date", "check_duration"):=list(event_end_date_previous,1)]
    
    # case: combination_LRFS
    # change current row to combination -> no minStepDuration
    data[combination_LRFS==1,event_cohort_id:=paste0(event_cohort_id, "+", event_cohort_id_previous)]
    
    # add a new row with end date (r) and end date (r-1) to split drug era (copy previous row + change end date) -> check minStepDuration 
    add_rows_LRFS <- data[shift(combination_LRFS, type = "lead")==1,]
    add_rows_LRFS[,c("event_start_date", "check_duration"):=list(event_end_date_next,1)]
    
    # change end date of previous row -> check minStepDuration 
    data[shift(combination_LRFS, type = "lead")==1,c("event_end_date", "check_duration"):=list(event_start_date_next,1)]
    
    # combine all rows and remove helper columns
    data <- rbind(data, add_rows_FRFS, fill=TRUE)
    data <- rbind(data, add_rows_LRFS)
    
    # re-calculate duration_era
    data[,duration_era:=difftime(event_end_date, event_start_date, units = "days")]
    
    data <- doStepDuration(data, minStepDuration)
    
    data <- data[,c("person_id", "index_year", "event_cohort_id", "event_start_date", "event_end_date", "duration_era")]
    
    data <- selectRowsCombinationWindow(data)
    iterations <- iterations + 1
    
    gc()
  }
  
  ParallelLogger::logInfo(print(paste0("After combinationWindow: ", nrow(data))))

  data[,GAP_PREVIOUS:=NULL]
  data[,SELECTED_ROWS:=NULL]
  
  return(data)
}

selectRowsCombinationWindow <- function(data) {
  # order data by person_id, event_start_date, event_end_date
  data <- data[order(person_id, event_start_date, event_end_date),]
  
  # calculate gap with previous treatment
  data[,GAP_PREVIOUS:=difftime(event_start_date, shift(event_end_date, type = "lag"), units = "days"), by = person_id]
  data$GAP_PREVIOUS <- as.integer(data$GAP_PREVIOUS)
  
  # find all rows with gap_previous < 0
  data[data$GAP_PREVIOUS < 0, ALL_ROWS:=which(data$GAP_PREVIOUS < 0)]
  
  # select one row per iteration for each person
  rows <- data[!is.na(ALL_ROWS),head(.SD,1), by=person_id]$ALL_ROWS
  
  data[rows,SELECTED_ROWS:=1]
  data[!rows,SELECTED_ROWS:=0]
  data[,ALL_ROWS:=NULL]
  
  return(data)
}

doFilterTreatments <- function(data, filterTreatments) {
  
  if (filterTreatments == "First") {
    data <- data[, head(.SD,1), by=.(person_id, event_cohort_id)]
    
  } else if (filterTreatments == "Changes") {
    
    # order data by person_id, event_start_date, event_end_date
    data <- data[order(person_id, event_start_date, event_end_date),]
    
    # group all rows per person for which previous treatment is same
    data <- data[, group:=rleid(person_id,event_cohort_id)]
    
    # remove all rows with same sequential treatments
    data <- data[,.(event_start_date=min(event_start_date), event_end_date=max(event_end_date), duration_era=sum(duration_era)), by = .(person_id,index_year,event_cohort_id,group)]
    
    data[,group:=NULL]
    
  } else if (filterTreatments == "All") {
  
    # do nothing
  }
  
  ParallelLogger::logInfo(print(paste0("After filterTreatments: ", nrow(data))))
  
  return(data)
}

doFirstTreatment <- function(data) {
 
  ParallelLogger::logInfo(print(paste0("After doFirstTreatment: ", nrow(data))))
  
  return(data)
}

addLabels <- function(data, outputFolder) {
  labels <- data.frame(readr::read_csv(paste(outputFolder, "/cohort.csv",sep=''), col_types = list("c", "c", "c", "c")))
  labels <- labels[labels$cohortType == "outcome",c("cohortId", "cohortName")]
  colnames(labels) <- c("event_cohort_id", "concept_name")

  data <- merge(data, labels, all.x = TRUE, by = "event_cohort_id")
  
  data$concept_name[is.na(data$concept_name)] <- sapply(data$event_cohort_id[is.na(data$concept_name)], function(x) {
    
    # revert search to look for longest concept_ids first
    for (l in nrow(labels):1)
    {
      
      # if treatment occurs twice in a combination (as monotherapy and in fixed-combination) -> remove monotherapy occurence
      if (any(grep(labels$concept_name[l], x))) {
        x <- gsub(labels$event_cohort_id[l], "", x)
      } else {
        x <- gsub(labels$event_cohort_id[l], labels$concept_name[l], x)
      }
    }
    
    return(x)
  })
  
  # Filter out + at beginning/end or repetitions
  data$concept_name <- gsub("\\++", "+", data$concept_name)
  data$concept_name <- gsub("^\\+", "", data$concept_name)
  data$concept_name <- gsub("\\+$", "", data$concept_name)
 
  return(data)
}
