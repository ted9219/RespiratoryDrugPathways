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
      # todo: look at minimum of combinationWindow & length minimum drug era -> this could replace the below requirement for drug era's starting on the same date
      if (-data$GAP_PREVIOUS[r] < combinationWindow & data[r,"DRUG_START_DATE"] != data[r - 1,"DRUG_START_DATE"]) {
        data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
      }
      
      # define combination
      else {
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
          new_row <- data[r - 1,]
          new_row[, "DRUG_START_DATE"]  <- data[r, DRUG_END_DATE]
          
          data[r - 1,"DRUG_END_DATE"] <- data[r,DRUG_START_DATE]
          
          data <- rbindlist(list(data, new_row))
        }
      }
    }
    
    # re-calculate duration_era
    data[,DURATION_ERA:=difftime(DRUG_END_DATE, DRUG_START_DATE, units = "days")]
    
    data <- doEraDuration(data, minEraDuration = 1) # todo: allow minEraDuration = 0 by changing switch/combination to day - 1?
    
    output <- selectRowsCombinationWindow(data)
    data <- output[[1]]
    rows <- output[[2]]
    
    writeLines(paste0("After iteration combinationWindow: ", nrow(data)))
    gc()
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

addLabels <- function(data, outputFolder) {
  cohortIds <- readr::read_csv(paste(outputFolder, "/cohort.csv",sep=''), col_types = readr::cols())
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
