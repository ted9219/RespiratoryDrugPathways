

#' Title
#'
#' @param studyName 
#' @param databaseName 
#' @param path 
#' @param maxPathLength 
#' @param minCellCount 
#'
#' @return
#' @export
#'
#' @examples
transformTreatmentSequence <- function(studyName, path, temp_path, maxPathLength, minCellCount) {
  
  file <- data.table(read.csv(paste(temp_path,"_paths.csv",sep=''), stringsAsFactors = FALSE))
  
  # Apply maxPathLength and remove unnessary columns
  layers <- as.vector(colnames(file)[!grepl("index_year|freq", colnames(file))])
  layers <- layers[1:maxPathLength]
  
  columns <- c(layers, "index_year", "freq")
  
  file <- file[,..columns]
  
  if (nrow(file) == 0) {
    warning(paste0("Data is empty for study settings ", studyName))
    return (NULL)
  }
  
  # Summarize which non-fixed combinations occurring
  findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
  
  combinations <- as.matrix(file)[findCombinations == TRUE]
  num_columns <-  sum(grepl("concept_name", colnames(file)))
  freqCombinations <- matrix(rep(file$freq, times = num_columns), ncol = num_columns)[findCombinations == TRUE]
  
  summaryCombinations <- data.table(combination = combinations, freq = freqCombinations)
  summaryCombinations <- summaryCombinations[,.(freq=sum(freq)), by=combination][order(-freq)]

  summaryCombinations <- summaryCombinations[freq >= minCellCount,]
  write.csv(summaryCombinations, file=paste(path,"_combinations.csv",sep=''), row.names = FALSE)
  
  # Group the resulting treatment paths
  file_noyear <- file[,.(freq=sum(freq)), by=layers]
  file_withyear <- file[,.(freq=sum(freq)), by=c(layers, "index_year")]

  ParallelLogger::logInfo("transformTreatmentSequence done")
  
  return(list(file_noyear, file_withyear))
}

#' Title
#'
#' @param file_noyear 
#' @param file_withyear 
#' @param path 
#' @param groupCombinations 
#' @param minCellCount 
#' @param minCellMethod 
#'
#' @return
#' @export
#'
#' @examples
saveTreatmentSequence <- function(file_noyear, file_withyear, path, groupCombinations, minCellCount, minCellMethod) {
  
  # Group non-fixed combinations in one group according to groupCobinations
  file_noyear <- groupInfrequentCombinations(file_noyear, groupCombinations)
  file_withyear <- groupInfrequentCombinations(file_withyear, groupCombinations)
  
  layers <- as.vector(colnames(file_noyear)[!grepl("index_year|freq", colnames(file_noyear))])
  
  # Apply minCellCount by adjusting to other most similar path (removing last treatment in path) or else remove complete path
  if (minCellMethod == "Adjust") {
    col <- ncol(file_noyear) - 1
    while (sum(file_noyear$freq < minCellCount) > 0 & col !=0) {
      ParallelLogger::logInfo(paste("Change col ", col, " to NA for ", sum(file_noyear$freq < minCellCount), " paths with too low frequency (without year)"))
      
      file_noyear[freq < minCellCount,col] <- NA
      file_noyear <- file_noyear[,.(freq=sum(freq)), by=layers]
      
      col <- col - 1
    }
    
    col <- ncol(file_withyear) - 2
    while (sum(file_withyear$freq < minCellCount) > 0 & col !=0) {
      ParallelLogger::logInfo(paste("Change col ", col, " to NA for ", sum(file_withyear$freq < minCellCount), " paths with too low frequency (with year)"))
      
      file_withyear[freq < minCellCount,col] <- NA
      file_withyear <- file_withyear[,.(freq=sum(freq)), by=c(layers, "index_year")]
      
      col <- col - 1
    }
    
    # If path becomes completely NA -> add to "Other" group to distinguish from non-treated
    file_noyear$event_cohort_name1[is.na(file_noyear$event_cohort_name1)] <- "Other"
    file_noyear <- file_noyear[,.(freq=sum(freq)), by=layers]
    
    file_withyear$event_cohort_name1[is.na(file_withyear$event_cohort_name1)] <- "Other"
    file_withyear <- file_withyear[,.(freq=sum(freq)), by=c(layers, "index_year")]
  
  }
  
  ParallelLogger::logInfo(paste("Remove ", sum(file_noyear$freq < minCellCount), " paths with too low frequency (without year)"))
  file_noyear <- file_noyear[freq >= minCellCount,]
  
  ParallelLogger::logInfo(paste("Remove ", sum(file_withyear$freq < minCellCount), " paths with too low frequency (with year)"))
  file_withyear <- file_withyear[freq >= minCellCount,]
  
  summary_counts <- read.csv(paste(path,"_summary_cnt.csv",sep=''), stringsAsFactors = FALSE)
  summary_counts <- rbind(summary_counts, c("Total number of pathways (after minCellCount)", sum(file_noyear$freq)))
  
  for (y in unique(file_withyear$index_year)) {
    summary_counts <- rbind(summary_counts, c(paste0("Number of pathways (after minCellCount) in ", y), sum(file_withyear$freq[file_withyear$index_year == y])))
  }
  
  write.table(summary_counts,file=paste(path,"_summary_cnt.csv",sep=''), sep = ",", row.names = FALSE, col.names = TRUE)
  
  write.csv(file_noyear,  paste(path,"_file_noyear.csv",sep=''), row.names = FALSE)
  write.csv(file_withyear,  paste(path,"_file_withyear.csv",sep=''), row.names = FALSE)
  
  ParallelLogger::logInfo("saveTreatmentSequence done")
  
}


#' Title
#'
#' @param data 
#' @param eventCohortIds 
#' @param groupCombinations 
#' @param path 
#' @param outputFolder 
#' @param outputFile 
#'
#' @return
#' @export
#'
#' @examples
outputPercentageGroupTreated <- function(data, eventCohortIds, groupCombinations, path, outputFolder, outputFile) {
  if (is.null(data$index_year)) {
    # For file_noyear compute once
    result <- computePercentageGroupTreated(data, eventCohortIds, groupCombinations, outputFolder)
    
  } else {
    # For file_withyear compute per year
    years <- unlist(unique(data[,"index_year"]))
    
    results <- lapply(years, function(y) {
      subset_data <- data[index_year == as.character(y),]
      
      subset_result <- cbind(y, computePercentageGroupTreated(subset_data, eventCohortIds, groupCombinations, outputFolder))
    }) 
    
    result <- rbindlist(results)
    result$y <- as.character(result$y)
  }
  
  write.csv(result, file=outputFile, row.names = FALSE)
  ParallelLogger::logInfo("outputPercentageGroupTreated done")
}

#' Title
#'
#' @param file_noyear 
#' @param path 
#' @param targetCohortId 
#'
#' @return
#' @export
#'
#' @examples
outputStepUpDown <- function(file_noyear, path, targetCohortId) { 
  
  # Replace & signs by + (so that definitions match both)
  file_noyear <- data.table(apply(file_noyear, 2, function(x) 
  {
    x <- gsub("&", "+", x, fixed = TRUE)
    
    # Order the combinations (again)
    concept_names <- strsplit(x, split="+", fixed=TRUE)
    x <- sapply(concept_names, function(c) paste(sort(c), collapse = "+"))
    
    # Fill gaps with NA
    x[x==""] <- NA
  
    return(x)
  }))
  
  file_noyear$freq <- as.numeric(file_noyear$freq)
  
  stepUpDown(file_noyear, path, targetCohortId, input = "manual rules")
  stepUpDown(file_noyear, path, targetCohortId, input = "generalized rules")
  
  ParallelLogger::logInfo("outputStepUpDown done")
} 


#' Title
#'
#' @param file 
#' @param path 
#' @param targetCohortId 
#' @param input 
#'
#' @return
#' @export
#'
#' @examples
stepUpDown <- function(file, path, targetCohortId, input) {
  
  if (input == "manual rules") {
    
    def_updown <- read.csv("inst/Settings/augment_switch.csv", stringsAsFactors = FALSE)
    
    # Order the definition columns from/to
    from <- strsplit(def_updown$from, split="+", fixed=TRUE)
    def_updown$from <- sapply(from, function(c) paste(sort(c), collapse = "+"))
    
    to <- strsplit(def_updown$to, split="+", fixed=TRUE)
    def_updown$to <- sapply(to, function(c) paste(sort(c), collapse = "+"))
    
    def_groups <- as.vector(unique(def_updown$targetCohortIds))
    
    # Define set of rules
    done <- FALSE
    counter <- 1
    
    while(!done & counter <= length(def_groups)) {
      if(targetCohortId %in% strsplit(def_groups[counter], split=",")[[1]]) {
        done <- TRUE
        group <- def_groups[counter]
      } else {
        counter <- counter + 1
      }
    }
    
    if (!done) {
      warning(paste0("No definition for augment/switching for target cohort ", targetCohortId))
      
    } else {
      
      # Select definitions for this target cohort
      def_target <- def_updown[def_updown$targetCohortIds == group,]
      def_target$targetCohortIds <- NULL
      
      all_results <- data.frame()
      
      # For each treament layer determine total: step up / step down / switching / undefined
      for (l in 1:(ncol(file)-2)) {
        
        cols <- c(l, l+1, ncol(file))
        subfile <- file[, ..cols]
        colnames(subfile) <- c("from", "to", "freq")
        
        result <- merge(subfile, def_target, by.x=c("from","to"), by.y=c("from","to"), all.x = TRUE)
        
        # Remove paths of inviduals who already stopped treatment
        result <- result[!is.na(result$from),]
        
        # Define stop treatment
        result$group[is.na(result$to)] <- 'stopped'
        
        # Fill NA's with 'undefined'
        result$group[is.na(result$group)] <- 'undefined'
        
        # Compute augment/switch
        result$freq <- as.integer(result$freq)
        total <- sum(result$freq)
        result <- result[,.(count = sum(freq), perc = round(sum(freq)*100/total,4)), by = "group"]
        result$layer <- l
        
        all_results <- rbind(all_results, result)
      }
      
      write.csv(all_results, paste(path,"_augmentswitch_manual.csv",sep=''), row.names = FALSE) 
    }
    
  } else if (input == "generalized rules") {
    
    
    # Drug class levels
    if (targetCohortId %in% c(1,4,5,3)) { # Asthma, ACO
      class_level <- list("SABA" = 1,
                          "SAMA" = 1,
                          "Systemic B2" = 1,
                          "ICS" = 2,
                          "LTRA" = 2,
                          "PDE4" = 2,
                          "Xanthines" = 2,
                          "LABA" = 3,
                          "LAMA" = 3,
                          "Systemic glucocorticoids (therapy)" = 4,
                          "Systemic glucocorticoids (acute)" = 4,
                          "Anti IL5" = 4,
                          "Anti Ig5" = 4,
                          "Anti IL4R" = 4) 
      
    } else if (targetCohortId %in% c(2)) { # COPD
      class_level <- list("SABA" = 1,
                          "SAMA" = 1,
                          "Systemic B2" = 1,
                          "LABA" = 2,
                          "LAMA" = 2,
                          "Xanthines" = 2,
                          "ICS" = 3,
                          "LTRA" = 3,
                          "PDE4" = 3,
                          "Systemic glucocorticoids (therapy)" = 4,
                          "Systemic glucocorticoids (acute)" = 4,
                          "Anti IL5" = 4,
                          "Anti Ig5" = 4,
                          "Anti IL4R" = 4)
    }
    
    all_results <- data.frame()
    
    # For each treament layer determine total: step up / step down / switching / undefined
    for (l in 1:(ncol(file)-2)) { # l <- 1
      
      cols <- c(l, l+1, ncol(file))
      subfile <- file[, ..cols]
      colnames(subfile) <- c("from", "to", "freq")
      
      subfile[is.na(subfile)] <- "End"
      
      # Start with NA labels
      subfile$group <- "NA"
      
      # Compute patient treatment level (sum of drug class levels)
      subfile$from_level <- sapply(subfile$from, function(r) sum(unlist(sapply(unlist(strsplit(r, split="+", fixed=TRUE)), function(r_s) class_level[[r_s]]))))
      subfile$to_level <- sapply(subfile$to, function(r) sum(unlist(sapply(unlist(strsplit(r, split="+", fixed=TRUE)), function(r_s) class_level[[r_s]]))))
      
      # Cap at level 4 (above everything is considered switching)
      subfile$from_level[subfile$from_level > 4] <- 4
      subfile$to_level[subfile$to_level > 4] <- 4
      
      # Assign labels based on higher/lower/same patient treatment level 
      subfile$group[subfile$from_level < subfile$to_level] <- "step_up_broad"
      subfile$group[subfile$from_level > subfile$to_level] <- "step_down_broad"
      subfile$group[subfile$from_level == subfile$to_level] <- "switching_broad"
      
      # Exceptions: short period of systemic glucocorticoids (acute)
      # Start exacerbation (from NO acute, to YES acute)
      selection <- !stringr::str_detect(subfile$from, stringr::fixed("Systemic glucocorticoids (acute)")) & stringr::str_detect(subfile$to, stringr::fixed("Systemic glucocorticoids (acute)"))
      ParallelLogger::logInfo(paste0("Start exacerbation: ", sum(selection)))
      subfile$group[selection] <- "acute_exacerbation"
      
      # End exacerbation (from YES acute, to NO acute)
      selection <- stringr::str_detect(subfile$from, stringr::fixed("Systemic glucocorticoids (acute)")) & !stringr::str_detect(subfile$to, stringr::fixed("Systemic glucocorticoids (acute)"))
      ParallelLogger::logInfo(paste0("End exacerbation: ", sum(selection)))
      subfile$group[selection] <- "end_of_acute_exacerbation"
      
      # Exceptions: some off label use
      if (targetCohortId %in% c(1,4,5)) { # Asthma
        # Off label (PDE4)
        selection <- stringr::str_detect(subfile$to, stringr::fixed("PDE4"))
        
        ParallelLogger::logInfo(paste0("Off label (asthma): ", sum(selection)))
        subfile$group[selection] <- "off_label"
      } else if (targetCohortId %in% c(2)) { # COPD
        # Off label (LTRA, Anti IL5, Anti IgE)
        selection <- stringr::str_detect(subfile$to, stringr::fixed("Anti IL5")) |
          stringr::str_detect(subfile$to, stringr::fixed("Anti IgE")) |
          stringr::str_detect(subfile$to, stringr::fixed("Anti IL4R"))
        
        ParallelLogger::logInfo(paste0("Off label (COPD): ", sum(selection)))
        subfile$group[selection] <- "off_label"
      }
      
      # Exceptions: non conform treatment
      if (targetCohortId %in% c(1,4,5)) { # Asthma
        # Non conform (therapy LABA/LAMA without ICS)
        selection <-  (stringr::str_detect(subfile$to, stringr::fixed("LABA"))  & !stringr::str_detect(subfile$to, stringr::fixed("ICS")))  |
          (stringr::str_detect(subfile$to, stringr::fixed("LAMA"))  & !stringr::str_detect(subfile$to, stringr::fixed("ICS")))
        
        ParallelLogger::logInfo(paste0("Non conform: ", sum(selection)))
        subfile$group[selection] <- "non_conform"
      }
      
      # Remove paths of inviduals who already stopped treatment
      subfile <- subfile[subfile$from != "End",]
      
      # Define stop treatment
      subfile$group[subfile$to == "End"] <- 'stopped'
      
      # Fill NA's with 'undefined'
      subfile$group[is.na(subfile$group)] <- 'undefined'
      
      # Compute augment/switch
      total <- sum(subfile$freq)
      subfile <- subfile[,.(count = sum(freq), perc = round(sum(freq)*100/total,4)), by = "group"]
      subfile$layer <- l
      
      all_results <- rbind(all_results, subfile)
    }
    
    write.csv(all_results, paste(path,"_augmentswitch_generalized.csv",sep=''), row.names = FALSE) 
    
    
  }
}

#' Title
#'
#' @param data 
#' @param eventCohortIds 
#' @param groupCombinations 
#' @param outputFolder 
#'
#' @return
#' @export
#'
#' @examples
computePercentageGroupTreated <- function(data, eventCohortIds, groupCombinations, outputFolder) {
  layers <- as.vector(colnames(data)[!grepl("index_year|freq", colnames(data))])
  cohorts <- read.csv(paste(outputFolder, "/cohort.csv",sep=''), stringsAsFactors = FALSE)
  outcomes <- c(cohorts$cohortName[cohorts$cohortType == "outcome"], "Other")
  
  # Group non-fixed combinations in one group according to groupCobinations
  data <- groupInfrequentCombinations(data, groupCombinations)
  
  percentGroupLayer <- sapply(layers, function(l) {
    percentGroup <- sapply(outcomes, function(g) {
      sumGroup <- sum(data$freq[data[,..l] == g], na.rm = TRUE)
      sumAllNotNA <- sum(data$freq[!is.na(data[,..l])])
      
      result <- sumGroup * 100.0 / sumAllNotNA
    })
  })
  
  # Add outcome names
  result <- data.frame(outcomes, percentGroupLayer, stringsAsFactors = FALSE)
  colnames(result) <- c("outcomes", layers)
  rownames(result) <- NULL
  
  paths_all <- sum(data$freq)
  
  result$ALL_LAYERS <- sapply(outcomes, function(o) {
    paths_with_outcome <- sum(sapply(1:nrow(data), function(r) ifelse(o %in% data[r,], data[r,freq], 0)))
    return(paths_with_outcome * 100.0 / paths_all)
  })
  
  # Add rows for total, fixed combinations, all combinations
  result <- rbind(result, c("Fixed combinations",colSums(result[grepl("\\&", result$outcomes), layers]), NA), c("All combinations",colSums(result[grepl("Other|\\+|\\&", result$outcomes), layers]), NA), c("Monotherapy",colSums(result[!grepl("Other|\\+|\\&", result$outcomes), layers]), NA))
  
  result$ALL_LAYERS[result$outcomes == "Fixed combinations"] <- sum(sapply(1:nrow(data), function(r) ifelse(grepl("\\&", data[r,]), data[r,freq], 0))) * 100.0 / paths_all
  result$ALL_LAYERS[result$outcomes == "All combinations"] <- sum(sapply(1:nrow(data), function(r) ifelse(grepl("Other|\\+|\\&", data[r,]), data[r,freq], 0))) * 100.0 / paths_all
  result$ALL_LAYERS[result$outcomes == "Monotherapy"] <- sum(sapply(1:nrow(data), function(r) ifelse(grepl("Other|\\+|\\&", data[r,]), data[r,freq], 0))) * 100.0 / paths_all
  
  return(result)
}

#' Title
#'
#' @param connection 
#' @param cohortDatabaseSchema 
#' @param outputFolder 
#' @param dbms 
#' @param studyName 
#' @param databaseName 
#' @param path 
#' @param maxPathLength 
#' @param groupCombinations 
#' @param minCellCount 
#' @param removePaths 
#'
#' @return
#' @export
#'
#' @examples
transformDuration <- function(outputFolder, studyName, databaseName, path, temp_path, maxPathLength, groupCombinations, minCellCount, removePaths) {
  
  # file <- data.table(extractFile(connection, tableName = "drug_seq_processed", resultsSchema = cohortDatabaseSchema, studyName = studyName,databaseName = databaseName, dbms = connectionDetails$dbms))
  file <- data.table(read.csv(paste(temp_path,"_drug_seq_processed.csv",sep=''), stringsAsFactors = FALSE))
  
  # Remove unnessary columns
  columns <- c("duration_era", "drug_seq", "concept_name")
  file <- file[,..columns]
  file$duration_era <- as.numeric(file$duration_era)
  
  # Apply maxPathLength
  file <- file[drug_seq <= maxPathLength,]
  
  # Group non-fixed combinations in one group according to groupCobinations
  # TODO: change to function
  findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
  file[findCombinations] <- "Other"
  
  result <- file[,.(AVG_DURATION=round(mean(duration_era),3), COUNT = .N), by = c("concept_name", "drug_seq")][order(concept_name, drug_seq)]
  
  # Add column for total treated, fixed combinations, all combinations
  file$total <- 1
  file$fixed_combinations[grepl("\\&", file$concept_name)] <- 1
  file$all_combinations[grepl("Other|\\+|\\&", file$concept_name)] <- 1
  file$monotherapy[!grepl("Other|\\+|\\&", file$concept_name)] <- 1
  
  result_total_seq <- file[,.(drug_seq = "Overall", AVG_DURATION= round(mean(duration_era),2), COUNT = .N), by = c("concept_name", "total")]
  result_total_seq$total <- NULL
  
  result_total_concept <- file[,.(concept_name = "Total treated", AVG_DURATION= round(mean(duration_era),2), COUNT = .N), by = c("drug_seq", "total")]
  result_total_concept$total <- NULL
  
  result_fixed_combinations <- file[,.(concept_name = "Fixed combinations", AVG_DURATION= round(mean(duration_era),2), COUNT = .N), by = c("drug_seq", "fixed_combinations")]
  result_fixed_combinations <- result_fixed_combinations[!is.na(fixed_combinations),]
  result_fixed_combinations$fixed_combinations <- NULL
  
  result_all_combinations <- file[,.(concept_name = "All combinations", AVG_DURATION=round(mean(duration_era),2), COUNT = .N), by = c("drug_seq", "all_combinations")]
  result_all_combinations <- result_all_combinations[!is.na(all_combinations),]
  result_all_combinations$all_combinations <- NULL
  
  result_monotherapy <- file[,.(concept_name = "Monotherapy", AVG_DURATION=round(mean(duration_era),2), COUNT = .N), by = c("drug_seq", "monotherapy")]
  result_monotherapy <- result_monotherapy[!is.na(monotherapy),]
  result_monotherapy$monotherapy <- NULL
  
  results <- rbind(result, result_total_seq, result_total_concept, result_fixed_combinations, result_all_combinations, result_monotherapy)
  
  # Add missing groups
  cohorts <- read.csv(paste(outputFolder, "/cohort.csv",sep=''), stringsAsFactors = FALSE)
  outcomes <- c(cohorts$cohortName[cohorts$cohortType == "outcome"], "Other")
  
  for (o in outcomes[!(outcomes %in% results$concept_name)]) {
    results <- rbind(results, list(o, "Overall", 0.0, 0))
  }

  # Remove durations computed using less than minCellCount observations
  results[COUNT < minCellCount,c("AVG_DURATION", "COUNT")] <- NA
  
  write.csv(results,  paste(path,"_duration.csv",sep=''), row.names = FALSE)
  
  ParallelLogger::logInfo("transformDuration done")
}

#' Title
#'
#' @param data 
#' @param databaseName 
#' @param eventCohortIds 
#' @param studyName 
#' @param outputFolder 
#' @param path 
#' @param groupCombinations 
#' @param addNoPaths 
#' @param maxPathLength 
#' @param createInput 
#' @param createPlot 
#'
#' @return
#' @export
#'
#' @examples
outputSunburstPlot <- function(data, databaseName, eventCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, createInput, createPlot) {
  if (is.null(data$index_year)) {
    # For file_noyear compute once
     createSunburstPlot(data, databaseName, eventCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year = 'all', createInput, createPlot)
    
  } else {
    # For file_withyear compute per year
    years <- unlist(unique(data[,"index_year"]))
    
    for (y in years) {
      subset_data <- data[index_year == as.character(y),]
      createSunburstPlot(subset_data, databaseName, eventCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year = y, createInput, createPlot)
    }
  }
  
  ParallelLogger::logInfo("outputSunburstPlot done")
}

#' Title
#'
#' @param data 
#' @param databaseName 
#' @param eventCohortIds 
#' @param studyName 
#' @param outputFolder 
#' @param path 
#' @param groupCombinations 
#' @param addNoPaths 
#' @param maxPathLength 
#' @param index_year 
#' @param createInput 
#' @param createPlot 
#'
#' @return
#' @export
#'
#' @examples
createSunburstPlot <- function(data, databaseName, eventCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year, createInput, createPlot){
  
  if (createInput) {
    inputSunburstPlot(data, path, addNoPaths, index_year)
  }
  
  if (createPlot) {
    
    transformCSVtoJSON(eventCohortIds, outputFolder, path, index_year, maxPathLength)
    
    # Load template HTML file
    html <- paste(readLines("shiny/sunburst/sunburst.html"), collapse="\n")
    
    # Replace @insert_data
    input_plot <- readLines(paste(path,"_inputsunburst_", index_year, ".txt",sep=''))
    html <- sub("@insert_data", input_plot, html)
    
    # Replace @name
    html <- sub("@name", paste0("(", databaseName, " ", studyName," ", index_year, ")"), html)
    
    # Save HTML file as sunburst_@studyName
    write.table(html, 
                file=paste0(outputFolder, "/", studyName, "/", "sunburst_", databaseName, "_", studyName,"_", index_year,".html"), 
                quote = FALSE,
                col.names = FALSE,
                row.names = FALSE)
    
  }
}


#' Title
#'
#' @param data 
#' @param path 
#' @param groupCombinations 
#' @param addNoPaths 
#' @param index_year 
#'
#' @return
#' @export
#'
#' @examples
inputSunburstPlot <- function(data, path, addNoPaths, index_year) {
  
  layers <- as.vector(colnames(data)[!grepl("INDEX_YEAR|freq", colnames(data))])
  
  transformed_file <- apply(data[,..layers],1, paste, collapse = "-")
  transformed_file <- stringr::str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=data$freq, stringsAsFactors = FALSE)
  
  if (addNoPaths) {
    summary_counts <- read.csv(paste(path,"_summary_cnt.csv",sep=''), stringsAsFactors = FALSE)
    
    if (index_year == "all") {
      noPath <- as.integer(summary_counts[summary_counts$index_year == "Number of persons in target cohort NA", "N"]) - sum(transformed_file$freq)
    } else {
      noPath <- as.integer(summary_counts[summary_counts$index_year == paste0("Number of persons in target cohort ", index_year), "N"]) - sum(transformed_file$freq)
    }
    
    transformed_file <- rbind(transformed_file, c("End", noPath))
  }
  
  transformed_file$path <- as.factor(transformed_file$path)
  transformed_file$freq <- as.integer(transformed_file$freq)
  transformed_file <- transformed_file[order(-transformed_file$freq, transformed_file$path),]
  
  write.table(transformed_file, file=paste(path,"_inputsunburst_", index_year, ".csv",sep=''), sep = ",", row.names = FALSE)
}

#' Title
#'
#' @param eventCohortIds 
#' @param outputFolder 
#' @param path 
#' @param index_year 
#' @param maxPathLength 
#'
#' @return
#' @export
#'
#' @examples
transformCSVtoJSON <- function(eventCohortIds, outputFolder, path, index_year, maxPathLength) {
  data <- read.csv(paste(path,"_inputsunburst_", index_year, ".csv",sep=''))
  
  cohorts <- read.csv(paste(outputFolder, "/cohort.csv",sep=''), stringsAsFactors = FALSE)
  outcomes <- c(cohorts$cohortName[cohorts$cohortType == "outcome"], "Other")
  
  # Add bitwise numbers to define combination treatments
  bitwiseNumbers <- sapply(1:length(outcomes), function(o) {2^(o-1)})
  linking <- data.frame(outcomes,bitwiseNumbers)
  
  # Generate lookup file
  series <- sapply(1:nrow(linking), function (row) {
    paste0('{ "key": "', linking$bitwiseNumbers[row] ,'", "value": "', linking$outcomes[row],'"}')
  })
  
  series <- c(series, '{ "key": "End", "value": "End"}')
  lookup <- paste0("[", paste(series, collapse = ","), "]")
  
  # Order names from longest to shortest to adjust in the right order
  linking <- linking[order(-sapply(linking$outcomes, function(x) stringr::str_length(x))),]
  
  # Apply linking
  # Change all outcomes to bitwise number
  updated_path <- sapply(data$path, function(p) {
    stringi::stri_replace_all_fixed(p, replacement = as.character(linking$bitwiseNumbers), pattern = as.character(linking$outcomes), vectorize = FALSE)
  })
  
  # Sum the bitwise numbers of combinations (indicated by +)
  updated_path <- sapply(updated_path, function(p) {
    while(!is.na(stringr::str_extract(p, "[[:digit:]]+[+][[:digit:]]+"))) {
      pattern <- stringr::str_extract(p, "[[:digit:]]+[+][[:digit:]]+")
      
      p <- sub("[[:digit:]]+[+][[:digit:]]+", eval(parse(text=pattern)), p)
    }
    return(p)
  })
  
  transformed_csv <- cbind(oath = updated_path, freq = data$freq)
  transformed_json <- buildHierarchy(transformed_csv, maxPathLength) 
  
  result <- paste0("{ \"data\" : ", transformed_json, ", \"lookup\" : ", lookup, "}")
  
  file <- file(paste(path,"_inputsunburst_", index_year, ".txt",sep=''))
  writeLines(result, file)
  close(file)
}

#' Title
#'
#' @param csv 
#' @param maxPathLength 
#'
#' @return
#' @export
#'
#' @examples
buildHierarchy <- function(csv, maxPathLength) {
  
  if (maxPathLength > 5) {
    stop(paste0("MaxPathLength exceeds 5, currently not supported in buildHierarchy function."))
  }
  
  root = list("name" = "root", "children" = list())
  
  # create nested structure of lists 
  for (i in 1:nrow(csv)) {
    sequence = csv[i,1]
    size = csv[i,2]
    
    parts = unlist(stringr::str_split(sequence,pattern="-"))
    
    currentNode = root
    
    for (j in 1:length(parts)) {
      children = currentNode[["children"]]
      nodeName = parts[j]
      
      if (j < length(parts)) {
        # not yet at the end of the sequence; move down the tree.
        foundChild = FALSE
        
        if (length(children) != 0) {
          for (k in 1:length(children)) {
            if (children[[k]]$name == nodeName) {
              childNode = children[[k]]
              foundChild = TRUE
              break
            }
          }
        }
        
        # if we dont already have a child node for this branch, create it.
        if (!foundChild) {
          childNode = list("name" = nodeName, "children" = list())
          children[[nodeName]] <- childNode
          
          # add to main root
          if (j == 1) {
            # root$children <- children
            root[['children']] <- children
          } else if (j == 2) {
            root[['children']][[parts[1]]][['children']] <- children
          } else if (j == 3) {
            root[['children']][[parts[1]]][['children']][[parts[2]]][['children']] <- children
          } else if (j == 4) {
            root[['children']][[parts[1]]][['children']][[parts[2]]][['children']][[parts[3]]][['children']] <- children
          } else if (j == 5) {
            root[['children']][[parts[1]]][['children']][[parts[2]]][['children']][[parts[3]]][['children']][[parts[4]]][['children']]  <- children
          }
        }
        currentNode = childNode
      } else {
        # reached the end of the sequence; create a leaf node.
        childNode = list("name" = nodeName, "size" = size)
        children[[nodeName]] <- childNode
        
        # add to main root
        if (j == 1) {
          root[['children']] <- children
        } else if (j == 2) {
          root[['children']][[parts[1]]][['children']] <- children
        } else if (j == 3) {
          root[['children']][[parts[1]]][['children']][[parts[2]]][['children']] <- children
        } else if (j == 4) {
          root[['children']][[parts[1]]][['children']][[parts[2]]][['children']][[parts[3]]][['children']] <- children
        } else if (j == 5) {
          root[['children']][[parts[1]]][['children']][[parts[2]]][['children']][[parts[3]]][['children']][[parts[4]]][['children']]  <- children
        }
      }
    }
  }
  
  # remove list names
  root <- suppressWarnings(stripname(root, "children"))
  
  # convert nested list structure to json
  json <- rjson::toJSON(root)
  
  return(json)
}

#' Title
#'
#' @param data 
#' @param databaseName 
#' @param studyName 
#'
#' @return
#' @export
#'
#' @examples
createSankeyDiagram <- function(data, databaseName, studyName) {
  
  # Group all non-fixed combinations in one group
  findCombinations <- apply(data, 2, function(x) grepl("+", x, fixed = TRUE))
  data[findCombinations] <- "Other"
  
  # Sankey diagram for first three treatment layers
  data$D1_CONCEPT_NAME <- paste0("1. ",data$D1_CONCEPT_NAME)
  data$D2_CONCEPT_NAME <- paste0("2. ",data$D2_CONCEPT_NAME)
  data$D3_CONCEPT_NAME <- paste0("3. ",data$D3_CONCEPT_NAME)
  
  results1 <- data %>% 
    dplyr::group_by(D1_CONCEPT_NAME,D2_CONCEPT_NAME) %>% 
    dplyr::summarise(freq = sum(freq))
  
  results2 <- data %>% 
    dplyr::group_by(D2_CONCEPT_NAME,D3_CONCEPT_NAME) %>% 
    dplyr::summarise(freq = sum(freq))
  
  # Format in prep for sankey diagram
  colnames(results1) <- c("source", "target", "value")
  colnames(results2) <- c("source", "target", "value")
  links <- as.data.frame(rbind(results1,results2))
  
  # Create nodes dataframe
  labels <- unique(as.character(c(links$source,links$target)))
  nodes <- data.frame(node = c(0:(length(labels)-1)), 
                      name = c(labels))
  
  # Change labels to numbers
  links <- merge(links, nodes, by.x = "source", by.y = "name")
  links <- merge(links, nodes, by.x = "target", by.y = "name")
  links <- links[ , c("node.x", "node.y", "value")]
  colnames(links) <- c("source", "target", "value")
  
  # Draw sankey network
  plot <- networkD3::sankeyNetwork(Links = links, Nodes = nodes, 
                                   Source = 'source', 
                                   Target = 'target', 
                                   Value = 'value', 
                                   NodeID = 'name',
                                   units = 'votes')
  networkD3::saveNetwork(plot, file=paste0("sankeydiagram_", databaseName,"_", studyName, "_all.html"), selfcontained=TRUE)
  # there seems to be an error in this package -> cannot change path to output folder
  
  ParallelLogger::logInfo("createSankeyDiagram done")
}


#' Title
#'
#' @param data 
#' @param groupCombinations 
#'
#' @return
#' @export
#'
#' @examples
groupInfrequentCombinations <- function(data, groupCombinations)  {
  
  # Find all non-fixed combinations occurring
  findCombinations <- apply(data, 2, function(x) grepl("+", x, fixed = TRUE))
  
  # Group all non-fixed combinations in one group if TRUE
  if (groupCombinations == "TRUE") {
    data[findCombinations] <- "Other"
  } else {
    # Otherwise: group infrequent treatments below groupCombinations as "other"
    combinations <- as.matrix(data)[findCombinations == TRUE]
    num_columns <-  sum(grepl("cohort_name", colnames(data)))
    freqCombinations <- matrix(rep(data$freq, times = num_columns), ncol = num_columns)[findCombinations == TRUE]
    
    summaryCombinations <- data.table(combination = combinations, freq = freqCombinations)
    summaryCombinations <- summaryCombinations[,.(freq=sum(freq)), by=combination][order(-freq)]
    
    summarizeCombinations <- summaryCombinations$combination[summaryCombinations$freq <= groupCombinations]
    selectedCombinations <- apply(data, 2, function(x) x %in% summarizeCombinations)
    data[selectedCombinations] <- "Other"
  }
  
  return(data)
}
