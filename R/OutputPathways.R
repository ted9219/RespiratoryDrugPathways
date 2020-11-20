
transformTreatmentSequence <- function(studyName, databaseName, path, maxPathLength, minCellCount, removePaths, otherCombinations) {
  
  file <- data.table(extractFile(connection, tableName = "drug_seq_summary", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName, dbms = connectionDetails$dbms))

  # Remove unnessary columns
  columns <- colnames(file)[!grepl("CONCEPT_ID", colnames(file))]
  file <- file[,..columns]
  
  # Group all non-fixed combinations in one group if TRUE
  if (otherCombinations) {
    findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
    
    otherCombinations <- as.matrix(file)[findCombinations == TRUE]
    num_columns <-  sum(grepl("CONCEPT_NAME", colnames(file)))
    freqCombinations <- matrix(rep(file$NUM_PERSONS, times =num_columns), ncol = num_columns)[findCombinations == TRUE]
    
    summaryCombinations <- data.table(combination = otherCombinations, freq = freqCombinations)
    summaryCombinations <- summaryCombinations[,.(freq=sum(freq)), by=combination][order(-freq)]
    summaryCombinations <- summaryCombinations[freq >= minCellCount,]
    
    write.csv(summaryCombinations, file=paste(path,"_othercombinations.csv",sep=''), row.names = FALSE)
    
    file[findCombinations] <- "Other combinations"
  }
  
  # Apply maxPathLength and group the resulting treatment paths
  layers <- as.vector(colnames(file)[!grepl("INDEX_YEAR|NUM_PERSONS", colnames(file))])
  layers <- layers[1:maxPathLength]
  
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=layers]
  file_withyear <- file[,.(freq=sum(NUM_PERSONS)), by=c(layers, "INDEX_YEAR")]
  
  # Apply minCellCount by removing complete path or adding to other most similar path (removing last treatment in path)
  if (removePaths != "TRUE") {
    col <- ncol(file_noyear) - 1
    while (sum(file_noyear$freq < minCellCount) > 0 & col !=0) {
      writeLines(paste("Change col ", col, " to NA for ", sum(file_noyear$freq < minCellCount), " paths with too low frequency (without year)"))
      
      file_noyear[freq < minCellCount,col] <- NA
      file_noyear <- file_noyear[,.(freq=sum(freq)), by=layers]
      
      col <- col - 1
    }
    
    col <- ncol(file_withyear) - 2
    while (sum(file_withyear$freq < minCellCount) > 0 & col !=0) {
      writeLines(paste("Change col ", col, " to NA for ", sum(file_withyear$freq < minCellCount), " paths with too low frequency (with year)"))
      
      file_withyear[freq < minCellCount,col] <- NA
      file_withyear <- file_withyear[,.(freq=sum(freq)), by=c(layers, "INDEX_YEAR")]
      
      col <- col - 1
    }
  }
  
  writeLines(paste("Remove ", sum(file_noyear$freq < minCellCount), " paths with too low frequency (without year)"))
  file_noyear <- file_noyear[freq >= minCellCount,]
  
  writeLines(paste("Remove ", sum(file_withyear$freq < minCellCount), " paths with too low frequency (with year)"))
  file_withyear <- file_withyear[freq >= minCellCount,]
  
  # summary_counts <- read.csv(paste(path,"_summary.csv",sep=''), stringsAsFactors = FALSE)
  # summary_counts <- rbind(summary_counts, c(4,   'Number of pathways final (after minCellCount)', sum(transformed_file$freq)  ))
  # write.table(summary_counts,file=paste(path,"_summary.csv",sep=''), sep = ",", row.names = FALSE, col.names = TRUE)
  
  write.csv(file_noyear,  paste(path,"_file_noyear.csv",sep=''), row.names = FALSE)
  write.csv(file_withyear,  paste(path,"_file_withyear.csv",sep=''), row.names = FALSE)
}

outputPercentageGroupTreated <- function(data, outcomeCohortIds, path, outputFolder, outputFile) {
  if (is.null(data$INDEX_YEAR)) {
    # For file_noyear compute once
    result <- computePercentageGroupTreated(data, outcomeCohortIds, outputFolder)
    
  } else {
    # For file_withyear compute per year
    years <- unlist(unique(data[,"INDEX_YEAR"]))
    
    results <- lapply(years, function(y) {
      subset_data <- data[INDEX_YEAR == as.character(y),]
      
      subset_result <- cbind(y, computePercentageGroupTreated(subset_data, outcomeCohortIds, outputFolder))
    }) 
    
    result <- rbindlist(results)
    # colnames(result[,1:2]) <- c("INDEX_YEAR" ,"outcomes")
    result$y <- as.character(result$y)
  }
  
  write.csv(result, file=outputFile, row.names = FALSE)
}

computePercentageGroupTreated <- function(data, outcomeCohortIds, outputFolder) {
  layers <- as.vector(colnames(data)[!grepl("INDEX_YEAR|freq", colnames(data))])
  cohorts <- read.csv(paste(outputFolder, "/cohort.csv",sep=''), stringsAsFactors = FALSE)
  outcomes <- cohorts$cohortName[cohorts$cohortId %in% unlist(strsplit(outcomeCohortIds, ","))]
  
  percentGroupLayer <- sapply(layers, function(l) {
    percentGroup <- sapply(outcomes, function(g) {
      sumGroup <- sum(data$freq[data[,..l] == g], na.rm = TRUE)
      sumAllNotNA <- sum(data$freq[!is.na(data[,..l])])
      
      result <- round(sumGroup * 100.0 / sumAllNotNA,3)
    })
  })
  
  # Add outcome names
  result <- data.frame(outcomes, percentGroupLayer, stringsAsFactors = FALSE)
  colnames(result) <- c("outcomes", layers)
  rownames(result) <- NULL
  
  # Add rows for total, fixed combinations, all combinations
  result <- rbind(result, c("Total",colSums(result[layers])), c("Fixed combinations",colSums(result[grepl("\\&", result$outcomes), layers])), c("All combinations",colSums(result[grepl("Other combinations|\\+|\\&", result$outcomes), layers])))
}



transformDuration <- function(studyName, databaseName, path, maxPathLength, minCellCount, removePaths, otherCombinations) {
  
  file <- data.table(extractFile(connection, tableName = "drug_seq_processed", resultsSchema = cohortDatabaseSchema, studyName = studyName,databaseName = databaseName, dbms = connectionDetails$dbms))
  
  # Remove unnessary columns
  columns <- c("DURATION_ERA", "DRUG_SEQ", "CONCEPT_NAME" )
  file <- file[,..columns]
  file$DURATION_ERA <- as.numeric(file$DURATION_ERA)
  
  # Group all non-fixed combinations in one group if TRUE
  if (otherCombinations) {
    findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
    file[findCombinations] <- "Other combinations"
  }
  
  # Apply maxPathLength
  file <- file[DRUG_SEQ <= maxPathLength,]
  
  # Add column for total, fixed combinations, all combinations
  file$total <- 1
  file$fixed_combinations[grepl("\\&", file$CONCEPT_NAME)] <- 1
  file$all_combinations[grepl("Other combinations|\\+|\\&", file$CONCEPT_NAME)] <- 1

  result <- file[,.(AVG_DURATION=round(mean(DURATION_ERA),3), COUNT = .N), by = c("CONCEPT_NAME", "DRUG_SEQ")][order(CONCEPT_NAME, DRUG_SEQ)]

  result_total_seq <- file[,.(DRUG_SEQ = "Total", AVG_DURATION= round(mean(DURATION_ERA),3), COUNT = .N), by = c("CONCEPT_NAME", "total")]
  result_total_seq$total <- NULL
  
  result_total_concept <- file[,.(CONCEPT_NAME = "Total", AVG_DURATION= round(mean(DURATION_ERA),3), COUNT = .N), by = c("DRUG_SEQ", "total")]
  result_total_concept$total <- NULL

  result_fixed_combinations <- file[,.(CONCEPT_NAME = "Fixed combinations", AVG_DURATION= round(mean(DURATION_ERA),3), COUNT = .N), by = c("DRUG_SEQ", "fixed_combinations")]
  result_fixed_combinations <- result_fixed_combinations[!is.na(fixed_combinations),]
  result_fixed_combinations$fixed_combinations <- NULL
  
  result_all_combinations <- file[,.(CONCEPT_NAME = "All combinations", AVG_DURATION=round(mean(DURATION_ERA),3), COUNT = .N), by = c("DRUG_SEQ", "all_combinations")]
  result_all_combinations <- result_all_combinations[!is.na(all_combinations),]
  result_all_combinations$all_combinations <- NULL
  
  results <- rbind(result, result_total_seq, result_total_concept, result_fixed_combinations, result_all_combinations)
  
  # Remove durations computed using less than minCellCount observations
  results[COUNT < minCellCount,c("AVG_DURATION", "COUNT")] <- NA

  write.csv(results,  paste(path,"_duration.csv",sep=''), row.names = FALSE)
}



outputSunburstPlot <- function(data, studyName, path, addNoPaths) {
  if (is.null(data$INDEX_YEAR)) {
    # For file_noyear compute once
    result <- createSunburstPlot(data, studyName, path, addNoPaths, index_year = 'all')
    
  } else {
    # For file_withyear compute per year
    years <- unlist(unique(data[,"INDEX_YEAR"]))
    
    for (y in years) {
      subset_data <- data[INDEX_YEAR == as.character(y),]
      createSunburstPlot(subset_data, studyName, path, addNoPaths, index_year = y)
    }
  }
}

createSunburstPlot <- function(data, studyName, path, addNoPaths, index_year){

  inputSunburstPlot(data, path, addNoPaths, index_year)
  # todo: convert csv to JSON + reuse OHDSI code with combination treatments
  
  inputFile <- paste(path,"_inputsunburst_", index_year, ".csv",sep='')

  # todo: create files for each year separately
  
  # Load template HTML file
  template_html <- paste(readLines("plots/index_template.html"), collapse="\n")
  
  # Replace @studyName
  html <- sub("@studyName", studyName, template_html)
  
  # Save HTML file as index_@studyName
  write.table(html, 
              file=paste0("plots/index_",studyName,".html"), 
              quote = FALSE,
              col.names = FALSE,
              row.names = FALSE)
  
  # Load template JS file
  template_js <- paste(readLines("plots/sequences_template.js"), collapse="\n")
  
  # Replace @file
  js <- sub("@file", inputFile, template_js)
  
  # Save JS file as sequences_@studyName
  write.table(js, 
              file=paste0("plots/sequences_",studyName,".js"), 
              quote = FALSE,
              col.names = FALSE,
              row.names = FALSE)
  
  # todo: automatically take screenshot of resulting html
  
}


inputSunburstPlot <- function(data, path, addNoPaths, index_year) {
  layers <- as.vector(colnames(data)[!grepl("INDEX_YEAR|freq", colnames(data))])
  
  transformed_file <- apply(data[,..layers],1, paste, collapse = "-")
  transformed_file <- stringr::str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=data$freq, stringsAsFactors = FALSE)
  
  if (addNoPaths) { # todo: change for separate years (but first merge summary + person_cnt file?)
    summary_counts <- read.csv(paste(path,"_summary_cnt.csv",sep=''), stringsAsFactors = FALSE)
    noPath <- as.integer(summary_counts[summary_counts$COUNT_TYPE == "Number of persons in target cohort", "NUM_PERSONS"]) - sum(transformed_file$freq)
    transformed_file <- rbind(transformed_file, c("End", noPath))
  }
  
  transformed_file$path <- as.factor(transformed_file$path)
  transformed_file$freq <- as.integer(transformed_file$freq)
  transformed_file <- transformed_file[order(-transformed_file$freq, transformed_file$path),]
  
  write.table(transformed_file, file=paste(path,"_inputsunburst_", index_year, ".csv",sep=''), sep = ",", row.names = FALSE)
}


createSankeyDiagram <- function(data) {
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
  colnames(results1) <- c("source", "target","value")
  colnames(results2) <- c("source", "target","value")
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
  networkD3::saveNetwork(plot, file="sankeydiagram.html", selfcontained=TRUE)
  # todo: change path (there seems to be an error in this package function)

}
