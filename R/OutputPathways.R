
transformTreatmentSequence <- function(studyName, databaseName, path, maxPathLength, minCellCount, removePaths, otherCombinations) {
  
  file <- data.table(extractFile(connection, tableName = "drug_seq_summary", resultsSchema = cohortDatabaseSchema, studyName = studyName, databaseName = databaseName, dbms = connectionDetails$dbms))
  
  # Apply maxPathLength and remove unnessary columns
  layers <- as.vector(colnames(file)[!grepl("CONCEPT_ID|INDEX_YEAR|NUM_PERSONS", colnames(file))])
  layers <- layers[1:maxPathLength]
  
  columns <- c(layers, "INDEX_YEAR", "NUM_PERSONS")
  
  file <- file[,..columns]
  
  if (nrow(file) == 0) {
    warning(paste0("Data is empty for study settings ", studyName))
    return (TRUE)
  }
  
  # Summarize all non-fixed combinations occuring
  findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
  
  combinations <- as.matrix(file)[findCombinations == TRUE]
  num_columns <-  sum(grepl("CONCEPT_NAME", colnames(file)))
  freqCombinations <- matrix(rep(file$NUM_PERSONS, times = num_columns), ncol = num_columns)[findCombinations == TRUE]
  
  summaryCombinations <- data.table(combination = combinations, freq = freqCombinations)
  summaryCombinations <- summaryCombinations[,.(freq=sum(freq)), by=combination][order(-freq)]
  summaryCombinations <- summaryCombinations[freq >= minCellCount,]
  
  write.csv(summaryCombinations, file=paste(path,"_combinations.csv",sep=''), row.names = FALSE)
  
  # Group all non-fixed combinations in one group if TRUE
  if (otherCombinations) {
    file[findCombinations] <- "Other combinations"
  } else {
    # Otherwise: process combination treatments
    
    # TODO: Group infrequent treatments below 25 as otherCombinations
    
  }
  
  # Group the resulting treatment paths
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
  
  summary_counts <- read.csv(paste(path,"_summary_cnt.csv",sep=''), stringsAsFactors = FALSE)
  summary_counts <- rbind(summary_counts, c("Total number of pathways (after minCellCount)", sum(file_noyear$freq)))
  
  for (y in unique(file_withyear$INDEX_YEAR)) {
    summary_counts <- rbind(summary_counts, c(paste0("Number of pathways (after minCellCount) in ", y), sum(file_withyear$freq[file_withyear$INDEX_YEAR == y])))
  }
  
  write.table(summary_counts,file=paste(path,"_summary_cnt.csv",sep=''), sep = ",", row.names = FALSE, col.names = TRUE)
  
  write.csv(file_noyear,  paste(path,"_file_noyear.csv",sep=''), row.names = FALSE)
  write.csv(file_withyear,  paste(path,"_file_withyear.csv",sep=''), row.names = FALSE)
  
  return(FALSE)
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

outputStepUpDown <- function(file_noyear, path, targetCohortId) { 
  
  # Replace & signs by + (so that definitions match both)
  file_noyear <- data.table(apply(file_noyear, 2, function(x) gsub("&", "+", x, fixed = TRUE)))
  file_noyear$freq <- as.numeric(file_noyear$freq)
  
  def_updown <- read.csv("inst/Settings/augment_switch.csv", stringsAsFactors = FALSE)
  def_groups <- as.vector(unique(def_updown$targetCohortIds))
  
  # Define set of rules # TODO: ACO?
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
    for (l in 1:(ncol(file_noyear)-2)) {
      
      cols <- c(l, l+1, ncol(file_noyear))
      file <- file_noyear[, ..cols]
      colnames(file) <- c("from", "to", "freq")
      
      result <- merge(file, def_target, by.x=c("from","to"), by.y=c("from","to"), all.x = TRUE)
      
      # Fill NA's with 'undefined'
      result$category[is.na(result$category)] <- 'undefined'
      
      # Compute augment/switch
      total <- sum(result$freq)
      result <- result[,.(count = sum(freq), perc = round(sum(freq)*100/total,4)), by = "category"]
      result$layer <- l
      
      all_results <- rbind(all_results, result)
    }
    
    write.csv(all_results, paste(path,"_augmentswitch.csv",sep=''), row.names = FALSE)
  }
  
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

transformDuration <- function(connection, cohortDatabaseSchema, dbms, studyName, databaseName, path, maxPathLength, minCellCount, removePaths, otherCombinations) {
  
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

outputSunburstPlot <- function(data, databaseId, outcomeCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, createInput, createPlot) {
  if (is.null(data$INDEX_YEAR)) {
    # For file_noyear compute once
    result <- createSunburstPlot(data, databaseId, outcomeCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year = 'all', createInput, createPlot)
    
  } else {
    # For file_withyear compute per year
    years <- unlist(unique(data[,"INDEX_YEAR"]))
    
    for (y in years) {
      subset_data <- data[INDEX_YEAR == as.character(y),]
      createSunburstPlot(subset_data, databaseId, outcomeCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year = y, createInput, createPlot)
    }
  }
}

createSunburstPlot <- function(data, databaseId, outcomeCohortIds, studyName, outputFolder, path, addNoPaths, maxPathLength, index_year, createInput, createPlot){
  
  if (createInput) {
    inputSunburstPlot(data, path, addNoPaths, index_year)
  }
  
  if (createPlot) {
    
    transformCSVtoJSON(outcomeCohortIds, outputFolder, path, index_year, maxPathLength)
    
    # Load template HTML file
    html <- paste(readLines("plots/sunburst.html"), collapse="\n")
    
    # Replace @insert_data
    input_plot <- readLines(paste(path,"_inputsunburst_", index_year, ".txt",sep=''))
    html <- sub("@insert_data", input_plot, html)
    
    # Replace @name
    html <- sub("@name", paste0("(", databaseId, " ", studyName," ", index_year, ")"), html)
    
    # Save HTML file as sunburst_@studyName
    write.table(html, 
                file=paste0("plots/sunburst_",paste0(studyName,"_", index_year),".html"), 
                quote = FALSE,
                col.names = FALSE,
                row.names = FALSE)
    
  }
}


inputSunburstPlot <- function(data, path, addNoPaths, index_year) {
  layers <- as.vector(colnames(data)[!grepl("INDEX_YEAR|freq", colnames(data))])
  
  transformed_file <- apply(data[,..layers],1, paste, collapse = "-")
  transformed_file <- stringr::str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=data$freq, stringsAsFactors = FALSE)
  
  if (addNoPaths) {
    summary_counts <- read.csv(paste(path,"_summary_cnt.csv",sep=''), stringsAsFactors = FALSE)
    
    if (index_year == "all") {
      noPath <- as.integer(summary_counts[summary_counts$COUNT_TYPE == "Number of persons in target cohort", "NUM_PERSONS"]) - sum(transformed_file$freq)
    } else {
      noPath <- as.integer(summary_counts[summary_counts$COUNT_TYPE == paste0("Number of persons in target cohort in ", index_year), "NUM_PERSONS"]) - sum(transformed_file$freq)
    }
    
    transformed_file <- rbind(transformed_file, c("End", noPath))
  }
  
  transformed_file$path <- as.factor(transformed_file$path)
  transformed_file$freq <- as.integer(transformed_file$freq)
  transformed_file <- transformed_file[order(-transformed_file$freq, transformed_file$path),]
  
  write.table(transformed_file, file=paste(path,"_inputsunburst_", index_year, ".csv",sep=''), sep = ",", row.names = FALSE)
}

transformCSVtoJSON <- function(outcomeCohortIds, outputFolder, path, index_year, maxPathLength) {
  data <- read.csv(paste(path,"_inputsunburst_", index_year, ".csv",sep=''))
  # data <- read.csv("output/IPCI/asthma1/test.csv")
  
  cohorts <- read.csv(paste(outputFolder, "/cohort.csv",sep=''), stringsAsFactors = FALSE)
  outcomes <- c(cohorts$cohortName[cohorts$cohortId %in% unlist(strsplit(outcomeCohortIds, ","))], "Other combinations")
  
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
    stringr::str_replace_all(p, structure(as.character(linking$bitwiseNumbers), names = as.character(linking$outcomes)))
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
  networkD3::saveNetwork(plot, file="sankeydiagram_all.html", selfcontained=TRUE)
  # there seems to be an error in this package -> cannot change path to output folder
  
}
