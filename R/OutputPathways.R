
generateOutput <- function(studyName,outputFolder, maxPathLength, minCellCount, addNoPaths, otherCombinations) {
  inputFile <- paste(outputFolder, "/",studyName, "/", studyName,"_drug_seq_summary.csv",sep='')
  file <- as.data.table(read.csv(inputFile, stringsAsFactors = FALSE))
  
  # Group all 'other' combinations in one group if TRUE
  if (otherCombinations) {
    findCombinations <- apply(file, 2, function(x) grepl("+", x, fixed = TRUE))
    file[findCombinations] <- "Other combinations"
  }
  
  outputStartSABA(data = file, outputFile = paste(outputFolder, "/",studyName, "/", studyName,"_percent_start_saba.csv",sep=''))
  
  # Apply maxPathLength and group
  group <- as.vector(colnames(file)[!grepl("X|INDEX_YEAR|NUM_PERSONS|CONCEPT_ID", colnames(file))])
  group <- group[1:maxPathLength]
  file_noyear <- file[,.(freq=sum(NUM_PERSONS)), by=group]
  
  outputCombinationTreated(data = file_noyear, group = group, outputFile = paste(outputFolder, "/",studyName, "/", studyName,"_percent_combination_treated.csv",sep=''))
  
  # todo: not remove minCellCount but aggregate pathway to other path
  writeLines(paste("Remove ", sum(file_noyear$freq < minCellCount), " paths with too low frequency"))
  file_noyear <- file_noyear[freq >= minCellCount,]
  
  sankeyDiagram(data = file_noyear)
  
  inputSunburstPlot(data = file_noyear, group = group, studyName = studyName, outputFolder = outputFolder, addNoPaths = addNoPaths)
  
  writeLines("Created output files")
}

sankeyDiagram <- function(data) {
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
  networkD3::sankeyNetwork(Links = links, Nodes = nodes, 
                           Source = 'source', 
                           Target = 'target', 
                           Value = 'value', 
                           NodeID = 'name',
                           units = 'votes')
  # To do: save network
}

outputStartSABA <- function(data, outputFile) {
  data[D1_CONCEPT_NAME == "SABA", IS_SABA:=1]
  percentStartSABA <- data[, sum(NUM_PERSONS), by = list(IS_SABA, INDEX_YEAR)]
  write.csv(percentStartSABA, file=outputFile)
}

outputCombinationTreated <- function(data, group, outputFile) {
  percentCombinationTreated <- sapply(group, function(g) {
    indexCombinations <- grepl("Other combinations|\\+|\\&", transpose(data[,..g]))
    
    sumCombinations <- sum(data$freq[indexCombinations])
    sumNotNA <- sum(data$freq[!is.na(data[,..g])])
    
    result <- sumCombinations * 100.0 / sumNotNA
    
  })
  write.csv(percentCombinationTreated, file=outputFile)
  
}

inputSunburstPlot <- function(data, group, studyName, outputFolder, outputFile, addNoPaths) {
  transformed_file <- apply(data[,..group],1, paste, collapse = "-")
  transformed_file <- str_replace_all(transformed_file, "-NA", "")
  transformed_file <- paste0(transformed_file, "-End")
  transformed_file <- data.frame(path=transformed_file, freq=data$freq, stringsAsFactors = FALSE)
  
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
  
  write.table(transformed_file[order(-transformed_file$freq, transformed_file$path),],file=paste(outputFolder, "/",studyName, "/", studyName,"_transformed_drug_seq_summary.csv",sep=''), sep = ",", row.names = FALSE, col.names = FALSE)
}

createSunburstPlot <- function(studyName, outputFolder){
  inputFile=paste(outputFolder, "/",studyName, "/", studyName,"_transformed_drug_seq_summary.csv",sep='')

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
}



