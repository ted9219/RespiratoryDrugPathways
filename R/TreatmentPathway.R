
treatmentPathway<-function(connectionDetails,
                        cohortDatabaseSchema,
                        cohortTable,
                        outputFolder = NULL,
                        outputFileTitle = NULL,
                        cohortIds,
                        targetCohortId,
                        outcomeCohortIds,
                        minimumRegimenChange = 1,
                        treatmentLine = 3,
                        collapseDates = 0,
                        minSubject = 0,
                        identicalSeriesCriteria = 30
){
  
  # Outcome cohorts (= different treatments)
  cohortForGraph<-cohortCycle(connectionDetails,
                              cohortDatabaseSchema,
                              cohortTable,
                              cohortIds,
                              targetCohortId,
                              outcomeCohortIds,
                              identicalSeriesCriteria)
  cohortForGraph <- cohortForGraph %>% subset(cycle == 1)
  cohortData <- cohortForGraph %>% select(-cycle)
  cohortData$cohortStartDate<-as.Date(cohortData$cohortStartDate)
  cohortData$cohortEndDate<-as.Date(cohortData$cohortEndDate)
  
  # Target cohort (= population)
  targetCohort<-cohortRecords(connectionDetails,
                             cohortDatabaseSchema,
                             cohortTable,
                             cohortIds,
                             selectCohortIds = targetCohortId)
  
  targetCohort<-targetCohort %>% subset(subjectId %in% cohortForGraph$subjectId)
  colnames(targetCohort) <- colnames(cohortData)
  targetCohort<-targetCohort %>% select(cohortDefinitionId,subjectId,cohortStartDate,cohortEndDate,cohortName)
  targetCohort$cohortStartDate<-as.Date(targetCohort$cohortStartDate)
  targetCohort$cohortEndDate<-as.Date(targetCohort$cohortEndDate)

  # Ignore the change to same regimen
  cohortData <- cohortData %>% arrange(subjectId,cohortStartDate) %>% group_by(subjectId)%>% mutate(lagCDI = lag(cohortName)) %>% subset(is.na(lagCDI)|lagCDI != cohortName) %>% select(-lagCDI)
  cohortData <- as.data.frame(cohortData)
  
  # Bind outcome and target cohort, ignore duplicated outcome records
  outcomeAndTarget<-rbind(cohortData,targetCohort) %>% arrange(subjectId,cohortStartDate) %>% group_by(subjectId)%>% mutate(lagCDI = lag(cohortName)) %>% subset(is.na(lagCDI)|lagCDI != cohortName) %>% select(-lagCDI) %>% ungroup()
  outcomeAndTarget$cohortName <- as.character(outcomeAndTarget$cohortName)
  outcomeAndTarget <- as.data.frame(outcomeAndTarget)

  # If regimens apart from each other less than collapseDates, collapse using '/'##
  # TODO: what is difference collapseDates with identicalSeriesCriteria?
  collapsedRecords<-data.table::rbindlist(lapply(unique(outcomeAndTarget$subjectId),function(targetSubjectId){
    reconstructedRecords <- data.frame()
    targetOutcomeAndTarget<-outcomeAndTarget %>% subset(subjectId == targetSubjectId)
    reconstructedRecords<-rbind(reconstructedRecords,targetOutcomeAndTarget[1,])

    if(nrow(targetOutcomeAndTarget)>=2){
      for(x in 2:nrow(targetOutcomeAndTarget)){
        if(as.integer(targetOutcomeAndTarget[x,3]-reconstructedRecords[nrow(reconstructedRecords),3])>collapseDates){
          reconstructedRecords <-rbind(reconstructedRecords,targetOutcomeAndTarget[x,])}else{sortNames<-sort(c(targetOutcomeAndTarget[x,5],reconstructedRecords[nrow(reconstructedRecords),5]))
          reconstructedRecords[nrow(reconstructedRecords),5]<-paste0(sortNames,collapse = '/')
          }}}
    return(reconstructedRecords)}))
  
  # Set minimum regimen change count
  outcomeAndTarget<-collapsedRecords
  minimunIndexId<-unique(outcomeAndTarget %>% arrange(subjectId,cohortStartDate) %>% group_by(subjectId) %>% mutate(line = row_number()) %>% subset(line >= minimumRegimenChange+1) %>% select(subjectId) %>% ungroup())
  outcomeAndTarget<-outcomeAndTarget %>% subset(subjectId %in% minimunIndexId$subjectId) %>% arrange(subjectId,cohortStartDate)
  
  # Maximum treatment line in graph
  outcomeAndTarget <- outcomeAndTarget %>% group_by(subjectId) %>% arrange(subjectId,cohortStartDate) %>% mutate(rowNumber = row_number()) %>% subset(rowNumber <= treatmentLine) %>% select(subjectId,cohortName,rowNumber) %>% mutate(nameOfConcept = paste0(rowNumber,'_',cohortName)) %>% ungroup()
  
  # Label
  label <-unique(outcomeAndTarget %>% select(cohortName,nameOfConcept) %>% arrange(nameOfConcept))
  label <-label %>% mutate(num = seq(from = 0,length.out = nrow(label)))
  
  # Nodes
  treatmentRatio<-data.table::rbindlist(lapply(1:treatmentLine,function(x){outcomeAndTarget %>% subset(rowNumber==x) %>% group_by(nameOfConcept) %>% summarise(n=n()) %>% mutate(ratio=round(n/sum(n)*100,1))}))
  treatmentRatio<-treatmentRatio %>% subset(n>=minSubject)
  label<-dplyr::left_join(treatmentRatio,label,by=c("nameOfConcept"="nameOfConcept")) %>% mutate(name = paste0(cohortName,' (n=',n,', ',ratio,'%)'))
  label<-label %>% mutate(num = seq(from = 0, length.out = nrow(label)))
  nodes<- label %>% select(name)
  nodes<-data.frame(nodes)
  
  # Pivot table
  pivotRecords<-reshape2::dcast(outcomeAndTarget,subjectId ~ rowNumber, value.var="nameOfConcept")
  
  # Link
  link<-data.table::rbindlist(lapply(2:max(outcomeAndTarget$rowNumber),function(x){
    source <- pivotRecords[,x]
    target <- pivotRecords[,x+1]
    link <-data.frame(source,target)
    link$source<-as.character(link$source)
    link$target<-as.character(link$target)
    link<-na.omit(link)
    return(link)}))
  link$source<-as.character(link$source)
  link$target<-as.character(link$target)
  link<-link %>% select(source,target)%>% group_by(source,target)%>% summarise(n=n()) %>% ungroup()
  source<-dplyr::left_join(link,label,by = c("source" = "nameOfConcept")) %>% select(num)
  target<-dplyr::left_join(link,label,by = c("target" = "nameOfConcept")) %>% select(num)
  freq<-link %>% select(n)
  links<-data.frame(source,target,freq)
  links<-na.omit(links)
  colnames(links) <-c('source','target','value')
  links$source<-as.integer(links$source)
  links$target<-as.integer(links$target)
  links$value<-as.numeric(links$value)
  
  # Sankey data
  treatment <-list(nodes=nodes,links=links)
  if(!is.null(outputFolder)){
    fileNameNodes <- paste0(outputFileTitle,'_','SankeyNodes.csv')
    write.csv(nodes, file.path(outputFolder, fileNameNodes),row.names = F)
    fileNameLinks <- paste0(outputFileTitle,'_','SankeyLinks.csv')
    write.csv(links, file.path(outputFolder, fileNameLinks),row.names = F)}
  treatmentPathway <- networkD3::sankeyNetwork(Links = treatment$links, Nodes = treatment$nodes, Source = "source",Target = "target", Value = "value", NodeID = "name", fontSize = 12, nodeWidth = 30,sinksRight = FALSE)
  return(treatmentPathway)
}
