usagePatternGraph<-function(connectionDetails,
                            cohortDatabaseSchema,
                            cohortTable,
                            outputFolder = NULL,
                            outputFileTitle = NULL,
                            cohortIds,
                            targetCohortId,
                            outcomeCohortIds,
                            identicalSeriesCriteria = 30,
                            fromYear,
                            toYear){
  
  # Outcome cohorts (= different treatments)
  cohortForGraph<-cohortCycle(connectionDetails,
                              cohortDatabaseSchema,
                              cohortTable,
                              cohortIds,
                              targetCohortId,
                              outcomeCohortIds,
                              identicalSeriesCriteria)
  cohortForGraph <- cohortForGraph %>% subset(cycle == 1)
  cohortData <- cohortForGraph %>% select(-cohortName,-cycle)
  cohortData$cohortStartDate<-as.Date(cohortData$cohortStartDate)
  cohortData$cohortEndDate<-as.Date(cohortData$cohortEndDate)
  
  cohortForGraph<-cohortForGraph %>% select(subjectId,cohortName,cohortStartDate)
  cohortForGraph$cohortStartDate<-format(as.Date(cohortForGraph$cohortStartDate, format="Y-%m-%d"),"%Y")
  
  cohortForGraph<-cohortForGraph %>% group_by(cohortStartDate,cohortName)
  cohortForGraph<-unique(cohortForGraph)
  cohortForGraph<-cohortForGraph %>% summarise(n=n()) %>%ungroup() %>%  arrange(cohortName,cohortStartDate) %>% subset(cohortStartDate <=toYear & cohortStartDate >=fromYear) %>% group_by(cohortStartDate) %>% mutate(total = sum(n)) %>% mutate(proportion = round(n/total*100,1)) %>% select(cohortStartDate,cohortName,proportion)
  colnames(cohortForGraph) <- c('Year','Cohort','proportion')
  cohortForGraph$Year<-as.integer(cohortForGraph$Year)
  Year<-rep(c(fromYear:toYear),length(unique(cohortForGraph$Cohort)))
  Cohort<-sort(rep(unique(cohortForGraph$Cohort),length(c(fromYear:toYear))))
  index<-data.frame(Year,Cohort)
  index$Year <- as.integer(index$Year)
  index$Cohort<-as.character(index$Cohort)
  plotData<-left_join(index,cohortForGraph)
  plotData[is.na(plotData)]<-0
  h<-plotData %>% highcharter::hchart(.,type="line",hcaes(x = Year,y=proportion,group = Cohort)) %>% hc_xAxis(title = list(text = "Year")) %>% hc_yAxis(title = list(text = "Proportion of the regimen treated patients for total chemotherapy received patients (%)"),from = 0, to =70)
  if(!is.null(outputFolder)){
    fileName <- paste0(outputFileTitle,'_','usagePatternRegimenProportion.csv')
    write.csv(plotData, file.path(outputFolder, fileName),row.names = F)}
  return(h)}
