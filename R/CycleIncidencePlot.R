
cycleIncidencePlot<-function(connectionDetails,
                        cohortDatabaseSchema,
                        cohortTable,
                        outputFolder = NULL,
                        outputFileTitle = NULL,
                        cohortIds,
                        targetCohortId,
                        outcomeCohortIds,
                      restrictInitialSeries = TRUE,
                        restricInitialEvent =TRUE,
                        identicalSeriesCriteria = 30,
                        eventPeriod = 30,
                        minSubject = 0){
  
  # Outcome cohorts (= different treatments)
  cohortData<-cohortCycle(connectionDetails,
                              cohortDatabaseSchema,
                              cohortTable,
                              cohortIds,
                              targetCohortId,
                              outcomeCohortIds,
                              identicalSeriesCriteria)
  
  # Restrict Initial Series
  if(restrictInitialSeries){
  cohortFirstIndex<-cohortData %>% subset(cycle == 1) %>% arrange(subjectId,cohortStartDate) %>% group_by(subjectId) %>% mutate(index= row_number())
  indexedCohort<-left_join(cohortData,cohortFirstIndex)
  indexedCohort$index<-data.table::nafill(indexedCohort$index, type = "locf")
  cohortData<-indexedCohort %>% subset(index == 1) %>% select(-index)
  }

  targetCohort<-cohortRecords(connectionDetails,
                              cohortDatabaseSchema,
                              cohortTable,
                              cohortIds,
                              selectCohortIds = targetCohortId)
  colnames(targetCohort) <- colnames(cohortData)
  targetCohort<-targetCohort %>% select(cohortDefinitionId,subjectId,cohortStartDate,cohortEndDate,cohortName,cycle)
  
  # targetCohort <- unique(targetCohort %>% mutate (cycle = 0) %>% select(-type) %>% subset(subjectId %in% cohortData$subjectId)) %>% select(-conceptId)
  targetCohort<-targetCohort %>% subset(subjectId %in% cohortData$subjectId)
  
  # Cohort name cycle
  collapsedCohort<-rbind(cohortData,targetCohort) %>% arrange(subjectId,cohortStartDate) %>% mutate(cohort_cycle = paste0(cycle,'_',
                                                                                                                           cohortName
  ))
  # Prev record
  collapsedCohort<-collapsedCohort %>% arrange(subjectId,cohortStartDate,desc(cohort_cycle))%>% group_by(subjectId) %>% mutate(prev_c_n_c = lag(cohort_cycle)) %>% mutate(prevDate = lag(cohortStartDate)) %>% ungroup()

  # Event after target
  eventAfterTarget<-unique(na.omit(collapsedCohort %>% subset(cohortName %in% unique(targetCohort$cohortName)) %>% subset(cohort_cycle != prev_c_n_c)) %>% subset(cohortStartDate-prevDate<= eventPeriod))

  if(restricInitialEvent){eventAfterTarget<-eventAfterTarget %>% arrange(subjectId,cohortStartDate)%>% group_by(subjectId) %>% slice(1)}

  summariseEvent <- unique(eventAfterTarget %>% group_by(prev_c_n_c))%>% summarise(n=n())

  summariseTarget <- unique(cohortData %>% mutate(cohort_cycle = paste0(cycle,'_',cohortName)) %>% group_by(cohort_cycle)) %>% summarise(n=n())

  # Collapse summarised data
  collapsedSummarise<-left_join(summariseTarget,summariseEvent, by=c('cohort_cycle'="prev_c_n_c"))
  colnames(collapsedSummarise) <-c('cohort_cycle','total','event')
  collapsedSummarise<-as.data.frame(collapsedSummarise)
  collapsedSummarise[is.na(collapsedSummarise)] <-0

  # Over the minimum patient number
  collapsedSummarise <- collapsedSummarise %>% subset(total >= minSubject)

  seperateNameIndex<-unique(cohortData %>% mutate(cohort_cycle = paste0(cycle,'_',cohortName)) %>% select(cohortName,cycle,cohort_cycle))

  # Plot data
  plotData<-left_join(collapsedSummarise,seperateNameIndex) %>% mutate(ratio = event/total)  %>% select(cycle,cohortName,event,total,ratio,cohort_cycle) %>% arrange(cohortName,cycle)
  if(!is.null(outputFolder)){
    fileName <- paste0(outputFileTitle,'_','EventIncidenceInCycle.csv')
    write.csv(plotData, file.path(outputFolder, fileName),row.names = F)}
  # plot #1 - Incidence Rate

  p1 <- ggplot(na.omit(plotData), aes(x = cohort_cycle, y = ratio, group = cohortName, color = cohortName)) +
    theme_bw() +
    scale_x_discrete(limits = na.omit(plotData)$cohort_cycle) +
    geom_point(size = 2, aes(fill = cohortName)) +
    geom_text(aes(label = percent(round(ratio, 2))),
              size = 4, hjust = -0.01, vjust = -0.2, fontface = "plain") +
    geom_smooth(size = 1.0, method = 'lm', aes(fill = cohortName)) +
    #scale_fill_viridis(discrete=TRUE) +
    #scale_color_viridis(discrete=TRUE) +
    theme(legend.position='none',
          plot.title = element_text(size=20, face="bold", vjust=2),
          axis.title.x = element_blank(),
          axis.title.y = element_text(size=13),
          axis.text = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank(),
          panel.border = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank()) +
    labs(y = 'Incidence Rate') +
    ggtitle("Event Incidence Rate - Cycle plot")


  # plot #2 - number of Incidence
  p2 <- ggplot(na.omit(plotData), aes(x = cohort_cycle, group = cohortName, color = cohortName)) +
    theme_bw() +
    scale_x_discrete(limits = na.omit(plotData)$cohort_cycle) +
    geom_bar(aes(y = total, fill = cohortName), alpha = 0.2, stat = 'identity',show.legend = TRUE) +
    geom_bar(aes(y = event, fill = cohortName), alpha = 0.6, stat = 'identity',show.legend = FALSE) +
    geom_text(aes(y = total, label = paste(event,'/',total)),
              angle = 90, size = 4, hjust = -0.1, vjust = 0.4,check_overlap = TRUE,show.legend = FALSE, position = position_dodge(width = 0.6)) +
    geom_text(aes(y = 0, label = cycle), angle = 90, size = 4, hjust = 1.25, vjust = 0.4,check_overlap = TRUE,show.legend = FALSE) +
   # scale_fill_viridis(discrete=TRUE) +
    # scale_color_viridis(discrete=TRUE) +
    theme(legend.position='bottom',
          legend.title=element_blank(),
          plot.title = element_text(size=20, vjust=2),
          axis.title.x = element_text(size=13),
          axis.title.y = element_text(size=13),
          axis.text = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank(),
          panel.border = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank()) +
    scale_y_continuous(limits = c(0, max(plotData$total * 1.1))) +
    labs(x = 'Iteration Number of treatment', y = 'Number of Incidence')

  # multiplot
  p<-grid.arrange(p1, p2, ncol = 1)
return(p)}
