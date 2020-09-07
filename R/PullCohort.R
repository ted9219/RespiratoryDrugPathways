cohortCycle<- function(connectionDetails,
                       cohortDatabaseSchema,
                       cohortTable,
                       cohortIds,
                       targetCohortId,
                       outcomeCohortIds,
                       identicalSeriesCriteria){
  
  # Treatment cohort
  cycleCohort<-cohortRecords(connectionDetails,
                             cohortDatabaseSchema,
                             cohortTable,
                             cohortIds,
                             selectCohortIds = outcomeCohortIds)
  cycleCohort$cohortStartDate<-as.Date(cycleCohort$cohortStartDate)
  cycleCohort$cohortEndDate<-as.Date(cycleCohort$cohortEndDate)

  # Target cohort
  if(!is.null(targetCohortId)){
    conditionCohort<-cohortRecords(connectionDetails,
                                   cohortDatabaseSchema,
                                   cohortTable,
                                   cohortIds,
                                   selectCohortIds = targetCohortId)
    cycleCohort<-cycleCohort %>% subset(subjectId %in% conditionCohort$subjectId)}
  
  # TODO: check dateDiff
  cohortWtDiff <- cycleCohort %>% group_by(subjectId,cohortDefinitionId) %>% arrange(subjectId,cohortStartDate) %>% mutate(dateDiff = (cohortStartDate-lag(cohortStartDate)))
  cohortWtDiff$dateDiff<-as.numeric(cohortWtDiff$dateDiff)
  cohortWtDiff$flagSeq <- NA
  cohortWtDiff$flagSeq[is.na(cohortWtDiff$dateDiff)|cohortWtDiff$dateDiff>=identicalSeriesCriteria] <- 1
  
  standardCycle<-data.table::as.data.table(cohortWtDiff)
  standardCycle[, cycle := seq_len(.N), by=.(cumsum(!is.na(flagSeq)))]
  
  standardCycle<-standardCycle %>% select(cohortDefinitionId,subjectId,cohortStartDate,cohortEndDate,cohortName,cycle)
  standardCycle<-data.frame(standardCycle)
  return(standardCycle)}

cohortRecords <- function(connectionDetails,
                          cohortDatabaseSchema,
                          cohortTable,
                          cohortIds,
                          selectCohortIds){
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- 'SELECT * FROM @result_database_schema.@cohort_table WHERE cohort_definition_id IN (@target_cohort_ids)'
  sql <- SqlRender::render(sql,
                           result_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable,
                           target_cohort_ids= selectCohortIds)
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  Cohort <- DatabaseConnector::querySql(connection, sql)
  colnames(Cohort) <- SqlRender::snakeCaseToCamelCase(colnames(Cohort))
  Cohort<-dplyr::left_join(Cohort,cohortIds, by= c("cohortDefinitionId"="cohortId"))
  DatabaseConnector::disconnect(connection)
  return(Cohort)}
