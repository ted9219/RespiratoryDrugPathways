## A few Helper functions
prettyHr <- function(x) {
  result <- sprintf("%.2f", x)
  result[is.na(x)] <- "NA"
  result <- suppressWarnings(format(as.numeric(result), big.mark=",")) # adds thousands separator
  return(result)
}

addThousandsSeparator<-function(table){
  if(is.data.frame(table)){
    is.num <- sapply(table, is.numeric)
    table[is.num] <- lapply(table[is.num], function(x) format(as.numeric(x), big.mark=","))
    return(table)
  } else {
    is.not.na<- !sapply(suppressWarnings(as.numeric(table)), is.na)
    table[is.not.na] <- format(as.numeric(table[is.not.na]), big.mark=",")
    return(table)
  }
  
}

getHoveroverStyle <- function(left_px, top_px) {
  style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
                  "left:",
                  left_px - 200,
                  "px; top:",
                  top_px - 130,
                  "px; width:400px;")
}

addResourcePath("workingdirectory", stringr::str_replace(getwd(),"/shiny",""))

## the shiny server update function
shinyServer(function(input, output, session) {
  cohortId <- reactive({
    return(cohort$cohortId[cohort$cohortName == input$ingredient])
  })
  
  cdata <- session$clientData
  
  # Stats
  output$population <- renderValueBox({
    valueBox(
      1000000,
      "Study Population",
      icon = icon("users")
    )
  })
  
  output$users <- renderValueBox({
    valueBox(
      100000,
      "Number of drug users",
      icon = icon("users")
    )
  })
  
  output$count <- renderValueBox({
    valueBox(
      value = 6,
      subtitle = "Number of databases",
      icon = icon("download")
    )
  })
  
  observeEvent(input$analysis,{
    ## hide tables tab
    if(input$analysis!='Observation Period' ){
      showTab(inputId = "resultTabsetPanel", target = "Tables")
    }
    else if(input$analysis=='Observation Period'){
      hideTab(inputId = "resultTabsetPanel", target = "Tables")
    }
    
    ## hide figures tab
    if(input$analysis!='Renal Impairment' && input$analysis!='Indications'){
      showTab(inputId = "resultTabsetPanel", target = "Figures")
    }
    else if(input$analysis=='Renal Impairment' || input$analysis=='Indications'){
      hideTab(inputId = "resultTabsetPanel", target = "Figures")
    }
  })
  
  tableASelected <- reactive({
    if (input$analysis == 'Drug Exposure (days)'){
      return(table1a)
    }
    
    if (input$analysis == 'PDD/DDD Ratio'){
      return(table2a)
    }
    
    if (input$analysis == 'Cumulative DDD'){
      return(table3a)
    }
    
    if (input$analysis == 'Cumulative Dose (mg)'){
      return(table4a)
    }
    
    if (input$analysis == 'Cumulative annual dose (mg/PY)'){
      return(table5a)
    }
    
    if (input$analysis == 'Renal Impairment'){
      return(table7)
    }
    return(NULL)
  })
  
  tableBSelected <- reactive({
    if (input$analysis == 'Drug Exposure (days)'){
      return(table1b)
    }
    
    if (input$analysis == 'PDD/DDD Ratio'){
      return(table2b)
    }
    
    if (input$analysis == 'Cumulative DDD'){
      return(table3b)
    }
    
    if (input$analysis == 'Cumulative Dose (mg)'){
      return(table4b)
    }
    
    if (input$analysis == 'Cumulative annual dose (mg/PY)'){
      return(table5b)
    }
    
    return(NULL)
  })
  colnamesTableA <- reactive({
    tableA1Columns <- c("Variable",
                        "Value",
                        "N users",
                        #  "Excluded",
                        "Mean",
                        "Median",
                        "P5",
                        "Q1",
                        "Q3",
                        "P95",
                        "Min",
                        "Max",
                        "0-1 Month",
                        "1-12 Months",
                        "1-10 Year",
                        ">10 Years")
    
    tableA2Columns <- c("Variable",
                        "Value",
                        "N users",
                        #  "Excluded",
                        "Mean",
                        "Median",
                        "P5",
                        "Q1",
                        "Q3",
                        "P95",
                        "Min",
                        "Max")
    
    tableA3Columns <- c("Variable",
                        "Value",
                        "N users",
                        #  "Excluded",
                        "Mean",
                        "Median",
                        "P5",
                        "Q1",
                        "Q3",
                        "P95",
                        "Min",
                        "Max",
                        "<1",
                        "=1",
                        ">1")
    
    
    table7AColumns <- c("Ingredient",
                        "N",
                        "Users",
                        "Percentage")
    
    if (input$analysis == 'Renal Impairment'){
      return(table7AColumns)
    } 
    if (input$analysis == 'Drug Exposure (days)'){
      return(tableA1Columns)
    } 
    if (input$analysis == 'PDD/DDD Ratio'){
      return(tableA3Columns)
    } 
    else {
      return(tableA2Columns)
    }
  })
  
  output$TableA <- renderDataTable({
    if (!is.null(tableASelected()) & input$analysis != 'Renal Impairment') {
      table <- tableASelected() %>% filter(ingredient == input$ingredient) %>%
        filter(databaseid == input$database) %>%
        select(-databaseid) %>% select(-excluded) 
      table <- clearSecondOccurrenceVariable(table, "variable")
      drops <- c("ingredient", "order")
      table <- table[,!(names(table) %in% drops)]
      
      colnames(table) <- colnamesTableA()
      table$Mean <- prettyHr(table$Mean)
      table$Median <- prettyHr(table$Median)
      table$P5 <- prettyHr(table$P5)
      table$Q1 <- prettyHr(table$Q1)
      table$Q3 <- prettyHr(table$Q3)
      table$P95 <- prettyHr(table$P95)
      table$Min <- prettyHr(table$Min)
      table$Max <- prettyHr(table$Max)
      
      table<-addThousandsSeparator(table)
      table[,"N users"]<-addThousandsSeparator(unname(unlist(table[,"N users"])))
      
      selection = list(mode = "single", target = "row")
      
      table <- datatable(
        table,
        extensions = c('Buttons','FixedColumns'),
        options = list(
          aoColumnDefs = list(list(className= 'dt-left', targets = "_all")),
          pageLength = 50,
          ordering = FALSE,
          dom = 'tB',
          scrollX = TRUE,
          fixedColumns = TRUE,
          buttons =
            list(
              'copy',
              'print',
              list(
                extend = 'collection',
                buttons = c('csv', 'excel', 'pdf'),
                text = 'Download'
              )
            )
        ),
        #options = options,
        selection = selection,
        rownames = FALSE,
        escape = FALSE,
        class = "stripe nowrap compact"
      )
    } else
      table <- NULL
    return(table)
  })
  
  output$TableB <- renderDataTable({
    if (!is.null(tableBSelected())) {
      table <- tableBSelected()  %>% select(-excluded) %>%
        filter(ingredient == input$ingredient)  %>%
        filter(indication == input$indication)  %>%
        filter(formulation == input$formulation) %>%
        filter(databaseid == input$database) %>%
        mutate(cumulativeDurationGroup=case_when(
          cumulativeDurationGroup=="1>10 Years" ~ ">10 Years",
          TRUE ~ cumulativeDurationGroup
        ))
      
      drops <- c("databaseid","ingredient","indication","formulation")
      table <- table[ , !(names(table) %in% drops)]
      
      colnames(table) <- tableBColumns
      table <- clearSecondOccurrenceVariable(table,"ICH_group")
      table <- clearSecondOccurrenceVariable(table,"Age")
      
      table$Mean <- prettyHr(table$Mean)
      table$Median <- prettyHr(table$Median)
      table$P5 <- prettyHr(table$P5)
      table$Q1 <- prettyHr(table$Q1)
      table$Q3 <- prettyHr(table$Q3)
      table$P95 <- prettyHr(table$P95)
      table$Min <- prettyHr(table$Min)
      table$Max <- prettyHr(table$Max)
      
      table<-addThousandsSeparator(table)
      table[,"N users"]<-addThousandsSeparator(unname(unlist(table[,"N users"])))
      
      selection = list(mode = "single", target = "row")
      table <- datatable(
        table,
        extensions = c('Buttons','FixedColumns'), 
        options = list(
          aoColumnDefs = list(list(className= 'dt-left', targets = "_all")),
          pageLength = 100,
          ordering = FALSE,
          dom = 'tB',
          scrollX = TRUE,
          fixedColumns = TRUE,
          buttons = 
            list('copy', 'print', list(
              extend = 'collection',
              buttons = c('csv', 'excel', 'pdf'),
              text = 'Download'
            ))),
        selection = selection,
        rownames = FALSE,
        escape = FALSE,
        class = "stripe nowrap compact"
      )
    } else
      table <- NULL
    return(table)
  })
  
  output$Table6A <- renderDataTable({
    selected <- input$analysis
    if (!is.null(selected)) {
      table <- indication %>%
        filter(databaseid == input$database) %>%
        filter(ingredient == input$ingredient)  %>%
        filter(formulation == input$formulation) %>%
        select(-databaseid)
      table$P180Gerd <- prettyHr(table$P180Gerd)
      table$P365Gerd <- prettyHr(table$P365Gerd)
      table$P180Ulcer <- prettyHr(table$P180Ulcer)
      table$P365Ulcer <- prettyHr(table$P365Ulcer)
      table$P180Zes <- prettyHr(table$P180Zes)
      table$P365Zes <- prettyHr(table$P365Zes)
      table$PUnknown <- prettyHr(table$PUnknown)
      
      table <- clearSecondOccurrenceVariable(table,"ingredient")
      table <- clearSecondOccurrenceVariable(table,"formulation")
      
      table<-addThousandsSeparator(table) 
      
      colnames(table) <- table6AColumns
      
      selection = list(mode = "single", target = "row")
      table <- datatable(
        table,
        extensions = c('Buttons','FixedColumns'),
        options = list(
          pageLength = 50,
          ordering = FALSE,
          dom = 'tB',
          scrollX = TRUE,
          fixedColumns = TRUE,
          lengthChange = TRUE,
          columnDefs = list(list(className = 'dt-left', targets = "_all")),
          buttons =
            list(
              'copy',
              'print',
              list(
                extend = 'collection',
                buttons = c('csv', 'excel', 'pdf'),
                text = 'Download'
              )
            )
        ),
        selection = selection,
        rownames = FALSE,
        escape = FALSE,
        class = "stripe nowrap compact"
      )
      return(table)
    } else {
      table <- NULL
      return(table)
    }
  })
  
  output$Table7A <- renderDataTable({
    table <- tableASelected() 
    if ((is.null(table) || nrow(table) == 0) || input$analysis != 'Renal Impairment') {
      return(NULL)
    } else {
      table <- table %>%
        filter(databaseid == input$database) %>%
        select(-databaseid)
      colnames(table) <- colnamesTableA()
      table$Percentage <- prettyHr(table$Percentage)
      table<-addThousandsSeparator(table)
      selection = list(mode = "single", target = "row")
      table <- datatable(
        table,
        extensions = 'Buttons',
        options = list(
          aoColumnDefs = list(list(className= 'dt-left', targets = "_all")),
          pageLength = 100,
          ordering = FALSE,
          dom = 'tB',
          buttons =
            list(
              'copy',
              'print',
              list(
                extend = 'collection',
                buttons = c('csv', 'excel', 'pdf'),
                text = 'Download'
              )
            )
        ),
        selection = selection,
        rownames = FALSE,
        escape = FALSE,
        class = "stripe nowrap compact"
      )}
    return(table)
  })
  
  # Table titles
  output$tableATitle <- renderText({
    result <- NULL
    selected <-input$analysis
    if (selected == 'Drug Exposure (days)'){
      result<-paste0("Table 1A: Cumulative duration of drug exposure (in days) for ",input$ingredient," in ", input$database)     
    }
    if (selected == 'PDD/DDD Ratio'){
      result<-paste0("Table 2A: PDD/DDD Ratio for ",input$ingredient," in ", input$database)     
    }
    if (selected == 'Cumulative DDD'){
      result<-paste0("Table 3A: Cumulative number of DDDs for ",input$ingredient," in ", input$database)     
    }
    if (selected == 'Cumulative Dose (mg)'){
      result<-paste0("Table 4A: Cumulative Dose (mg) for ",input$ingredient," in ", input$database)     
    }
    if (selected == 'Cumulative annual dose (mg/PY)'){
      result<-paste0("Table 5A: Cumulative annual dose (mg/PY) for ",input$ingredient," in ", input$database)     
    }
    if (selected == 'Indications'){
      result<-paste0("Table 6: Indications for all ingredients for ",input$ingredient," as ",input$formulation," in ",input$database)     
    }
    if (selected == 'Renal Impairment'){
      result<-paste0("Table 7: History of renal impairment for all ingredients in ", input$database)     
    }
    if (is.null(result)){
      result <- "No Results available"
    }
    return(result)
  })
  
  output$tableBTitle <- renderText({
    result <- NULL
    selected <-input$analysis
    if (selected == 'Drug Exposure (days)'){
      result<-paste0("Table 1B: Cumulative duration of drug exposure (days) in drug exposure, age category and gender strata for ",input$ingredient, " with formulation ", input$formulation, ", indication ", input$indication, ", in ", input$database)
    }
    if (selected == 'PDD/DDD Ratio'){
      result<-paste0("Table 2B: PDD/DDD Ratio for  ",input$ingredient, " with formulation ", input$formulation, ", indication ", input$indication, ", in ", input$database)
    }
    if (selected == 'Cumulative DDD'){
      result<-paste0("Table 3B: Cumulative number of DDDs for  ",input$ingredient, " with formulation ", input$formulation, ", indication ", input$indication, ", in ", input$database)
    }
    if (selected == 'Cumulative Dose (mg)'){
      result<-paste0("Table 4B: Cumulative Dose (mg) for  ",input$ingredient, " with formulation ", input$formulation, ", indication ", input$indication, ", in ", input$database)
    }
    if (selected == 'Cumulative annual dose (mg/PY)'){
      result<-paste0("Table 5B: Cumulative annual dose (mg/PY) for  ",input$ingredient, " with formulation ", input$formulation, ", indication ", input$indication, ", in ", input$database)
    }
    if (selected == 'Indications'){
      result<-" "   
    }
    if (selected == 'Renal Impairment'){
      result<-" "   
    }
    if (is.null(result)){
      result <- " "
    }
    return(result)
  })
  
  # Plots
  output$incidenceProportionPlot <- renderPlot({
    data <- filteredIncidenceProportions()
    if (is.null(data)) {
      return(NULL)
    }
    plot <- plotProportion(data = data,
                           stratifyByAge = "Age" %in% input$ipStratification,
                           stratifyByGender = "Gender" %in% input$ipStratification,
                           stratifyByCalendarYear = "Calendar Year" %in% input$ipStratification,
                           yAxisLabel = "Incidence Per 1000 People",
                           scales = input$yAxisChoiceIp)
    return(plot)
  }, res = 100)
  
  output$prevalenceProportionPlot <- renderPlot({
    data <- filteredPrevalenceProportions()
    if (is.null(data)) {
      return(NULL)
    }
    plot <- plotProportion(data = data,
                           stratifyByAge = "Age" %in% input$ppStratification,
                           stratifyByGender = "Gender" %in% input$ppStratification,
                           stratifyByCalendarYear = "Calendar Year" %in% input$ppStratification,
                           yAxisLabel = "Prevalence per 1000 persons",
                           scales = input$yAxisChoicePp)
    return(plot)
  }, res = 100)
  
  output$hoverInfoIp <- renderUI({
    data <- filteredIncidenceProportions()
    if (is.null(data)) {
      return(NULL)
    }else {
      hover <- input$plotHoverIp
      point <- nearPoints(data, hover, threshold = 5, maxpoints = 1, addDist = TRUE)
      if (nrow(point) == 0) {
        return(NULL)
      }
      left_px <- hover$coords_css$x
      top_px <- hover$coords_css$y
      
      tooltip <- getProportionTooltip("Incidence", top_px, point)
      style <- getHoveroverStyle(left_px = left_px, top_px = tooltip$top_px)
      div(
        style = "position: relative; width: 0; height: 0",
        wellPanel(
          style = style,
          p(HTML(tooltip$text))
        )
      )
    }
  }) 
  
  output$hoverInfoPp <- renderUI({
    data <- filteredPrevalenceProportions()
    if (is.null(data)) {
      return(NULL)
    }else {
      hover <- input$plotHoverPp
      point <- nearPoints(data, hover, threshold = 5, maxpoints = 1, addDist = TRUE)
      if (nrow(point) == 0) {
        return(NULL)
      }
      left_px <- hover$coords_css$x
      top_px <- hover$coords_css$y
      
      tooltip <- getProportionTooltip("Prevalence", top_px, point)
      style <- getHoveroverStyle(left_px = left_px, top_px = tooltip$top_px)
      div(
        style = "position: relative; width: 0; height: 0",
        wellPanel(
          style = style,
          p(HTML(tooltip$text))
        )
      )
    }
  }) 
  
  output$BoxplotBxp <- renderPlot({
    if (!is.null(tableBSelected())) {
      table <- tableBSelected()
      plotdata <- table %>%
        filter(databaseid %in% input$databases) %>%
        filter(ingredient == input$ingredient) %>%
        filter(indication == input$indication) %>%
        filter(formulation == input$formulation) %>%
        filter(gender == "Total") %>%
        filter(!is.na(suppressWarnings(as.numeric(n))))%>% # removes the <5 rows
        mutate(cumulativeDurationGroup=case_when(
          cumulativeDurationGroup=="1>10 Years" ~ ">10 Years",
          TRUE ~ cumulativeDurationGroup
        ))  
      if (is.null(plotdata) || nrow(plotdata) == 0) {
        plot <- NULL
      }
      else{
        normalizeWidth <- function(x){((x-min(x))/(max(x)-min(x))+0.2)/1.2}
        plot <-
          ggplot(plotdata,
                 aes(
                   x = factor(cumulativeDurationGroup, 
                              levels = c("0-1 Month","1-12 Months","1-10 Year",">10 Years","Overal exposure")
                   ),
                   ymin = p5,
                   lower = q1,
                   middle = median,
                   upper = q3,
                   ymax = p95
                 )) +
          geom_boxplot(stat = 'identity',width = normalizeWidth(log10(as.numeric(plotdata$n))),fill = "#56B4E9") +
          geom_text(aes(label=paste0("n=",n),y=Inf),position=position_dodge(0.9),vjust = 1) +
          facet_grid(sort(databaseid)~factor(additionalAgegroup, as.character(sort(unique(additionalAgegroup)))), scales = "free_y") +
          labs(x = "Exposure duration strata",
               y = input$analysis,
               subtitle = "Age strata (year)") +
          theme_light() +
          theme(plot.subtitle = element_text(hjust = 0.5))+
          theme(text = element_text(size=15)) +
          theme(strip.text=element_text(size=15))+
          theme(legend.position = "none")#+
        #scale_y_continuous(trans='log10')
      }
    }
    else {
      plot <- NULL
    }
    return(plot)
  }, res = 100)
  
  output$observationPeriodHistogram <- renderGirafe({
    if (!is.null(observationperiodhistogramfulldatabase)) {
      plotdata <- observationperiodhistogramfulldatabase %>%
        filter(databaseid %in% input$databases)
      if (is.null(plotdata) || nrow(plotdata) == 0) {
        return(NULL)
      } else{
        date<-as.Date(paste(plotdata$obsYearMonth,"01",sep=""),"%Y%m%d")
        p <- plotdata %>%
          ggplot(aes(x=date, y=numPersons/1000, fill=databaseid)) +  
          scale_x_date(date_breaks = "1 year", 
                       labels = date_format("%Y"))+
          geom_bar_interactive(tooltip =paste0("n:\t\t",plotdata$numPersons,"\n","date:\t",date),stat = "identity")+
          xlab("Year")+
          ylab("Number of persons (x1000)")+
          facet_grid(sort(plotdata$databaseid)~., scales="free_y")+
          theme(axis.text.x = element_text(angle = 90, vjust = .5),legend.position = "none",text = element_text(size=18))
        return(girafe(code = print(p),pointsize = 18,
                      width_svg = (1*input$pltChange$width/input$pltChange$dpi),
                      height_svg = (1.2*input$pltChange$height/input$pltChange$dpi)
        ))
      }
    } else {
      return(NULL)
    }
  })
  
  # Plot titles
  output$FigureTitle <- renderText({
    result <- NULL
    selected <-input$analysis
    if (selected == 'Drug Exposure (days)'){
      result<-paste0("Figure 3: Cumulative duration of drug exposure (days) in drug exposure, age category and gender strata for ",input$ingredient, " with formulation ", input$formulation, " and indication ", input$indication)
    }
    if (selected == 'PDD/DDD Ratio'){
      result<-paste0("Figure 4: PDD/DDD Ratio for  ",input$ingredient, " with formulation ", input$formulation, " and indication ", input$indication)
    }
    if (selected == 'Cumulative DDD'){
      result<-paste0("Figure 5: Cumulative number of DDDs for  ",input$ingredient, " with formulation ", input$formulation, " and indication ", input$indication)
    }
    if (selected == 'Cumulative Dose (mg)'){
      result<-paste0("Figure 6: Cumulative Dose (mg) for  ",input$ingredient, " with formulation ", input$formulation, " and indication ", input$indication)
    }
    if (selected == 'Cumulative annual dose (mg/PY)'){
      result<-paste0("Figure 6b: Cumulative annual dose (mg/PY) for  ",input$ingredient, " with formulation ", input$formulation, " and indication ", input$indication)
    }
    if (selected == 'Observation Period'){
      result<-"Figure 7: Observation Period per database"   
    }
    if (selected == 'Renal Impairment'){
      result<-" "   
    }
    if (is.null(result)){
      result <- " "
    }
    return(result)
  })
  
  # Functionality for help messages
  showInfoBox <- function(title, htmlFileName) {
    showModal(modalDialog(
      title = title,
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(readChar(htmlFileName, file.info(htmlFileName)$size) )
    ))
  }
  
  observeEvent(input$aboutInfo, {
    showInfoBox("About", "html/about.html")
  })
  observeEvent(input$databaseInfo, {
    showInfoBox("Databases", "html/databases.html")
  })
  observeEvent(input$characterizationInfo, {
    showInfoBox("Characterization", "html/characterization.html")
  })
  observeEvent(input$treatmentPathwaysInfo, {
    showInfoBox("Treatment pathways", "html/treatmentPathways.html")
  })
  observeEvent(input$resultsInfo, {
    showInfoBox("Study Results", "html/results.html")
  })
  
  output$sunburstplots <- renderUI({
    
    if (input$viewer == "Compare databases") {
      result <- lapply(input$dataset,
                       function(d) {
                         tagList(tags$h4(d), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", d, "_",input$population,"_" ,input$year,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
    } else if  (input$viewer == "Compare study populations") {
      result <- lapply(input$population,
                       function(p) {
                         tagList(tags$h4(p), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset, "_",p,"_" ,input$year,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
      
    } else if (input$viewer == "Compare over time") {
      result <- lapply(input$year,
                       function(y) {
                         tagList(tags$h4(y), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset, "_",input$population,"_" ,y,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
    }
   
    return(result)
  })
  
  
  output$dynamic_input = renderUI({
    
    if (input$viewer == "Compare databases") {
      # Select multiple databases
      one <- checkboxGroupInput("dataset", label = "Database", choices = list("IPCI" = "IPCI", 
                                                                              "CCAE" = "ccae", 
                                                                              "MDCD" = "mdcd", 
                                                                              "MDCR" = "mdcr",
                                                                              "Estonia" = "Asthma"), selected = "IPCI")
      
      # Select single population, year
      two <- selectInput("population", label = "Study population", choices = list("Asthma > 18"= "asthma",
                                                                                   "COPD > 40" = "copd",
                                                                                   "ACO > 40" = "aco",
                                                                                   "Asthma 6-17" = "asthma6plus",
                                                                                   "Asthma < 5" = "asthma6min"), selected = "asthma", multiple = FALSE)
      three <- selectInput("year", label = "Year", choices = c("all", 
                                                               "2010",
                                                               "2011",
                                                               "2012",
                                                               "2013",
                                                               "2014", 
                                                               "2015",
                                                               "2016",
                                                               "2017"), selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer == "Compare study populations") {
      # Select multiple databases
      one <- checkboxGroupInput("population", label = "Study population", choices = list("Asthma > 18"= "asthma",
                                                                                  "COPD > 40" = "copd",
                                                                                  "ACO > 40" = "aco",
                                                                                  "Asthma 6-17" = "asthma6plus",
                                                                                  "Asthma < 5" = "asthma6min"), selected = "asthma")
     
      
      # Select single population, year
      two <- selectInput("dataset", label = "Database", choices = list("IPCI" = "IPCI", 
                                                                                "CCAE" = "ccae", 
                                                                                "MDCD" = "mdcd", 
                                                                                "MDCR" = "mdcr",
                                                                                "Estonia" = "Asthma"), selected = "IPCI")
        
      three <- selectInput("year", label = "Year", choices = c("all", 
                                                               "2010",
                                                               "2011",
                                                               "2012",
                                                               "2013",
                                                               "2014", 
                                                               "2015",
                                                               "2016",
                                                               "2017"), selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer == "Compare over time") {
      # Select multiple databases
      one <- checkboxGroupInput("year", label = "Year", choices = c("all", 
                                                               "2010",
                                                               "2011",
                                                               "2012",
                                                               "2013",
                                                               "2014", 
                                                               "2015",
                                                               "2016",
                                                               "2017"), selected = "all")
      
      # Select single population, year
      two <- selectInput("dataset", label = "Database", choices = list("IPCI" = "IPCI", 
                                                                       "CCAE" = "ccae", 
                                                                       "MDCD" = "mdcd", 
                                                                       "MDCR" = "mdcr",
                                                                       "Estonia" = "Asthma"), selected = "IPCI")
      
      three <- selectInput("population", label = "Study population", choices = list("Asthma > 18"= "asthma",
                                                                                  "COPD > 40" = "copd",
                                                                                  "ACO > 40" = "aco",
                                                                                  "Asthma 6-17" = "asthma6plus",
                                                                                  "Asthma < 5" = "asthma6min"), selected = "asthma", multiple = FALSE)
     
      return(tagList(one, two, three))
      
    }
    
    
  })
  
  
})
