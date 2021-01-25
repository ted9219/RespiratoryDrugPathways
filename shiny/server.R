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
    
    if (input$viewer2 == "Compare databases") {
      result <- lapply(input$dataset,
                       function(d) {
                         tagList(tags$h4(d), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", d, "_",input$population,"_" ,input$year,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
    } else if  (input$viewer2 == "Compare study populations") {
      result <- lapply(input$population,
                       function(p) {
                         tagList(tags$h4(p), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset, "_",p,"_" ,input$year,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
      
    } else if (input$viewer2 == "Compare over time") {
      result <- lapply(input$year,
                       function(y) {
                         tagList(tags$h4(y), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset, "_",input$population,"_" ,y,".html"), width=400, height=400, scrolling = "no",frameborder = "no"))
                       })
      
    }
   
    return(result)
  })
  
  output$dynamic_input1 = renderUI({
    if (input$viewer1 == "Compare databases") {
      # Select multiple databases
      one <- checkboxGroupInput("dataset", label = "Database", choices = included_databases, selected = "IPCI")
      
      # Select single population
      two <- selectInput("population", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
      return(tagList(one, two))
      
    } else if (input$viewer1 == "Compare study populations") {
      # Select single database
      one <- selectInput("dataset", label = "Database", choices = included_databases, selected = "IPCI")
      
      return(tagList(one))
      
    }
  })
  
  output$dynamic_input2 = renderUI({
    if (input$viewer2 == "Compare databases") {
      # Select multiple databases
      one <- checkboxGroupInput("dataset", label = "Database", choices = included_databases, selected = "IPCI")
      
      # Select single population, year
      two <- selectInput("population", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
      three <- selectInput("year", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare study populations") {
      # Select multiple populations
      one <- checkboxGroupInput("population", label = "Study population", choices = all_populations, selected = "asthma")
     
      
      # Select single dataset, year
      two <- selectInput("dataset", label = "Database", choices = included_databases, selected = "IPCI")
        
      three <- selectInput("year", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare over time") {
      # Select multiple years
      one <- checkboxGroupInput("year", label = "Year", choices = all_years, selected = "all")
      
      # Select single dataset, population
      two <- selectInput("dataset", label = "Database", choices = included_databases, selected = "IPCI")
      
      three <- selectInput("population", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
     
      return(tagList(one, two, three))
    }
  })
  
  output$tableCharacterizationTitle <- renderText({"Table with characterization of study population." })
  
  output$tableCharacterization <- renderDataTable({
    if (input$viewer1 == "Compare databases") {
      # Get the data
      
      
      # Columns different databases (rows different characteristics)
      table <- NULL
      
    } else if  (input$viewer1 == "Compare study populations") { 
      
      # Get the data
      data <- characterization[[input$dataset]]

      data$sd <- NULL
      data$database_id <- NULL
      data$covariate_id <- NULL
      
      # Multiply all rows by 100 to get percentages (except Age, Charlson comorbidity index score)
      data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))] <- data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))]*100 
      
      # Rename to study populations
      data$cohort_id <- sapply(data$cohort_id, function(c) names(all_populations[c]))
      
      # Columns different study populations (rows different characteristics)
      table <- dcast(data, covariate_name ~ cohort_id, value.var = "mean")
      
      }
  
    return(table)
  })
  
})
