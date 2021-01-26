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
  
  output$dynamic_input1 = renderUI({
    if (input$viewer1 == "Compare databases") {
      # Select multiple databases
      one <- checkboxGroupInput("dataset1", label = "Database", choices = included_databases, selected = "IPCI")
      
      # Select single population
      two <- selectInput("population1", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
      return(tagList(one, two))
      
    } else if (input$viewer1 == "Compare study populations") {
      # Select single database
      one <- selectInput("dataset1", label = "Database", choices = included_databases, selected = "IPCI")
      
      return(tagList(one))
      
    }
  })
  
  output$dynamic_input2 = renderUI({
    if (input$viewer2 == "Compare databases") {
      # Select multiple databases
      one <- checkboxGroupInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
      
      # Select single population, year
      two <- selectInput("population2", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
      three <- selectInput("year2", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare study populations") {
      # Select multiple populations
      one <- checkboxGroupInput("population2", label = "Study population", choices = all_populations, selected = "asthma")
      
      # Select single dataset, year
      two <- selectInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
        
      three <- selectInput("year2", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare over time") {
      # Select multiple years
      one <- checkboxGroupInput("year2", label = "Year", choices = all_years, selected = "all")
      
      # Select single dataset, population
      two <- selectInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
      
      three <- selectInput("population2", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
     
      return(tagList(one, two, three))
    }
  })
  
  output$tableCharacterizationTitle <- renderText({"Table with characterization of study population." })

  
  output$tableCharacterization <- renderDataTable({
    
    if (input$viewer1 == "Compare databases") {
      
      # Get the data
      data <- data.frame()
      
      for (d in input$dataset1) {
        data <- rbind(data, characterization[[d]])
      }
      
      # Rename to study populations
      data$cohort_id <- sapply(data$cohort_id, function(c) names(all_populations[c]))
      data <- data[data$cohort_id == names(which(all_populations == input$population1)),]
    
      data$sd <- NULL
      data$cohort_id <- NULL
      data$covariate_id <- NULL
      
      data$database_id <- sapply(data$database_id, function(d) names(which(included_databases == d)))
      
      # Multiply all rows by 100 to get percentages (except Age, Charlson comorbidity index score)
      data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))] <- round(data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))]*100,digits=3)
      
      # Columns different databases (rows different characteristics)
      table <- dcast(data, covariate_name ~ database_id, value.var = "mean")
      
    } else if  (input$viewer1 == "Compare study populations") { 
      
      # Get the data
      data <- characterization[[input$dataset1]]
      
      data$sd <- NULL
      data$database_id <- NULL
      data$covariate_id <- NULL
      
      # Multiply all rows by 100 to get percentages (except Age, Charlson comorbidity index score)
      data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))] <- round(data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score'))]*100,digits=3)
      
      # Rename to study populations
      data$cohort_id <- sapply(data$cohort_id, function(c) names(all_populations[c]))
      
      # Columns different study populations (rows different characteristics)
      table <- dcast(data, covariate_name ~ cohort_id, value.var = "mean")
      
    }
    
    return(table)
  }, options = list(pageLength = 20))
  
  
  
  output$sunburstplots <- renderUI({
    n_cols <- 2
    
    if (input$viewer2 == "Compare databases") {
      
      result <- list()
      
      for(i in 1:ceiling(length(input$dataset2)/n_cols)) { 
        cols_ <- list();
        
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$dataset2))) {
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(names(which(included_databases == input$dataset2[[j]]))), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset2[[j]], "_",input$population2,"_" ,input$year2,".html"), width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)

      
    } else if  (input$viewer2 == "Compare study populations") {
      
      result <- list()
      
      for(i in 1:ceiling(length(input$population2)/n_cols)) { 
        cols_ <- list();
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$population2))) {
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(names(which(all_populations == input$population2[[j]]))), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset2, "_",input$population2[[j]],"_" ,input$year2,".html"), width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)
    
      
      
    } else if (input$viewer2 == "Compare over time") {
      
      result <- list()
      
      for(i in 1:ceiling(length(input$year2)/n_cols)) { 
        cols_ <- list();
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$year2))) {
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(names(which(all_years == input$year2[[j]]))), tags$iframe(seamless="seamless", src= paste0("workingdirectory/plots/sunburst_", input$dataset2, "_",input$population2,"_" ,input$year2[[j]],".html"), width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)
    
      
    }
    
    return(result)
  })
  
})
