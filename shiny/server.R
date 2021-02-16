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
server <- function(input, output, session) {
  
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
  observeEvent(input$stepupdownInfo, {
    showInfoBox("Step up/down", "html/stepupdown.html")
  })
  
  output$dynamic_input1 = renderUI({
    if (input$viewer1 == "Compare databases") {
      one <- checkboxGroupInput("dataset1", label = "Database", choices = included_databases, selected = "IPCI")
      two <- selectInput("population1", label = "Study population", choices = all_populations, selected = "asthma")
      return(tagList(one, two))
      
    } else if (input$viewer1 == "Compare study populations") {
      one <- selectInput("dataset1", label = "Database", choices = included_databases, selected = "IPCI")
      return(tagList(one))
      
    }
  })
  
  output$dynamic_input2 = renderUI({
    if (input$viewer2 == "Compare databases") {
      one <- checkboxGroupInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
      two <- selectInput("population2", label = "Study population", choices = all_populations, selected = "asthma")
      three <- selectInput("year2", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare study populations") {
      one <- checkboxGroupInput("population2", label = "Study population", choices = all_populations, selected = "asthma")
      two <- selectInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
      three <- selectInput("year2", label = "Year", choices = all_years, selected = "all")
      return(tagList(one, two, three))
      
    } else if (input$viewer2 == "Compare over time") {
      one <- checkboxGroupInput("year2", label = "Year", choices = all_years, selected = "all")
      two <- selectInput("dataset2", label = "Database", choices = included_databases, selected = "IPCI")
      three <- selectInput("population2", label = "Study population", choices = all_populations, selected = "asthma", multiple = FALSE)
      return(tagList(one, two, three))
      
    }
  })
  
  output$tableCharacterizationTitle <- renderText({"Table with selected demographics and patient characteristics." })
  
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
      
      # Multiply all rows by 100 to get percentages (except Age, Charlson comorbidity index score, Number of persons)
      data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score', 'Number of persons'))] <- round(data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score', 'Number of persons'))]*100,digits=3)
      
      # Columns different databases (rows different characteristics)
      table <- reshape2::dcast(data, covariate_name ~ database_id, value.var = "mean")
      
      # Sort
      table  <- table[order(match(table$covariate_name,orderRows)),]  
      
    } else if  (input$viewer1 == "Compare study populations") { 
      
      # Get the data
      data <- characterization[[input$dataset1]]
      
      data$sd <- NULL
      data$database_id <- NULL
      data$covariate_id <- NULL
      
      # Multiply all rows by 100 to get percentages (except Age, Charlson comorbidity index score, Number of persons)
      data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score', 'Number of persons'))] <- round(data$mean[!(data$covariate_name %in% c('Age', 'Charlson comorbidity index score', 'Number of persons'))]*100,digits=3)
      
      # Rename to study populations
      data$cohort_id <- sapply(data$cohort_id, function(c) names(all_populations[c]))
      
      # Columns different study populations (rows different characteristics)
      table <- reshape2::dcast(data, covariate_name ~ cohort_id, value.var = "mean")
      
      # Sort
      table <- table[order(match(table$covariate_name,orderRows)),]
    }
    
    return(table)
  }, options = list(pageLength = 20))
  
  result_sunburstplot <- reactive({
    
    n_cols <- 2
    
    result <- list()
    
    inhalation <- ""
    if (input$inhalation2 == "Yes") {
      inhalation <- "_inhaler"
    } 
    
    if (input$viewer2 == "Compare databases") {
      
      for(i in 1:ceiling(length(input$dataset2)/n_cols)) { 
        cols_ <- list();
        
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$dataset2))) {
          
          info <- summary_counts[[input$dataset2[[j]]]][[paste0(input$population2, inhalation)]]
          title_plot <- paste0(names(which(included_databases == input$dataset2[[j]])), " (N = ", info$number_target[info$year == input$year2], " , Treated % = ", info$perc[info$year == input$year2], ")")
          plot_location <- paste0("workingdirectory/plots/sunburst_", input$dataset2[[j]], "_",input$population2, inhalation, "_" ,input$year2,".html")
          
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(title_plot), tags$iframe(seamless="seamless", src=plot_location, width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)
      
    } else if  (input$viewer2 == "Compare study populations") {
      
      for(i in 1:ceiling(length(input$population2)/n_cols)) { 
        cols_ <- list();
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$population2))) {
          
          info <- summary_counts[[input$dataset2]][[paste0(input$population2[[j]], inhalation)]]
          title_plot <- paste0(names(which(all_populations == input$population2[[j]])), " (N = ", info$number_target[info$year == input$year2], " , Treated % = ", info$perc[info$year == input$year2], ")")
          plot_location <- paste0("workingdirectory/plots/sunburst_", input$dataset2, "_",input$population2[[j]], inhalation, "_" ,input$year2,".html")
          
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(title_plot), tags$iframe(seamless="seamless", src=plot_location, width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)
      
    } else if (input$viewer2 == "Compare over time") {
      
      for(i in 1:ceiling(length(input$year2)/n_cols)) { 
        cols_ <- list();
        for(j in (1+n_cols*(i-1)):min(i*n_cols, length(input$year2))) {
          
          info <- summary_counts[[input$dataset2]][[paste0(input$population2, inhalation)]]
          title_plot <- paste0(names(which(all_years == input$year2[[j]])), " (N = ", info$number_target[info$year == input$year2[[j]]], " , Treated % = ", info$perc[info$year == input$year2[[j]]], ")")
          plot_location <- paste0("workingdirectory/plots/sunburst_", input$dataset2, "_",input$population2, inhalation, "_" ,input$year2[[j]],".html")
          
          cols_ <- append(cols_,list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(title_plot), tags$iframe(seamless="seamless", src=plot_location, width=400, height=400, scrolling = "no", frameborder = "no")))));
        }
        result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
      }
      do.call(tagList, result)
    }
    
    return(result)
  }) 
  
  output$sunburstplots <- renderUI({
    
    result <- result_sunburstplot()
    
    return(result)
  })
  
  
  output$stepupdownpie <- renderUI({
    
    # Get the data
    data <- data.frame()
    
    for (d in input$dataset) {
      data <- rbind(data, cbind(stepupdown[[d]][[input$population]], d))
    }
    
    data <- as.data.table(data)
    data <- data[layer == input$level,]
    
    data$group <- data$category
    data$group[data$group == "step_up_broad"] <- "step_up"
    data$group[data$group == "step_down_broad"] <- "step_down"
    data$group[data$group == "switching_broad"] <- "switching"
    data$group[data$group == "acute_exacerbation + step_up"] <- "acute_exacerbation"
    data$group[data$group == "end_of_acute_exacerbation + step_up"] <- "end_of_acute_exacerbation"
    
    output <- data[,sum(perc), by = .(group, d)]
    colnames(output) <- c("group", "dataset", "perc")
    output$colors <- sapply(output$group, function(g) colors[[g]])
    
    n_cols <- 2
    result <- list()
    
    for(i in 1:ceiling(length(input$dataset)/n_cols)) { 
      cols_ <- lapply((1+n_cols*(i-1)):min(i*n_cols, length(input$dataset)), function(j) {
        d <- input$dataset[[j]]
        
        output_d <- output[output$dataset == d,]
        
        title_plot <- paste0(names(which(included_databases == d)), " (Layer ", input$level, " to ",as.integer(input$level)+1, " for ", names(which(all_populations == input$population)), ")")
        
        return(list(column(width = floor(8/n_cols), offset = 0, tagList(tags$h4(title_plot), renderPlot({pie(output_d$perc, labels = output_d$group, col = output_d$colors, border = "white")})))))
      })
      
      result <- append(result, list(fluidRow(cols_, style = "width:1200px" )));
    }
    do.call(tagList, result)
    
    # result <- lapply(input$dataset, function(d) renderPlot({pie(output$perc[output$dataset == d], labels = output$group[output$dataset == d], col = output$colors[output$dataset == d], border = "white", main = paste0(names(which(included_databases == d)), " (Layer ", input$level, " to ",as.integer(input$level)+1, " for ", names(which(all_populations == input$population)), ")"))}))
    
    return(result)
    
    
  })
  
  output$tableSummaryClassesTitle <- renderText({paste0("Summary table with percentages of drug classes and combinations of 'layer ",input$layer3, " in '", names(which(all_years == input$year3)), "'.") })
  
  output$tableSummaryClasses <- renderDataTable({
    
    # Get the data
    if (input$year3 == "all") {
      data <- summary_drugclasses[[input$dataset]][[input$population]]
    } else {
      data <- summary_drugclasses_year[[input$dataset]][[input$population]]
      
      data <- data[data$y == input$year3,]
      data$y <- NULL
    }
    
    # Select and rename column
    col_name <- paste0("D", input$layer3, "_CONCEPT_NAME")
    table <- data[,c("outcomes", col_name)]
    colnames(table) <- c("Group", paste0("Layer ", input$layer3))
    
    return(table)
  }, options = list(pageLength = 20))
  
  output$figureSummaryClassesTitleYears <- renderText({
    paste0("Figure with percentages of drug classes and combinations of 'layer ", input$layer3,"' over the different years.")
  })
  
  output$figureSummaryClassesYears <- renderPlot({
    data <- summary_drugclasses_year[[input$dataset]][[input$population]]
    
    col_name <- paste0("D", input$layer3, "_CONCEPT_NAME")
    
    plot.data <- data[,c("y", "outcomes", col_name)]
    colnames(plot.data) <- c("Year", "Group", "Percentage")
    
    # Plot
    ggplot(plot.data) +
      geom_line(mapping = aes(x = Year, y = Percentage, group = Group, colour = Group))  + 
      labs (x = "Years", y = "Percentage (%)", title = "") 
  })
  
  output$figureSummaryClassesTitleLayers <- renderText({
    paste0("Figure with percentages of drug classes and combinations in '", names(which(all_years == input$year3)) , "' over the different layers.")
  })
  
  output$figureSummaryClassesLayers <- renderPlot({
    
    if (input$year3 == "all") {
      data <- summary_drugclasses[[input$dataset]][[input$population]]
    } else {
      data <- summary_drugclasses_year[[input$dataset]][[input$population]]
      
      data <- data[data$y == input$year3,]
      data$y <- NULL
    }
    
    # Transform
    plot.data <- reshape2::melt(data, id.vars = 'outcomes')
    plot.data$variable <- stringr::str_replace(plot.data$variable, "_CONCEPT_NAME", "")
    plot.data$variable <- stringr::str_replace(plot.data$variable, "D", "")
    
    colnames(plot.data) <- c("Group", "Layer", "Percentage")
    
    # Plot
    ggplot(plot.data) +
      geom_line(mapping = aes(x = Layer, y = Percentage, group = Group, colour = Group))  + 
      labs (x = "Layers", y = "Percentage (%)", title = "") 
    
  })
  
  
  
}

