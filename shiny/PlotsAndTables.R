filteredProportions <- function(data, stratCriteria, dateFilter, cohortId) {
  data <- data[data$proportion > 0, ]
  if (nrow(data) == 0) {
    print(paste0("No Data for CohortId:", cohortId))
    return(NULL)
  }
  stratifyByAge <- "Age" %in% stratCriteria
  stratifyByGender <- "Gender" %in% stratCriteria
  stratifyByCalendarYear <- "Calendar Year" %in% stratCriteria
  #minPersonYears = 1000
  
  idx <- rep(TRUE, nrow(data))
  if (stratifyByAge) {
    idx <- idx & !is.na(data$ageGroup)
  } else {
    idx <- idx & is.na(data$ageGroup)
  }
  if (stratifyByGender) {
    idx <- idx & !is.na(data$gender)
  } else {
    idx <- idx & is.na(data$gender)
  }
  if (stratifyByCalendarYear) {
    idx <- idx & !is.na(data$calendarYear)
    idx <- idx & (data$calendarYear >= dateFilter[1])
    idx <- idx & (data$calendarYear <= dateFilter[2])
  } else {
    idx <- idx & is.na(data$calendarYear)
  }
  data <- data[idx, ]
  data <- data[data$cohortCount > 0, ]
  data$gender <- as.factor(data$gender)
  data$calendarYear <- as.integer(as.character(data$calendarYear))
  ageGroups <- unique(data$ageGroup)
  ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
  data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
  data <- data[data$proportion > 0, ]
  data$proportion <- data$proportion
  data$dummy <- 0
  if (nrow(data) == 0) {
    return(NULL)
  } else {
    return(data)
  }
}

plotProportion <- function(data,
                           stratifyByAge = TRUE,
                           stratifyByGender = TRUE,
                           stratifyByCalendarYear = TRUE,
                           yAxisLabel = "",
                           scales = "free_y",
                           fileName = NULL) {
  aesthetics <- list(y = "proportion")
  if (stratifyByCalendarYear) {
    aesthetics$x <- "calendarYear"
    xLabel <- "Calendar year"
    showX <- TRUE
    if (stratifyByGender) {
      aesthetics$group <- "gender"
      aesthetics$color <- "gender"
    }
    plotType <- "line"
  } else {
    xLabel <- ""
    if (stratifyByGender) {
      aesthetics$x <- "gender"
      aesthetics$color <- "gender"
      aesthetics$fill <- "gender"
      showX <- TRUE
    } else {
      aesthetics$x <- "dummy"
      showX <- FALSE
    }
    plotType <- "bar"
    
  }
  
  plot <- ggplot2::ggplot(data = data, do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab(xLabel) +
    ggplot2::ylab(yAxisLabel) +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = if (showX) ggplot2::element_text(angle = 90, vjust = 0.5) else ggplot2::element_blank() )
  
  if (plotType == "line") {
    plot <- plot + ggplot2::geom_line(size = 1.25, alpha = 0.6) +
      ggplot2::geom_point(size = 1.25, alpha = 0.6)
  } else {
    plot <- plot + ggplot2::geom_bar(stat = "identity", alpha = 0.6)
  }

  if (!is.null(data$databaseId)) {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(databaseId~ageGroup, scales = scales)
    } else {
      plot <- plot + ggplot2::facet_grid(databaseId~., scales = scales) 
    }
  } else {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(~ageGroup) 
    }
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}


getProportionTooltip <- function(proportionType = "Incidence",
                                     top_px,
                                     point) {
  text <- gsub("-", "<", sprintf("<b>%s proportion: </b> %0.3f per 1000 persons", proportionType, point$proportion))
  if (!is.na(point$ageGroup)) {
    text <- paste(text, sprintf("<b>Age group: </b> %s years", point$ageGroup), sep = "<br/>")
    top_px <- top_px - 15
  }
  if (!is.na(point$gender)) {
    text <- paste(text, sprintf("<b>Gender: </b> %s", point$gender), sep = "<br/>")
    top_px <- top_px - 15
  }
  if (!is.na(point$calendarYear)) {
    text <- paste(text, sprintf("<b>Calendar year: </b> %s", point$calendarYear), sep = "<br/>")
    top_px <- top_px - 15
  }
  if (!is.na(point$cohortCount)) {
    text <- paste(text, sprintf("<b>%s patients: </b> %s", proportionType, scales::comma(point$cohortCount)), sep = "<br/>")
    top_px <- top_px - 15
  }
  if (!is.na(point$numPersons)) {
    text <- paste(text, sprintf("<b>Denominator: </b> %s", scales::comma(point$numPersons)), sep = "<br/>")
    top_px <- top_px - 15
  }
  text <- paste(text, sprintf("<b>Database: </b> %s", point$databaseId), sep = "<br/>")
  return(list(top_px = top_px, text = text))
}

# Tables

table6AColumns <- c("Ingredient",
                    "Formulation",
                    "Variable",
                    "N",
                    "N180-GERD",
                    "P180-GERD",
                    "N365-GERD",
                    "P365-GERD",
                    "N180-ULCER",
                    "P180-ULCER",
                    "N365-ULCER",
                    "P365-ULCER",
                    "N180-ZES",
                    "P180-ZES",
                    "N365-ZES",
                    "P365-ZES",
                    "N-Unknown",
                    "P-Unknown")

tableBColumns <- c("ICH_group",
                    "Age",
                    "Gender",
                    "N users",
                    #"Excluded",
                    "Mean",
                    "Median",
                    "P5",
                    "Q1",
                    "Q3",
                    "P95",
                    "Min",
                    "Max")

# Funtion that removes from the variable column the second occurrence of the same value.This is used to format the tables.
clearSecondOccurrenceVariable <- function(data, variable) {
  if (!is.null(data) && nrow(data) > 1) {
    current <- data[1, variable]
    for (row in 2:nrow(data)) {
      same <- all_equal(data[row, variable], current)
      if (same != TRUE) {
        current <- data[row, variable]
      } else {
        data[row, variable] = ""
      }
    }
  }
  return(data)
}
