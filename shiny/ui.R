library(shinydashboard)
library(shiny)
library(DT)
library(plotly)

addInfo <- function(item, infoId) {
  infoTag <- tags$small(
    class = "badge pull-right action-button",
    style = "padding: 1px 6px 2px 6px; background-color: steelblue;",
    type = "button",
    id = infoId,
    "i"
  )
  item$children[[1]]$children <-
    append(item$children[[1]]$children, list(infoTag))
  return(item)
}

ui <- dashboardPage(
  dashboardHeader(title = "Pathways Results"),
  dashboardSidebar(
    sidebarMenu(
      id = "tabs",
      ## Tabs
      addInfo(menuItem("About", tabName = "about"), "aboutInfo"),
      addInfo(menuItem("Databases", tabName = "databases"), "databaseInfo"),
      addInfo(menuItem("Characterization", tabName = "characterization"), "characterizationInfo"),
      addInfo(menuItem("Treatment pathways", tabName = "pathways"), "treatmentPathwaysInfo"),
      addInfo(menuItem("Summary pathways", tabName = "summarypathway"), "summaryPathwaysInfo"),
      addInfo(menuItem("Duration eras", tabName = "duration"), "durationInfo"),
      addInfo(menuItem("Step up/down", tabName = "stepupdown"), "stepupdownInfo"),
     
      ## Option panel
      conditionalPanel(
        condition = "input.tabs=='characterization'",
        radioButtons("viewer1", label = "Viewer", choices = c("Compare databases", "Compare study populations"), selected = "Compare databases")
      ),
      
      conditionalPanel(
        condition = "input.tabs=='characterization'",
        htmlOutput("dynamic_input1")),
      
      conditionalPanel(
        condition = "input.tabs=='pathways'",
        radioButtons("viewer2", label = "Viewer", choices = c("Compare databases", "Compare study populations", "Compare over time"), selected = "Compare databases")
      ),
      
     conditionalPanel(
      condition = "input.tabs=='pathways'",
      htmlOutput("dynamic_input2")),

     conditionalPanel(
       condition = "input.tabs=='pathways'",
       selectInput("inhalation2", label = "Show only inhalation", choices = c("Yes", "No"), selected = "No")
     ),

    conditionalPanel(
      condition = "input.tabs=='summarypathway' || input.tabs=='duration'",
      selectInput("dataset34", label = "Database", choices = included_databases, selected = "IPCI")
    ),
    
    conditionalPanel(
      condition = "input.tabs=='summarypathway' || input.tabs=='duration' || input.tabs=='stepupdown'",
      selectInput("population345", label = "Study population", choices = all_populations, selected = "asthma")
    ),
    
    conditionalPanel(
      condition = "input.tabs=='summarypathway'",
      selectInput("year3", label = "Year", choices = all_years, selected = "all")),
    
    conditionalPanel(
      condition = "input.tabs=='summarypathway'",
      radioButtons("layer3", label = "Treatment layer", choices = layers, selected = 1)),
    
    conditionalPanel(
      condition = "input.tabs=='stepupdown'",
      checkboxGroupInput("dataset5", label = "Database", choices = included_databases, selected = "IPCI")
    ),
      conditionalPanel(
        condition = "input.tabs=='stepupdown'",
        radioButtons("transition5", label = "Transition after treatment layer", choices = layers[1:3], selected = 1)
      )
    
   
    )
    
  ),
  dashboardBody(
    
    tags$body(tags$div(id="ppitest", style="width:1in;visible:hidden;padding:0px")),
    tags$script('$(document).on("shiny:connected", function(e) {
                                    var w = window.innerWidth;
                                    var h = window.innerHeight;
                                    var d =  document.getElementById("ppitest").offsetWidth;
                                    var obj = {width: w, height: h, dpi: d};
                                    Shiny.onInputChange("pltChange", obj);
                                });
                                $(window).resize(function(e) {
                                    var w = $(this).width();
                                    var h = $(this).height();
                                    var d =  document.getElementById("ppitest").offsetWidth;
                                    var obj = {width: w, height: h, dpi: d};
                                    Shiny.onInputChange("pltChange", obj);
                                });
                            '),
    
    tabItems(
      tabItem(
        tabName = "about",
        br(),
        p(
          "This web-based application provides an interactive platform to explore the results of the RespiratoryDrugPathways R Package. 
          This R package contains the resources for performing the treatment pathway analysis of the study assessing respiratory drug use in patients with asthma or COPD, as described in detail in the protocol as registered at ENCePP website under registration number (to be added)."
        ),
        HTML("<li>R study package: <a href=\"https://github.com/AniekMarkus/RespiratoryDrugPathways\">GitHub</a></li>"),
        # HTML("<li>The study is registered: <a href=\"http://www.encepp.eu/encepp/viewResource.htm?id=33398\">EU PASS Register</a></li>"),
        h3("Background"),
        p("Today, many guidelines are available that provide clinical recommendations on asthma or COPD care with as ultimate goal to improve outcomes of patients. There is a lack of knowledge how patients newly diagnosed with asthma or COPD are treated in real-world. We give insight in treatment patterns of newly diagnosed patients across countries to help understand and address current research gaps in clinical care by utilizing the powerful analytical tools developed by the Observational Health Data Sciences and Informatics (OHDSI) community."),
           h3("Methods"),
        p("This study will describe the treatment pathways of patients diagnosed with asthma, COPD or ACO. For each of the cohorts, a sunburst diagram is produced to describe the proportion of the respiratory drugs for each treatment sequence observed in the target population."),
        h3("Development Status"),
        p(
          "The results presented in this application are not final yet and should be treated as such (no definite conclusions can be drawn based upon this and the results should not be distributed further). Because the study is currently in progress there can be (minor) differences between results of databases at each point in time due to the fact that  slightly different versions of the study package were run."
        )
      ),
      tabItem(
        tabName = "databases",
        includeHTML("./html/databasesInfo.html")
      ),
      tabItem(tabName = "characterization",
              box(width = 12,
                textOutput("tableCharacterizationTitle"),
                dataTableOutput("tableCharacterization")
              )
      ),
      tabItem(tabName = "pathways",
              column(width = 9, 
                     box(
                       title = "Treatment Pathways", width = 30, status = "primary",
                       htmlOutput("sunburstplots"))),
              column(width = 3, tags$img(src = paste0("workingdirectory/plots/legend.png"), height = 400))
      ),
      
      tabItem(tabName = "summarypathway",
              box(width = 6,
                  textOutput("tableSummaryPathwayTitle"),
                  dataTableOutput("tableSummaryPathway")
              ),
              box(width = 6,
                  textOutput("figureSummaryPathwayTitleYears"),
                  plotOutput("figureSummaryPathwayYears", height = "450px"),
                  textOutput("figureSummaryPathwayTitleLayers"),
                  plotOutput("figureSummaryPathwayLayers", height = "450px"),
              )
      ),
      
      tabItem(tabName = "duration",
              
              tabsetPanel(
                id = "resultDurationPanel",
                
                tabPanel(
                  "Tables",
                  br(),
                  textOutput("tableDurationTitle"),
                  br(),
                  
                 dataTableOutput("tableDuration")
                ),
                
                tabPanel(
                  "Figures",
                  br(),
                  # textOutput("tableDurationTitle"),
                  br(),
                  
                  plotOutput("heatmapDuration", height = "500px")
                  )
                  
                )
              ),
              
              # box(width = 12,
              #    textOutput("tableDurationTitle"),
              #    dataTableOutput("tableDuration")
              # )
      tabItem(tabName = "stepupdown",
              box(width = 12,
                  uiOutput("stepupdownpie")
              )
      )
    )
  )
)




