# Respiratory Drug Treatment Pathways Study

## Introduction
This R package contains the resources for performing the treatment pathway analysis of the study assessing respiratory drug use in patients with asthma or COPD, as described in detail in the protocol as registered at ENCePP website under registration number [...] ().

*Background*:
Today, many guidelines are available that provide clinical recommendations on asthma or COPD care with as ultimate goal to improve outcomes of patients. There is a lack of knowledge how patients newly diagnosed with asthma or COPD are treated in real-world. We give insight in treatment patterns of newly diagnosed patients across countries to help understand and address current research gaps in clinical care by utilizing the powerful analytical tools developed by the Observational Health Data Sciences and Informatics (OHDSI) community. 

*Methods*: 
This study will describe the treatment pathways of patients diagnosed with asthma, COPD or ACO. For each of the cohorts, a sunburst diagram is produced to describe the proportion of the respiratory drugs for each treatment sequence observed in the target population. 

## Installation & Usage
If you like to execute this study against your own OMOP CDM follow these instructions:

1. Download and open the R package using RStudio. 
2. Build the package (packages required are listed in DESCRIPTION file).
3. In extras -> CodeToRun.R: specify connection details + set analysis settings to TRUE. 
4. To execute the study run code in CodeToRun.R. 
5. Run the Shiny App for an interactive visualisation of the results.

````
  shiny::runApp('shiny')
````




