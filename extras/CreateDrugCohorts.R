

# 1. use CreateCustomConceptSet.sql first to create drug_classes.csv

pathToCsv <- "inst/settings/drug_classes.csv"

custom_definitions <- readr::read_csv(pathToCsv, col_types = readr::cols())

# 2. remove some dose forms NULL from 
# load in concepts to remove (selected by manual inspection of all concepts with missing dose forms)
removed_ICS <- read.csv("extras/removed_ICS.txt", sep="")
removed_ICS <- as.numeric(unlist(removed_ICS))

removed_SAMA <- read.csv("extras/removed_SAMA.txt", sep="")
removed_SAMA <- as.numeric(unlist(removed_SAMA))

removed_SG <- read.csv("extras//removed_Systemic Glucocorticosteroids.txt", sep="")
removed_SG <- as.numeric(unlist(removed_SG))

# load in current concept sets for monotherapy
concept_set_ICSmono <- custom_definitions[custom_definitions$name == "ICS","conceptSet"]
concept_set_ICSmono <- substr(concept_set_ICSmono, 2, nchar(concept_set_ICSmono)-1)
concept_set_ICSmono <- as.numeric(unlist(strsplit(concept_set_ICSmono, ",")))

concept_set_SAMAmono <- custom_definitions[custom_definitions$name == "SAMA","conceptSet"]
concept_set_SAMAmono <- substr(concept_set_SAMAmono, 2, nchar(concept_set_SAMAmono)-1)
concept_set_SAMAmono <- as.numeric(unlist(strsplit(concept_set_SAMAmono, ",")))

concept_set_SGmono <- custom_definitions[custom_definitions$name == "Systemic glucocorticosteroids","conceptSet"]
concept_set_SGmono <- substr(concept_set_SGmono, 2, nchar(concept_set_SGmono)-1)
concept_set_SGmono <- as.numeric(unlist(strsplit(concept_set_SGmono, ",")))

# remove the concepts selected
concept_set_ICSmono <- setdiff(concept_set_ICSmono, removed_ICS)
concept_set_SAMAmono <- setdiff(concept_set_SAMAmono, removed_SAMA)
concept_set_SGmono <- setdiff(concept_set_SGmono, removed_SG)

# replace these new concept sets (copy to drug_classes.csv manually)
concept_set_ICSmono <- paste0("{", paste0(concept_set_ICSmono, collapse = ","), "}")
concept_set_SAMAmono <- paste0("{", paste0(concept_set_SAMAmono, collapse = ","), "}")
concept_set_SGmono <- paste0("{", paste0(concept_set_SGmono, collapse = ","), "}")

# 3. create fixed combinations
# load in current concept sets for combinations
concept_set_LAMA <- custom_definitions[custom_definitions$name == "LAMA combi","conceptSet"]
concept_set_LAMA <- substr(concept_set_LAMA, 2, nchar(concept_set_LAMA)-1)
concept_set_LAMA <- as.numeric(unlist(strsplit(concept_set_LAMA, ",")))

concept_set_LABA <- custom_definitions[custom_definitions$name == "LABA combi","conceptSet"]
concept_set_LABA <- substr(concept_set_LABA, 2, nchar(concept_set_LABA)-1)
concept_set_LABA <- as.numeric(unlist(strsplit(concept_set_LABA, ",")))

concept_set_ICS <- custom_definitions[custom_definitions$name == "ICS combi","conceptSet"]
concept_set_ICS <- substr(concept_set_ICS, 2, nchar(concept_set_ICS)-1)
concept_set_ICS <- as.numeric(unlist(strsplit(concept_set_ICS, ",")))

concept_set_SAMA <- custom_definitions[custom_definitions$name == "SAMA combi","conceptSet"]
concept_set_SAMA <- substr(concept_set_SAMA, 2, nchar(concept_set_SAMA)-1)
concept_set_SAMA <- as.numeric(unlist(strsplit(concept_set_SAMA, ",")))

concept_set_SABA <- custom_definitions[custom_definitions$name == "SABA combi","conceptSet"]
concept_set_SABA <- substr(concept_set_SABA, 2, nchar(concept_set_SABA)-1)
concept_set_SABA <- as.numeric(unlist(strsplit(concept_set_SABA, ",")))

# create fixed combinations
LABA_LAMA_ICS <- intersect(concept_set_LABA,intersect(concept_set_LAMA,concept_set_ICS))
LABA_ICS <- setdiff(intersect(concept_set_LABA,concept_set_ICS), LABA_LAMA_ICS)
LABA_LAMA <- setdiff(intersect(concept_set_LABA,concept_set_LAMA), LABA_LAMA_ICS)
SABA_SAMA <- intersect(concept_set_SABA,concept_set_SAMA)

# remove the concepts selected
LABA_LAMA_ICS <- setdiff(LABA_LAMA_ICS, removed_ICS)
LABA_ICS <- setdiff(LABA_ICS, removed_ICS)
SABA_SAMA <- setdiff(SABA_SAMA, removed_SAMA)

# add these new concept sets (copy to drug_classes.csv manually)
LABA_LAMA_ICS <- paste0("{", paste0(LABA_LAMA_ICS, collapse = ","), "}")
LABA_ICS <- paste0("{", paste0(LABA_ICS, collapse = ","), "}")
LABA_LAMA <- paste0("{", paste0(LABA_LAMA, collapse = ","), "}")
SABA_SAMA <- paste0("{", paste0(SABA_SAMA, collapse = ","), "}")
