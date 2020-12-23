pathToCsv <- "inst/settings/drug_classes.csv"
custom_definitions <- readr::read_csv(pathToCsv, col_types = readr::cols())

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

LABA_LAMA_ICS <- intersect(concept_set_LABA,intersect(concept_set_LAMA,concept_set_ICS))
LABA_ICS <- setdiff(intersect(concept_set_LABA,concept_set_ICS), LABA_LAMA_ICS)
LABA_LAMA <- setdiff(intersect(concept_set_LABA,concept_set_LAMA), LABA_LAMA_ICS)
SABA_SAMA <- intersect(concept_set_SABA,concept_set_SAMA)

LABA_LAMA_ICS <- paste0("{", paste0(LABA_LAMA_ICS, collapse = ","), "}")
LABA_ICS <- paste0("{", paste0(LABA_ICS, collapse = ","), "}")
LABA_LAMA <- paste0("{", paste0(LABA_LAMA, collapse = ","), "}")
SABA_SAMA <- paste0("{", paste0(SABA_SAMA, collapse = ","), "}")
