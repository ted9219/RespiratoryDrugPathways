
# 1. use CreateCustomConceptSet.sql first to create drug_classes.csv
custom_definitions <- readr::read_csv("inst/settings/drug_classes.csv", col_types = readr::cols())

# 2. remove some dose forms NULL from 
# load in concepts to remove (selected by manual inspection of all concepts with missing dose forms AND searching for terms such as "rectal" / "topical" / "otic" / "nasal" / "medicated pad" / "tape")
removed_concepts <- read.csv("extras/removed_concepts.csv", sep=",")

for (m in unique(removed_concepts$med_group)) { # 
  print(paste0("Remove selected concepts for ", m))
  
  # concepts to remove
  concept_set_m_remove <- as.numeric(removed_concepts$concept_id[removed_concepts$med_group == m])
  
  # current concept set for monotherapy
  concept_set_m <- custom_definitions[custom_definitions$name == m,"conceptSet"]
  concept_set_m <- substr(concept_set_m, 2, nchar(concept_set_m)-1)
  concept_set_m <- as.numeric(unlist(strsplit(concept_set_m, ",")))
  
  concept_set_m <- setdiff(concept_set_m, concept_set_m_remove)
  
  count_m <- length(concept_set_m)
  concept_set_m <- paste0("{", paste0(concept_set_m, collapse = ","), "}")
  
  custom_definitions$count[custom_definitions$name == m] <- count_m
  custom_definitions$conceptSet[custom_definitions$name == m] <- concept_set_m
  
  # if also present as combination therapy:
  if (paste0(m, " combi") %in% custom_definitions$name) {
    
    concept_set_m_combi <- custom_definitions[custom_definitions$name == paste0(m, " combi"),"conceptSet"]
    concept_set_m_combi <- substr(concept_set_m_combi, 2, nchar(concept_set_m_combi)-1)
    concept_set_m_combi <- as.numeric(unlist(strsplit(concept_set_m_combi, ",")))
    
    concept_set_m_combi <- setdiff(concept_set_m_combi, concept_set_m_remove)
    
    count_m_combi <- length(concept_set_m_combi)
    concept_set_m_combi <- paste0("{", paste0(concept_set_m_combi, collapse = ","), "}")
    
    custom_definitions$count[custom_definitions$name == paste0(m, " combi")] <- count_m_combi
    custom_definitions$conceptSet[custom_definitions$name == paste0(m, " combi")] <- concept_set_m_combi
  }
  
}

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

# count concepts
count_LABA_LAMA_ICS <- length(LABA_LAMA_ICS)
count_LABA_ICS <- length(LABA_ICS)
count_LABA_LAMA <- length(LABA_LAMA)
count_SABA_SAMA <- length(SABA_SAMA)

# transform concept sets to string
LABA_LAMA_ICS <- paste0("{", paste0(LABA_LAMA_ICS, collapse = ","), "}")
LABA_ICS <- paste0("{", paste0(LABA_ICS, collapse = ","), "}")
LABA_LAMA <- paste0("{", paste0(LABA_LAMA, collapse = ","), "}")
SABA_SAMA <- paste0("{", paste0(SABA_SAMA, collapse = ","), "}")

# add these new concept sets
custom_definitions$count[custom_definitions$name == "LABA&LAMA&ICS"] <- count_LABA_LAMA_ICS
custom_definitions$conceptSet[custom_definitions$name == "LABA&LAMA&ICS"] <- LABA_LAMA_ICS

custom_definitions$count[custom_definitions$name == "LABA&ICS"] <- count_LABA_ICS
custom_definitions$conceptSet[custom_definitions$name == "LABA&ICS"] <- LABA_ICS

custom_definitions$count[custom_definitions$name == "LABA&LAMA"] <- count_LABA_LAMA
custom_definitions$conceptSet[custom_definitions$name == "LABA&LAMA"] <- LABA_LAMA

custom_definitions$count[custom_definitions$name == "SABA&SAMA"] <- count_SABA_SAMA
custom_definitions$conceptSet[custom_definitions$name == "SABA&SAMA"] <- SABA_SAMA


# overwrite old file
write.csv(custom_definitions, "inst/settings/drug_classes.csv", row.names = FALSE )


