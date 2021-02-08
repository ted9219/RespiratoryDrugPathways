
### Old vs new concept sets drug classes

custom_definitions_old <- readr::read_csv("~/Desktop/drug_classes_old.csv", col_types = readr::cols())
custom_definitions_new <- readr::read_csv("~/Desktop/drug_classes_new.csv", col_types = readr::cols())

# load in current concept sets for monotherapy
concept_set_old <- custom_definitions_old[custom_definitions_old$name == "LABA","conceptSet"]
concept_set_old <- substr(concept_set_old, 2, nchar(concept_set_old)-1)
concept_set_old <- as.numeric(unlist(strsplit(concept_set_old, ",")))

concept_set_new <- custom_definitions_new[custom_definitions_new$name == "LABA","conceptSet"]
concept_set_new <- substr(concept_set_new, 2, nchar(concept_set_new)-1)
concept_set_new <- as.numeric(unlist(strsplit(concept_set_new, ",")))

# remove the concepts selected
inold_notinnew <- setdiff(concept_set_old, concept_set_new)


# print difference
print <- paste0("{", paste0(inold_notinnew, collapse = ","), "}")


### Check correctness concept set
pathToCsv <- "inst/settings/drug_classes.csv"

custom_definitions <- readr::read_csv(pathToCsv, col_types = readr::cols())

concept_set <- custom_definitions[custom_definitions$name == "ICS","conceptSet"]
concept_set <- substr(concept_set, 2, nchar(concept_set)-1)
concept_set <- as.numeric(unlist(strsplit(concept_set, ",")))
print(paste0(concept_set, collapse = ","))

