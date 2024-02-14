library(RPostgreSQL)
library(DBI)

# Establish database connection
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname='gp_practice_data', host='localhost',
                 port=5432, user='postgres',
                 password=.rs.askForPassword('Password:'))


###### FUNCTIONS ######

# Function to query practices by postcode
query_practices_by_postcode <- function(postcode) {
  formatted_postcode <- sprintf("%s", postcode)
  query <- sprintf("
                   SELECT DISTINCT ad.practiceid, ad.street 
                   FROM address AS ad 
                   WHERE ad.postcode LIKE '%s' 
                   ORDER BY ad.street", formatted_postcode)
  return(dbGetQuery(con, query))
}

# Function to query similar practices by postcode (first four characters)
query_similar_practices <- function(postcode) {
  formatted_postcode <- sprintf("%s%%", substr(postcode, 1, 4))
  query <- sprintf("
                   SELECT DISTINCT ad.practiceid, ad.street 
                   FROM address AS ad 
                   WHERE ad.postcode LIKE '%s' 
                   ORDER BY ad.street", formatted_postcode)
  return(dbGetQuery(con, query))
}

# Function to calculate median threshold
calculate_median_threshold <- function(con) {
  query <- "
    WITH PrescriptionCounts AS (
      SELECT practiceid, COUNT(*) AS total_prescriptions
      FROM gp_data_up_to_2015
      GROUP BY practiceid
    ),
    RankedPrescriptions AS (
      SELECT total_prescriptions,
             ROW_NUMBER() OVER (ORDER BY total_prescriptions) AS rn,
             COUNT(*) OVER () AS cnt
      FROM PrescriptionCounts
    )
    SELECT AVG(total_prescriptions) AS median_prescriptions
    FROM RankedPrescriptions
    WHERE rn IN (FLOOR((cnt + 1) / 2), FLOOR((cnt + 2) / 2));
  "
    result <- dbGetQuery(con, query)
  
  median_threshold <- result$median_prescriptions[1]
  
  return(median_threshold)
}

# Function to define size of practice
define_practice_size_by_median <- function(con, practice_id) {
  # Dynamically calculate the median threshold
  median_threshold <- calculate_median_threshold(con)
  
  # SQL query to count total prescriptions for the given practice_id
  query <- sprintf("
                   SELECT COUNT(*) AS total_prescriptions
                   FROM gp_data_up_to_2015
                   WHERE practiceid = '%s'", practice_id)
  
  cat("\nDetermining size of practice...\n")
  
  # Execute the query
  result <- dbGetQuery(con, query)
  
  # Extract the total prescriptions count
  total_prescriptions <- ifelse(nrow(result) > 0, result$total_prescriptions, 0)
  
  # Categorize the practice based on the median threshold
  practice_size <- if (total_prescriptions > median_threshold) 'big' else 'small'
  
  cat(sprintf("\nThe size of this practice (based on number of prescriptions in Wales) is: %s.\n", practice_size))
  
  return(practice_size)
}

# Function to fetch and display the top 10 drugs by practice
fetch_and_display_top_drugs <- function(con, selected_practice_postcode) {
  query <- sprintf("SELECT gp.bnfname, COUNT(*) AS total_items
                    FROM gp_data_up_to_2015 AS gp
                    JOIN address AS ad ON gp.practiceid = ad.practiceid
                    WHERE ad.postcode LIKE '%s%%'
                    GROUP BY gp.bnfname
                    ORDER BY total_items DESC
                    LIMIT 10;", selected_practice_postcode)
  
  cat("\nFetching Top 10 Drugs Prescribed...\n")
  
  top_drugs <- dbGetQuery(con, query)
  
  if(nrow(top_drugs) == 0) {
    cat("No drugs found for the selected practice.\n")
  } else {
    colnames(top_drugs) <- c('Drug Name', 'Total Prescriptions')
    cat("\nTop 10 drugs prescribed:\n=======================\n")
    print(top_drugs)
  }
}

# Function to fetch and display top 5 drug categories
fetch_and_display_top_drug_categories <- function(con, selected_practice_id) {
  query_for_top_drug_categories <- sprintf("
                                            SELECT b.sectiondesc, COUNT(*) AS TotalPrescriptions
                                            FROM bnf AS b
                                            JOIN gp_data_up_to_2015 AS gp ON LEFT(b.bnfchemical, 6) = LEFT(gp.bnfcode, 6)
                                            JOIN address AS ad ON gp.practiceid = ad.practiceid
                                            WHERE ad.practiceid = '%s'
                                            GROUP BY b.sectiondesc
                                            ORDER BY TotalPrescriptions DESC
                                            LIMIT 5;", selected_practice_id)
  
  cat("\nFetching Top 5 Drug Categories Prescribed...\n")
  
  top_drug_categories <- dbGetQuery(con, query_for_top_drug_categories)
  if(nrow(top_drug_categories) > 0) {
    colnames(top_drug_categories) <- c('Drug Type', 'Total Prescriptions')
    cat("\nTop 5 drug categories:\n=====================\n")
    print(top_drug_categories)
  } else {
    cat("No drug categories found for the selected practice.\n")
  }
}


###### PROGRAM INTRODUCTION ######

# Prompt the user to enter a postcode
user_postcode <- readline(prompt = "Enter the postcode of the GP of interest: ")

# Fetch practice(s) by postcode
practices <- query_practices_by_postcode(user_postcode)

# Check if multiple practices are found
# If no practices are found
if (nrow(practices) == 0) {
  
  # Fetch similar practices based on first four characters of the postcode
  similar_practices <- query_similar_practices(user_postcode)
  cat("No practices found for the provided postcode. Finding practices with a similar postcode...\n")
  # If there are 1 or more practices found: 
  if (nrow(similar_practices) > 0) {
    for (i in 1:nrow(similar_practices)) {
      cat(sprintf("%d. %s\n", i, similar_practices$street[i]))
    }
    selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select and press Enter: "))
    if (selection < 1 || selection > nrow(similar_practices)) {
      cat("Invalid selection.\n")
    } else {
      selected_practice_id <- similar_practices$practiceid[selection]
      selected_practice_name <- similar_practices$street[selection]
      cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      
      # Fetch the postcode of the selected practice
      selected_practice_postcode <- dbGetQuery(con, sprintf("
                                                            SELECT postcode 
                                                            FROM address 
                                                            WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      # Call the function to categorize and display practice size
      define_practice_size_by_median(con, selected_practice_id)
      
      # Call the function to fetch and display top 10 drugs
      fetch_and_display_top_drugs(con, selected_practice_postcode)
    
      # Call the function to fetch and display top 5 drug categories
      fetch_and_display_top_drug_categories(con, selected_practice_id)
      
    }
  } else {
    cat("No practices found with a similar postcode.\n")
  }
} else if (nrow(practices) == 1) {
  # Fetch the postcode of the selected practice
    selected_practice_id <- practices$practiceid[1]
    selected_practice_name <- practices$street[1]
    selected_practice_postcode <- dbGetQuery(con, sprintf("
                                                            SELECT postcode 
                                                            FROM address 
                                                            WHERE practiceid = '%s'", selected_practice_id))$postcode
    
    # Call the function to categorize and display practice size
    define_practice_size_by_median(con, selected_practice_id)
    
    # Call the function to fetch and display top 10 drugs
    fetch_and_display_top_drugs(con, selected_practice_postcode)
    
    # Call the function to fetch and display top 5 drug categories
    fetch_and_display_top_drug_categories(con, selected_practice_id)
    
} else {
  cat("\nMultiple practices found for the provided postcode. Please select one from the list below:\n")
  for (i in 1:nrow(practices)) {
    cat(sprintf("%d: %s\n", i, practices$street[i]))
  }
  selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select: "))
  
  # Validate the selection
  if (selection < 1 || selection > nrow(practices)) {
    cat("Invalid selection.\n")
  } else {
    selected_practice_id <- practices$practiceid[selection]
    selected_practice_name <- practices$street[selection]
    cat(sprintf("\nSelected practice: %s. Please wait a few seconds...\n", selected_practice_name))
    
    # Fetch the postcode of the selected practice
    selected_practice_postcode <- dbGetQuery(con, sprintf("SELECT postcode FROM address WHERE practiceid = '%s'", selected_practice_id))$postcode
    
    # Call the function to categorize and display practice size
    define_practice_size_by_median(con, selected_practice_id)
    
    # Call function to fetch and display top 10 drugs
    fetch_and_display_top_drugs(con, selected_practice_postcode)
    
    # Query for top 5 drug categories for the selected practice
    fetch_and_display_top_drug_categories(con, selected_practice_id)
    
  }
}


dbDisconnect(con)