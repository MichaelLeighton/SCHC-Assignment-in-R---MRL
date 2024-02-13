library(RPostgreSQL)
library(DBI)

# Establish database connection
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname='gp_practice_data', host='localhost',
                 port=5432, user='postgres',
                 password=.rs.askForPassword('Password:'))

# Don't think the following code is necessary

tables <- dbListTables(con)
df <- dbGetQuery(con, "
    select gp.practiceid as practice_id,
           min(gp.period) as earliest_date,
           max(gp.period) as latest_date
    from gp_data_up_to_2015 as gp
    inner join address as ad
    on gp.practiceid = ad.practiceid
    where ad.postcode like 'CF%'
    group by gp.practiceid
")

# Function to query practices by postcode
# formatted_postcode provides protection against future additional formatting
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

# Prompt the user to enter a postcode
user_postcode <- readline(prompt = "Enter your postcode: ")

# Fetch practice(s) by postcode
practices <- query_practices_by_postcode(user_postcode)

# Check if multiple practices are found
if (nrow(practices) == 0) {
  
  # Fetch similar practices based on first four characters of the postcode
  similar_practices <- query_similar_practices(user_postcode)
  cat("No practices found for the provided postcode. Here are a list of practices with a similar postcode:\n")
  if (nrow(similar_practices) > 0) {
    for (i in 1:nrow(similar_practices)) {
      cat(sprintf("%d. %s\n", i, similar_practices$street[i]))
    }
    selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select: "))
    if (selection < 1 || selection > nrow(similar_practices)) {
      cat("Invalid selection.\n")
    } else {
      selected_practice_id <- similar_practices$practiceid[selection]
      selected_practice_name <- similar_practices$street[selection]
      cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      
      # Fetch the postcode of the selected practice
      selected_practice_postcode <- dbGetQuery(con, sprintf("SELECT postcode FROM address WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      # Fetch top 10 drugs for the selected practice
      query <- sprintf("SELECT gp.bnfname, COUNT(*) AS total_items
                        FROM gp_data_up_to_2015 AS gp
                        JOIN address AS ad ON gp.practiceid = ad.practiceid
                        WHERE ad.postcode LIKE '%s%%'
                        GROUP BY gp.bnfname
                        ORDER BY total_items DESC
                        LIMIT 10;", selected_practice_postcode)
      
      cat("Executing SQL Query:\n", query, "\n")
      
      tryCatch({
        top_drugs <- dbGetQuery(con, query)
        # Display the top drugs
        if(nrow(top_drugs) == 0) {
          cat("No drugs found for the selected practice.\n")
        } else {
          print(top_drugs)
        }
      }, error = function(e) {
        cat("Error executing query:\n")
        message(e)
      })
    
      # Query for top 5 drug categories for the selected practice
      query_for_top_drug_categories <- sprintf("
                                                SELECT DISTINCT b.bnfchemical, b.sectiondesc, COUNT(*) AS TotalPrescriptions
                                                FROM bnf b
                                                JOIN gp_data_up_to_2015 gp ON b.bnfchemical = SUBSTRING(gp.bnfcode FROM 1 FOR LENGTH(b.bnfchemical))
                                                JOIN address a ON gp.practiceid = a.practiceid
                                                WHERE a.practiceid = '%s'
                                                GROUP BY b.bnfchemical, b.sectiondesc
                                                ORDER BY TotalPrescriptions DESC
                                                LIMIT 5;", selected_practice_id)
      
      # Execute the query for top drug categories
      top_drug_categories <- dbGetQuery(con, query_for_top_drug_categories)
      
      # Check results and display
      if(nrow(top_drug_categories) > 0) {
        print(top_drug_categories)
      } else {
        cat("No drug categories found for the selected practice.\n")
      }
      
    }
  } else {
    cat("No practices found with a similar postcode.\n")
  }
} else if (nrow(practices) == 1) {
  # Only one practice found, proceed to fetch top 10 drugs
  # Code for fetching top 10 drugs...
} else {
  # Multiple practices found, prompt user to select one
  # Code for prompting user to select one...
}

dbDisconnect(con)

#Test comment
