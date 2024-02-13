# --------------------------------------------
# Drug Prescriptions in Wales.R
# Summary Information About Drug Prescriptions At Welsh GP Practices
# --------------------------------------------

# To access the database.

library(RPostgreSQL)
library(DBI)

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname='gp_practice_data', host='localhost',
                 port=5432, user='postgres',
                 password=.rs.askForPassword('Password:'))

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

# Prompt the user to enter a GP practice ID
selected_practice_id <- readline(prompt = "Please enter the GP practice ID and press Enter: ")

# Check if the entered GP practice ID exists in the database
check_query <- sprintf("SELECT EXISTS(SELECT 1 FROM gp_data_up_to_2015 WHERE practiceid = '%s')", selected_practice_id)

exists <- dbGetQuery(con, check_query)

# If the GP practice ID does not exist in the database, inform the user and stop or prompt again
if (!exists$exists) {
  cat(sprintf("Unfortunately, %s does not exist in the database. Please try again.\n", selected_practice_id))
} else {
  query <- sprintf("SELECT ad.street AS practice_name, gp.bnfname, COUNT(*) AS total_items
                    FROM gp_data_up_to_2015 as gp
                    INNER JOIN address AS ad ON gp.practiceid = ad.practiceid
                    WHERE gp.practiceid = '%s'
                    GROUP BY ad.street, gp.bnfname
                    ORDER BY total_items DESC
                    LIMIT 10;", selected_practice_id)
  
  # Execute the main query
  results <- dbGetQuery(con, query)
  
  # Rename the columns for legibility
  names(results) <- c('GP Practice','Drug Name', 'Number of Prescriptions')
  
  # Display results
  print(results)
}

dbDisconnect(con)