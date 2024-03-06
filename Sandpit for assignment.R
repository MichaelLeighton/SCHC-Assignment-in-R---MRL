library(RPostgreSQL) # Database Interface (DBI) compliant driver to access PostgreSQL  
library(DBI) # Database management connections
library(ggplot2) # For plotting 
library(sf) # Spatial manipulation package - used in Q2.1 choropleth map, st_read() function to read the dataset as a sf object
library(dplyr)
library(tidyr)
library(patchwork) # For grouping plots in one display
library(scales) # for percentage display in 2.1 function (NOTE: double check if I kept this)
library(cluster) # For cluster analysis in 2.3 Spend (NOTE: double check if I kept this)
library(lubridate) # For W0 vs W9 analysis


# Global variable to control the main loop
.keep_running <- TRUE

# Establish database connection
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname='gp_practice_data', host='localhost',
                 port=5432, user='postgres',
                 password=.rs.askForPassword('Password:'))


#### FUNCTIONS #### ######

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
  # Median threshold was chosen to account for data that is not normally distributed, as a mean threshold would skew the data.
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
  
  # Apply median threshold calculation function
  median_threshold <- calculate_median_threshold(con)
  
  # Query to count total prescriptions for the given practice_id
  query <- sprintf("
                   SELECT COUNT(*) AS total_prescriptions
                   FROM gp_data_up_to_2015
                   WHERE practiceid = '%s'", practice_id)
  
  # Execute the query
  result <- dbGetQuery(con, query)
  
  # Extract the total prescriptions count
  total_prescriptions <- ifelse (nrow(result) > 0, result$total_prescriptions, 0)
  
  # Categorize the practice based on the median threshold
  practice_size <- ifelse (total_prescriptions > median_threshold, 'Big', 'Small')
  
  cat(sprintf("\nThe size of this practice (based on number of prescriptions in Wales) is classified as: %s.\n", practice_size))
  
  return(practice_size)
}

# Function to fetch and display the top 10 drugs by practice
fetch_and_display_top_drugs <- function(con, selected_practice_postcode) {
  query <- sprintf("SELECT gp.bnfname, SUM(gp.items) AS total_items
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

# Function to fetch hypertension data for a specific practice
fetch_hypertension_rate_specific <- function(con, practice_id) {
  query <- sprintf("
    SELECT indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE orgcode = '%s' AND indicator = 'HYP001'
    GROUP BY indicator", practice_id)
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to calculate mean hypertension rate for all practices in Wales
fetch_mean_hypertension_rate_wales <- function(con) {
  query <- "
    SELECT 'HYP001' AS indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator = 'HYP001'
    GROUP BY indicator"
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to calculate mean hypertension rate for similar size practice in Wales
fetch_mean_hypertension_rate_same_size <- function(con, practice_size) {
  median_threshold <- calculate_median_threshold(con)
  practice_sizes <- dbGetQuery(con, "
    SELECT practiceid,
           COUNT(*) AS total_prescriptions
    FROM gp_data_up_to_2015
    GROUP BY practiceid
  ")
  practice_sizes$size_category <- ifelse (practice_sizes$total_prescriptions > median_threshold, 'Big', 'Small')
  same_size_practices <- practice_sizes %>% 
    filter(size_category == practice_size) %>% 
    .$practiceid
  query <- sprintf("
    SELECT 'HYP001' AS indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator = 'HYP001' AND orgcode IN ('%s')
    GROUP BY indicator", paste(same_size_practices, collapse="','"))
  same_size_data <- dbGetQuery(con, query)
  return(same_size_data)
}

# Function to visualize hypertension comparison
visualize_hypertension_comparison <- function(con, selected_practice_id, practice_size) {
  # Fetch hypertension data for the selected practice
  practice_data <- fetch_hypertension_rate_specific(con, selected_practice_id)
 
  # Calculate mean hypertension rate across Wales
  wales_data <- fetch_mean_hypertension_rate_wales(con)
  
  # Fetch mean rates for similarly sized practices
  cat("\nPlotting Comparison of Hypertension Rate...\n")
  same_size_data <- fetch_mean_hypertension_rate_same_size(con, practice_size)
  
  # Check for empty data frames
  if (nrow(practice_data) == 0 || nrow(wales_data) == 0 || nrow(same_size_data) == 0) {
    cat("No data available for one or more categories. Cannot proceed with visualization.\n")
    return()
  }
  
  # Assign labels
  practice_data$Category <- "Selected Practice"
  wales_data$Category <- "Wales Average"
  same_size_data$Category <- sprintf("%s Practices Average", practice_size)
  
  # Combine the data
  combined_data <- rbind(practice_data, same_size_data, wales_data)
  combined_data$Category <- factor(combined_data$Category, levels = c("Selected Practice", sprintf("%s Practices Average", practice_size), "Wales Average"))
  
  cat("\nComparison of Hypertension Rate:\n===============================\n")
  print(combined_data)
  cat("\nDefinitions:\n===========\nHYP001: The contractor establishes and maintains a register of patients with established hypertension.\n")
  
  # Plot the data
  print(
    ggplot(combined_data, aes(x = indicator, y = percentage, fill = Category)) +
      geom_bar(stat = "identity", position = position_dodge()) +
      scale_fill_brewer(palette = "Pastel1") +
      labs(title = "Comparison of Hypertension Rate",
           subtitle = "Selected Practice vs. Same Size Practices vs. Wales Average",
           x = "Hypertension", y = "Percentage (%)",
           fill = "Comparison Group") +
      theme_minimal()
  )
}

# Function to fetch obesity data for a specific practice
fetch_obesity_rate_specific <- function(con, practice_id) {
  query <- sprintf("
    SELECT 'OB001W' AS indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE orgcode = '%s' AND indicator = 'OB001W'
    GROUP BY indicator", practice_id)
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to calculate mean obesity rate for all practices in Wales
fetch_mean_obesity_rate_wales <- function(con) {
  query <- "
    SELECT 'OB001W' AS indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator = 'OB001W'
    GROUP BY indicator"
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to calculate mean obesity rate for similar size practice in Wales
fetch_mean_obesity_rate_same_size <- function(con, practice_size) {
  median_threshold <- calculate_median_threshold(con)
  practice_sizes <- dbGetQuery(con, "
    SELECT practiceid,
           COUNT(*) AS total_prescriptions
    FROM gp_data_up_to_2015
    GROUP BY practiceid
  ")
  practice_sizes$size_category <- ifelse (practice_sizes$total_prescriptions > median_threshold, 'Big', 'Small')
  same_size_practices <- practice_sizes %>% 
    filter(size_category == practice_size) %>% 
    .$practiceid
  query <- sprintf("
    SELECT 'OB001W' AS indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator = 'OB001W' AND orgcode IN ('%s')
    GROUP BY indicator", paste(same_size_practices, collapse="','"))
  same_size_data <- dbGetQuery(con, query)
  return(same_size_data)
}

# Function to visualize obesity comparison
visualize_obesity_comparison <- function(con, selected_practice_id, practice_size) {
  # Fetch obesity data for the selected practice
  practice_data <- fetch_obesity_rate_specific(con, selected_practice_id)
  
  # Calculate mean obesity rate across Wales
  wales_data <- fetch_mean_obesity_rate_wales(con)
  
  # Fetch mean rates for similarly sized practices
  cat("\nPlotting Comparison of Obesity Rate...\n")
  same_size_data <- fetch_mean_obesity_rate_same_size(con, practice_size)
  
  # Check for empty dataframes
  if (nrow(practice_data) == 0 || nrow(wales_data) == 0 || nrow(same_size_data) == 0) {
    cat("No data available for one or more categories. Cannot proceed with visualization.\n")
    return()
  }
  
  # Assign labels if all dataframes have rows
  practice_data$Category <- "Selected Practice"
  wales_data$Category <- "Wales Average"
  same_size_data$Category <- sprintf("%s Practices Average", practice_size)
  
  # Combine data and adjust category levels for plotting
  combined_data <- rbind(practice_data, same_size_data, wales_data)
  combined_data$Category <- factor(combined_data$Category, levels = c("Selected Practice", sprintf("%s Practices Average", practice_size), "Wales Average"))
  
  cat("\nComparison of Obesity Rate:\n==========================\n")
  print(combined_data)
  cat("\nDefinitions:\n===========\nOB001W: The contractor establishes and maintains a register of patients aged 16 or over with a BMI greater than or equal to 30 in the preceding 15 months.\n")
  
  # Plot the data
  print(
    ggplot(combined_data, aes(x = indicator, y = percentage, fill = Category)) +
      geom_bar(stat = "identity", position = position_dodge()) +
      scale_fill_brewer(palette = "Pastel1") +
      labs(title = "Comparison of Obesity Rate",
           subtitle = "Selected Practice vs. Same Size Practices vs. Wales Average",
           x = "Obesity", y = "Percentage (%)",
           fill = "Comparison Group") +
      theme_minimal()
  )
}

# Function combining top 10 drugs, top 5 drug categories, and hypertension/obesity data
combined_drug_hypertension_obesity_data <- function(con, selected_practice_postcode, selected_practice_id) {
  # Fetch and display top 10 drugs
  fetch_and_display_top_drugs(con, selected_practice_postcode)
  
  # Fetch and display top 5 drug categories
  fetch_and_display_top_drug_categories(con, selected_practice_id)
  
  # Determine the size of the selected practice
  practice_size <- define_practice_size_by_median(con, selected_practice_id)
  
  # Fetch, calculate, and visualize hypertension rates
  visualize_hypertension_comparison(con, selected_practice_id, practice_size)
  
  # Fetch, calculate, and visualize obesity rates
  visualize_obesity_comparison(con, selected_practice_id, practice_size)
}

# Function for Q1.1: User selection of a GP practice
select_gp_info<- function(){
  repeat{
    # Prompt the user to enter a postcode
    user_postcode_raw <- readline(prompt = "Enter the postcode of the GP of interest: ")
    
    # Convert postcode to upper case
    user_postcode <- toupper(user_postcode_raw)
    
    # Fetch practice by postcode
    practices <- query_practices_by_postcode(user_postcode)
    
    # Check if multiple practices are found
    # If no practices are found
    if (nrow(practices) == 0) {
      
      # Fetch similar practices based on first four characters of the postcode
      similar_practices <- query_similar_practices(user_postcode)
      cat("No practices found for the provided postcode. Finding practices with a similar postcode...\n")
      # If there are 1 or more practices found: 
      if (nrow(similar_practices) > 0) {
        repeat{
          for (i in 1:nrow(similar_practices)) {
            cat(sprintf("%d. %s\n", i, similar_practices$street[i]))
          }
          selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select and press Enter: "))
          
          if (selection >= 1 && selection <= nrow(similar_practices)) {
            break
          } else {
            cat("Invalid selection. Please try again.\n \n")
          }
        }
        
        selected_practice_id <- similar_practices$practiceid[selection]
        selected_practice_name <- similar_practices$street[selection]
        cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
        
        # Fetch the postcode of the selected practice
        selected_practice_postcode <- dbGetQuery(con, sprintf("
                                                          SELECT postcode 
                                                          FROM address 
                                                          WHERE practiceid = '%s'", selected_practice_id))$postcode
        
        # Execute combined function for top 10 drugs, top 5 drug categories, hypertension & obesity data
        combined_drug_hypertension_obesity_data(con, selected_practice_postcode, selected_practice_id)
        
      } else {
        cat("No practices found with a similar postcode.\n")
      }
    } else if (nrow(practices) == 1) {
      # Fetch the postcode of the selected practice
      selected_practice_id <- practices$practiceid[1]
      selected_practice_name <- practices$street[1]
      cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      selected_practice_postcode <- dbGetQuery(con, sprintf("
                                                            SELECT postcode 
                                                            FROM address 
                                                            WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      # Execute combined function for top 10 drugs, top 5 drug categories, hypertension & obesity data
      combined_drug_hypertension_obesity_data(con, selected_practice_postcode, selected_practice_id)
      
    } else {
      cat("\nMultiple practices found for the provided postcode. Please select one from the list below:\n")
      repeat {
        for (i in 1:nrow(practices)) {
          cat(sprintf("%d: %s\n", i, practices$street[i]))
        }
        selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select: "))
        
        # Validate selection
        if (selection >= 1 && selection <= nrow(practices)) {
          break
        } else {
          cat("Invalid selection. Please try again.\n \n")
        }
      }
      
      selected_practice_id <- practices$practiceid[selection]
      selected_practice_name <- practices$street[selection]
      cat(sprintf("\nSelected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      
      # Fetch the postcode of the selected practice
      selected_practice_postcode <- dbGetQuery(con, sprintf("SELECT postcode FROM address WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      # Execute combined function for top 10 drugs, top 5 drug categories, hypertension & obesity data
      combined_drug_hypertension_obesity_data(con, selected_practice_postcode, selected_practice_id)
    }
    
    # Prompt next action
    cat("
  Please make a selection:
  =======================
  1. Select another practice
  2. Return to Main Menu
  ")

    next_action_input <- readline(prompt = "Enter the number of your selection and press Enter: ")
    next_action <- as.integer(next_action_input)
    
    # Check if conversion to integer resulted in NA values
    if (is.na(next_action)) {
      cat("Invalid selection. Returning to Main Menu.\n")
      break 
    } else if (next_action == 2) {
      break 
    } else if (next_action != 1) {
      cat("Invalid selection. Returning to Main Menu.\n")
      break
    }
  }
}

# Function to interpret correlation results
interpret_correlation <- function(cor_test_result) {
  
  # Determine strength of the correlation
  cor_strength <- abs(cor_test_result$estimate)
  cor_significance <- cor_test_result$p.value
  
  # Define thresholds for the strength of correlation
  strength <- ifelse (cor_strength < 0.1, "very weak",
                     ifelse (cor_strength < 0.3, "weak",
                            ifelse (cor_strength < 0.5, "moderate",
                                   "strong")))
  
  # Determine if the correlation is significant
  significance <- ifelse (cor_significance < 0.05, "statistically significant", "not statistically significant")
  
  # Return the interpretation
  return(paste("The correlation is", strength, "and", significance, "."))
}

# Function to print drug columns side by side
print_drug_options <- function(first_col, second_col) {
  max_length <- max(nchar(first_col), nchar(second_col))
  for (i in seq_along(first_col)) {
    # Prepare first column entry
    first_entry <- first_col[i]
    # Check if there's a corresponding second column entry
    second_entry <- ifelse (i <= length(second_col), second_col[i], "")
    # Print the line with both entries
    cat(sprintf("%-*s %s\n", max_length + 4, first_entry, second_entry))
  }
}

# Function to display end-of-operation choices and handle user selection
end_of_operation_choice <- function() {
  cat("
  Please make a selection:
  =======================
  1. Return to Main Menu
  2. Exit
  ")
  
  choice_input <- readline(prompt = "Enter the number of your selection and press Enter: ")
  choice <- as.integer(choice_input)
  
  # Handle non-integer input or out-of-range choices by defaulting to a choice that leads back to the main menu
  if (is.na(choice) || !(choice %in% c(1, 2))) {
    cat("Invalid input detected. Returning to Main Menu...\n")
    choice <- 1
  }
  
  if (choice == 2) {
    cat("Exiting program...\nThank you for using the GP Researcher program! Goodbye.\n")
    .GlobalEnv$.keep_running <- FALSE
  }
  
  return(choice)
}

# Function for Q1.2: Comparing metformin with hypertension/obesity rates
select_metformin_drug_info <- function() {
  obesity_query <- "
                    WITH metformin_prescriptions AS (
                       SELECT practiceid, SUM(items) AS total_metformin_items
                       FROM gp_data_up_to_2015
                       WHERE bnfname LIKE 'Metformin%'
                       GROUP BY practiceid
                    ),
                    obesity_rates AS (
                       SELECT orgcode, AVG(ratio) * 100 AS obesity_rate
                       FROM qof_achievement
                       WHERE indicator = 'OB001W'
                       GROUP BY orgcode
                    )
                       SELECT m.practiceid, m.total_metformin_items, o.obesity_rate
                       FROM metformin_prescriptions AS m
                       JOIN obesity_rates AS o ON m.practiceid = o.orgcode
                       ORDER BY m.total_metformin_items DESC, o.obesity_rate DESC;"
  
  hypertension_query <- "
                         WITH metformin_prescriptions AS (
                            SELECT practiceid, SUM(items) AS total_metformin_items
                            FROM gp_data_up_to_2015
                            WHERE bnfname LIKE 'Metformin%'
                            GROUP BY practiceid
                         ),
                         hypertension_rates AS (
                            SELECT orgcode, AVG(ratio) * 100 AS hypertension_rate
                            FROM qof_achievement
                            WHERE indicator = 'HYP001'
                            GROUP BY orgcode
                         )
                            SELECT m.practiceid, m.total_metformin_items, h.hypertension_rate
                            FROM metformin_prescriptions AS m
                            JOIN hypertension_rates AS h ON m.practiceid = h.orgcode
                            ORDER BY m.total_metformin_items DESC, h.hypertension_rate DESC;"
  
  # Execute queries
  obesity_data <- dbGetQuery(con, obesity_query)
  hypertension_data <- dbGetQuery(con, hypertension_query)
  
  # Kendall correlation test for obesity
  kendall_test_metformin_obesity <- cor.test(obesity_data$total_metformin_items, obesity_data$obesity_rate, method = "kendall")
  
  print(kendall_test_metformin_obesity)
  
  # Kendall correlation test for hypertension
  kendall_test_metformin_hypertension <- cor.test(hypertension_data$total_metformin_items, hypertension_data$hypertension_rate, method = "kendall")
  
  print(kendall_test_metformin_hypertension)
  
  # Apply interpretation function to obesity and hypertension correlation results
  obesity_interpretation <- interpret_correlation(kendall_test_metformin_obesity)
  hypertension_interpretation <- interpret_correlation(kendall_test_metformin_hypertension)
  
  # Print the interpretations
  cat("Summary information:\n===================\n \n")
  cat("Obesity and Metformin:", obesity_interpretation, "\n \n")
  cat("Hypertension and Metformin:", hypertension_interpretation, "\n \n")
  
  # Compare the strength of the two correlations
  if (abs(kendall_test_metformin_obesity$estimate) > abs(kendall_test_metformin_hypertension$estimate)) {
    cat("The relationship between Metformin and Obesity is stronger than the relationship between Metformin and Hypertension.\n")
  } else if (abs(kendall_test_metformin_obesity$estimate) < abs(kendall_test_metformin_hypertension$estimate)) {
    cat("The relationship between Metformin and Hypertension is stronger than the relationship between Metformin and Obesity.\n")
  } else {
    cat("The relationship between Metformin and Obesity is as strong as the relationship between Metformin and Hypertension.\n")
  }
  
  # Plot for obesity data
  print(
    ggplot(obesity_data, aes(x = total_metformin_items, y = obesity_rate)) +
      geom_point() +
      geom_smooth(method = "lm", formula = y ~ poly(x, 2), color = "blue") +
      labs(title = "Scatter Plot of Metformin Prescriptions vs. Obesity Rate",
           x = "Total Metformin Items",
           y = "Obesity Rate (%)") +
      theme_minimal()
  )
  
  # Plot for hypertension data
  print(
    ggplot(hypertension_data, aes(x = total_metformin_items, y = hypertension_rate)) +
      geom_point() +
      geom_smooth(method = "lm", formula = y ~ poly(x, 2), color = "blue") +
      labs(title = "Scatter Plot of Metformin Prescriptions vs. Hypertension Rate",
           x = "Total Metformin Items",
           y = "Hypertension Rate (%)") +
      theme_minimal()
  )
  
  # Provide user with choice to exit or return to Main Menu
  user_choice <- end_of_operation_choice()
  
  if (user_choice == 1) {
    # Return to Main Menu
    return(TRUE)
  } else if (user_choice == 2) {
    # Exit the program by returning FALSE to break the main loop
    cat("Exiting program...\n")
    return(FALSE)
  } else {
    cat("Invalid choice. Returning to the Main Menu...\n")
    return(TRUE)
  }
}

# Function for Q1.3: allow the user to select a diabetic drug vs hypertension/obesity rates 
select_diabetic_drug_info <- function() {
  
  # Query to get a list of diabetic drugs
  diabetic_drugs_query <- "
                                        SELECT bnfchemical, MIN(chemicaldesc) AS chemicaldesc
                                        FROM bnf
                                        WHERE bnfsection = '601'
                                        AND chemicaldesc NOT LIKE '%/%'
                                        AND LOWER(chemicaldesc) NOT LIKE '%test%'
                                        AND LOWER(chemicaldesc) NOT LIKE '%other%'
                                        GROUP BY bnfchemical
                                        ORDER BY bnfchemical;
                                      "
  
  # Execute the query to get the list
  diabetic_drugs <- dbGetQuery(con, diabetic_drugs_query)
  
  # Add an index column to diabetic_drugs for easier reference
  diabetic_drugs$Index <- seq_len(nrow(diabetic_drugs))
  
  # Index and drug description column
  diabetic_drugs$IndexDesc <- paste(diabetic_drugs$Index, ": ", diabetic_drugs$chemicaldesc)
  
  # Define the split row
  split_row <- 29
  
  # Create two vectors for the two columns
  first_col <- diabetic_drugs$IndexDesc[1:split_row]
  second_col <- diabetic_drugs$IndexDesc[(split_row + 1):nrow(diabetic_drugs)]
  
  # Combine the two columns
  combined_cols <- list(first_col, second_col)
  
  # Display the list of drugs to the user
  cat("Please select the number of the drug from the list below and press Enter:\n========================================================================\n")
  print_drug_options(first_col, second_col)
  
  # Get the user's choice
  drug_choice <- as.integer(readline(prompt = "Enter the number of the drug you want to select and press Enter: "))
  
  # Check if the choice is valid
  if (drug_choice < 1 || drug_choice > nrow(diabetic_drugs)) {
    cat("Invalid selection. Please try again.\n")
    return(TRUE) # Return to the main menu
  }
  
  cat("Performing analysis. Please wait, this may take up to a minute...\n \n")
  
  # Get the selected drug's chemical
  selected_drug_chemical <- diabetic_drugs$bnfchemical[drug_choice]
  
  # Save the drug name for future use
  selected_drug_name <- trimws(diabetic_drugs$chemicaldesc[drug_choice])
  
  # Obesity query with the user's selected drug
  prescription_query <- sprintf("
                                WITH drug_prescriptions AS (
                                  SELECT gp.practiceid, SUM(gp.items) AS total_drug_items
                                  FROM gp_data_up_to_2015 gp
                                  INNER JOIN bnf ON SUBSTRING(gp.bnfcode, 1, 8) = SUBSTRING('%s', 1, 8)
                                  GROUP BY gp.practiceid
                                ),
                                obesity_rates AS (
                                  SELECT orgcode, AVG(ratio) * 100 AS obesity_rate
                                  FROM qof_achievement
                                  WHERE indicator = 'OB001W'
                                  GROUP BY orgcode
                                )
                                SELECT d.practiceid, d.total_drug_items, o.obesity_rate
                                FROM drug_prescriptions d
                                JOIN obesity_rates o ON d.practiceid = o.orgcode
                                ", selected_drug_chemical);
  
  # Similar query for hypertension rates, using the first 8 characters
  hypertension_query <- sprintf("
                                WITH drug_prescriptions AS (
                                  SELECT gp.practiceid, SUM(gp.items) AS total_drug_items
                                  FROM gp_data_up_to_2015 gp
                                  INNER JOIN bnf ON SUBSTRING(gp.bnfcode, 1, 8) = SUBSTRING('%s', 1, 8)
                                  GROUP BY gp.practiceid
                                ),
                                hypertension_rates AS (
                                  SELECT orgcode, AVG(ratio) * 100 AS hypertension_rate
                                  FROM qof_achievement
                                  WHERE indicator = 'HYP001'
                                  GROUP BY orgcode
                                )
                                SELECT d.practiceid, d.total_drug_items, h.hypertension_rate
                                FROM drug_prescriptions d
                                JOIN hypertension_rates h ON d.practiceid = h.orgcode
                                ", selected_drug_chemical);
  
  # Execute queries
  obesity_data <- dbGetQuery(con, prescription_query)
  hypertension_data <- dbGetQuery(con, hypertension_query)
  
  # Initialize correlation test results to NULL
  kendall_test_drug_obesity <- NULL
  kendall_test_drug_hypertension <- NULL
  
  # Kendall correlation test for obesity
  if (nrow(obesity_data) > 1 && sum(!is.na(obesity_data$total_drug_items)) > 1 && sum(!is.na(obesity_data$obesity_rate)) > 1) {
    kendall_test_drug_obesity <- cor.test(obesity_data$total_drug_items, obesity_data$obesity_rate, method = "kendall")
    print(kendall_test_drug_obesity)
  } else {
    cat("Not enough data for obesity correlation test.\n")
  }
  
  # Kendall correlation test for hypertension
  if (nrow(hypertension_data) > 1 && sum(!is.na(hypertension_data$total_drug_items)) > 1 && sum(!is.na(hypertension_data$hypertension_rate)) > 1) {
    kendall_test_drug_hypertension <- cor.test(hypertension_data$total_drug_items, hypertension_data$hypertension_rate, method = "kendall")
    print(kendall_test_drug_hypertension)
  } else {
    cat("Not enough data for hypertension correlation test.\n")
  }
  
  cat("Summary information:\n===================\n \n")
  
  # Check if correlation tests were performed and interpret results
  if (!is.null(kendall_test_drug_obesity)) {
    obesity_interpretation <- interpret_correlation(kendall_test_drug_obesity)
    cat(sprintf("Obesity and %s: %s\n \n", selected_drug_name, obesity_interpretation))
  } else {
    cat("Obesity data not sufficient for correlation test.\n \n")
  }
  
  if (!is.null(kendall_test_drug_hypertension)) {
    hypertension_interpretation <- interpret_correlation(kendall_test_drug_hypertension)
    cat(sprintf("Hypertension and %s: %s\n \n", selected_drug_name, hypertension_interpretation))
  } else {
    cat("Hypertension data not sufficient for correlation test.\n \n")
  }
  
  # Only compare correlations if both tests were performed
  if (!is.null(kendall_test_drug_obesity) && !is.null(kendall_test_drug_hypertension)) {
    # Compare the two correlations and print a statement about which is stronger
    if (abs(kendall_test_drug_obesity$estimate) > abs(kendall_test_drug_hypertension$estimate)) {
      cat(sprintf("The relationship between %s and Obesity is stronger than the relationship between %s and Hypertension.\n", selected_drug_name, selected_drug_name))
    } else if (abs(kendall_test_drug_obesity$estimate) < abs(kendall_test_drug_hypertension$estimate)) {
      cat(sprintf("The relationship between %s and Hypertension is stronger than the relationship between %s and Obesity.\n", selected_drug_name, selected_drug_name))
    } else {
      cat(sprintf("The relationship between %s and Obesity is as strong as the relationship between %s and Hypertension.\n", selected_drug_name, selected_drug_name))
    }
  } else {
    cat("Insufficient data for comparing correlations.\n")
  }
  
  # Plot for obesity data
  print(
    ggplot(obesity_data, aes(x = total_drug_items, y = obesity_rate)) +
      geom_point() +
      geom_smooth(method = "lm", formula = y ~ poly(x, 2), color = "blue") +
      labs(title = sprintf("Scatter Plot of %s Prescriptions vs. Obesity Rate", selected_drug_name),
           x = sprintf("Total %s Items", selected_drug_name),
           y = "Obesity Rate (%)") +
      theme_minimal()
  )
  
  # Plot for hypertension data
  print(
    ggplot(hypertension_data, aes(x = total_drug_items, y = hypertension_rate)) +
      geom_point() +
      geom_smooth(method = "lm", formula = y ~ poly(x, 2), color = "blue") +
      labs(title = sprintf("Scatter Plot of %s Prescriptions vs. Hypertension Rate", selected_drug_name),
           x = sprintf("Total %s Items", selected_drug_name),
           y = "Hypertension Rate (%)") +
      theme_minimal()
  )
  
  # Provide user with choice to exit or return to Main Menu
  user_choice <- end_of_operation_choice()
  
  # Handle the user's choice
  if (user_choice == 1) {
    # Return to main menu by breaking out of the current function and returning to the main loop
    return(TRUE)
  } else if (user_choice == 2) {
    # Exit the program by returning FALSE to break the main loop
    cat("Exiting program...\n")
    return(FALSE)
  } else {
    cat("Invalid choice. Returning to Main Menu...\n")
    return(TRUE)
  }
  
}


#### FUNCTIONS FOR Q.2 ####

# Function to fetch chd data for a specific practice
fetch_chd_rate_specific <- function(con, practice_id) {
  query <- sprintf("
    SELECT centile
    FROM qof_achievement
    WHERE orgcode = '%s' AND indicator = 'CHD001'
    GROUP BY indicator", practice_id)
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to standardize county
standardize_county <- function(county, posttown) {
  
  # NOTE: While this method of standardizing by county is not the most elegant, the 'posttown' column often provided a better indicator of which of the 22 counties a practice was in. I had tried to standardize using 'postcode', however there was so much overlap between multiple counties and the postcode prefix that it seemed to make things more complicated and I could not get it to work. Therefore assigning specific strings in the posttown and county columns to one of the 22 counties was the most successful method I could come up with.
  
  # Convert posttown to county to handle those with inconsistent 'county' columns
  posttown_to_county <- c(
    "56 - 58 HIGH STREET" = "Rhondda Cynon Taf",
    "ABERCARN" = "Caerphilly", 
    "ABERCYNON" = "Rhondda Cynon Taf", 
    "ABERDARE" = "Rhondda Cynon Taf",
    "ABERFAN" = "Merthyr Tydfil",
    "ABERGAVENNY" = "Monmouthshire",
    "ABERGELE" = "Conwy",
    "ABERTARE" = "Rhondda Cynon Taf", 
    "ABERTILLERY" = "Blaenau Gwent",
    "ABERYSTWYTH" = "Ceredigion",
    "ALLTAMI ROAD" = "Flintshire",
    "ANGLESEY" = "Isle of Anglesey",
    "BALA" = "Gwynedd",
    "BANGOR" = "Gwynedd",
    "BARGOED" = "Monmouthshire", 
    "BARMOUTH" = "Gwynedd",
    "BARRY" = "Vale of Glamorgan",
    "BENNLECH" = "Isle of Anglesey",
    "BETHESDA" = "Gwynedd",
    "BETWS Y COED" = "Conwy",
    "BISHOPS WALK" = "Denbighshire",
    "BLACKWOOD" = "Caerphilly",
    "BLAENAU FFESTINIOG" = "Gwynedd",
    "BLAENAVON" = "Torfaen",
    "BLAINA" = "Blaenau Gwent",
    "BORTH" = "Ceredigion",
    "BRECON" = "Powys",
    "BRIDGEND" = "Bridgend", 
    "BRITON FERRY" = "Neath Port Talbot",
    "BROAD SHROAD COWBRIDGE" = "Vale of Glamorgan",
    "BRUNEL WAY" = "Neath Port Talbot",
    "BRYNHYFRYD" = "Swansea",
    "BRYNMAWR" = "Blaenau Gwent",
    "BUILTH WELLS" = "Powys",
    "BURRY PORT" = "Carmarthenshire",
    "CAERGWRLE WREXHAM" = "Flintshire",
    "CAERLEON" = "Newport",
    "CAERNARFON" = "Gwynedd",
    "CAERPHILLY" = "Caerphilly", 
    "CALDICOT" = "Monmouthshire",
    "CARDIFF" = "Cardiff", 
    "CARMARTHEN" = "Carmarthenshire",
    "CEREDIGION" = "Ceredigion",
    "CHEPSTOW" = "Monmouthshire",
    "CHESTER" = "Flintshire",
    "CHURCH VILLAGE" = "Rhondda Cynon Taf",
    "CLYDACH" = "Swansea",
    "COEDPOETH" = "Wrexham",
    "COLWYN" = "Denbighshire",
    "CONNAHS QUAY" = "Flintshire",
    "CONWAY" = "Conwy",
    "CONWY" = "Conwy",
    "CORWEN" = "Denbighshire",
    "COTTRELL STREET" = "Merthyr Tydfil",
    "COWBRIDGE ROAD" = "Cardiff",
    "COWBRIDGE" = "Vale of Glamorgan",
    "CRUMLIN" = "Caerphilly",
    "CRYMYCH" = "Pembrokeshire",
    "CWMBRAN" = "Torfaen",
    "CWMLLYNFELL" = "Neath Port Talbot",
    "DEESIDE" = "Flintshire",
    "DENBIGH" = "Denbighshire",
    "DINAS POWYS" = "Vale of Glamorgan",
    "DOLGELLAU" = "Gwynedd",
    "EBBW" = "Blaenau Gwent",
    "FFORESTFACH" = "Swansea",
    "FLINTSHIRE" = "Flintshire",
    "GABALFA" = "Cardiff",
    "GAERWEN" = "Isle of Anglesey",
    "GELLIGAER" = "Caerphilly",
    "GLANRAFON" = "Flintshire",
    "GOODWICK" = "Pembrokeshire",
    "GRANGETOWN" = "Cardiff",
    "GURWEN" = "Neath Port Talbot",
    "GWENT" = "Blaenau Gwent",
    "GWYNEDD" = "Gwynedd",
    "GYFFIN" = "Conwy",
    "HAVERFORDWEST" = "Pembrokeshire",
    "HAWARDEN" = "Flintshire",
    "HIGHTOWN" = "Wrexham",
    "HOLYHEAD" = "Isle of Anglesey",
    "HOLYWELL" = "Flintshire",
    "HOPE WREXHAM" = "Flintshire",
    "KINMEL BAY RHYL" = "Denbighshire",
    "KNIGHTON" = "Powys",
    "LAMPETER" = "Ceredigion",
    "LANGDON" = "Swansea",
    "LLANBERIS" = "Gwynedd",
    "LLANDEILO" = "Carmarthenshire",
    "LLANDRINDOD WELLS" = "Powys",
    "LLANDUDNO" = "Conwy",
    "LLANELLI" = "Carmarthenshire",
    "LLANFAIRFECHAN" = "Conwy",
    "LLANFYLLIN" = "Powys",
    "LLANGOLLEN" = "Denbighshire",
    "LLANHILLETH" = "Blaenau Gwent",
    "LLANIDLOES" = "Powys",
    "LLANRWST" = "Conwy",
    "LLANSAMLET" = "Swansea",
    "LLANTRISANT" = "Rhondda Cynon Taf", 
    "LLANTWIT" = "Vale of Glamorgan",
    "LLWYNHENDY" = "Carmarthenshire",
    "MACHYNLLETH" = "Powys",
    "MAESTEG" = "Bridgend",
    "MANCHESTER SQUARE" = "Pembrokeshire",
    "MANSELTON" = "Swansea",
    "MERTHYR TYDFIL" = "Merthyr Tydfil",
    "MID GLAMORGAN" = "Rhondda Cynon Taf",
    "MILFORD" = "Pembrokeshire",
    "MIN Y NANT" = "Powys",
    "MOLD" = "Flintshire",
    "MONMOUTH" = "Monmouthshire",
    "MONTGOMERY" = "Powys",
    "MORRISTON" = "Swansea",
    "MOUNTAIN ASH" = "Rhondda Cynon Taf", 
    "NEATH" = "Neath Port Talbot",
    "NEFYN" = "Gwynedd",
    "NELSON" = "Caerphilly", 
    "NEW TREDEGAR" = "Caerphilly",
    "NEWBRIDGE" = "Caerphilly",
    "NEWPORT" = "Newport",
    "NEWTOWN" = "Powys",
    "NEYLAND" = "Pembrokeshire",
    "OLD COLWYN" = "Conwy",
    "OVERTON ON DEE" = "Wrexham",
    "PEMBROKE" ="Pembrokeshire",
    "PENCLAWDD" = "Swansea",
    "PENCOED" = "Bridgend",
    "PENGAM GREEN" = "Cardiff",
    "PENRHYNDEUDRAETH" = "Gwynedd",
    "PENYGRAIG PORTH" = "Rhondda Cynon Taf", 
    "PENYGROES" = "Gwynedd",
    "PLAS IONA" = "Cardiff",
    "PONTYCLUN" = "Rhondda Cynon Taf", 
    "PONTYPOOL" = "Torfaen",
    "PONTYPRIDD" = "Rhondda Cynon Taf",
    "PORT TALBOT" = "Neath Port Talbot",
    "PORTHMADOG" = "Gwynedd",
    "POWYS" = "Powys",
    "PRESTATYN" = "Denbighshire",
    "PRESTEIGNE" = "Powys",
    "PWLLHELI" = "Gwynedd",
    "QUEENSFERRY" = "Flintshire",
    "RAGLAN" = "Monmouthshire",
    "RHAYADER" = "Powys",
    "RHAYADER" = "Powys",
    "RHONDDA" = "Rhondda Cynon Taf", 
    "RHUDDLAN" = "Denbighshire",
    "RHYL" = "Denbighshire",
    "RHYMNEY" = "Caerphilly",
    "RISCA" = "Caerphilly",
    "RUMNEY" = "Cardiff",
    "RUTHIN" = "Denbighshire",
    "SAINT THOMAS GREEN" = "Pembrokeshire",
    "SCHOOL ROAD" = "Wrexham",
    "SCURLAGE" = "Swansea",
    "SEVEN SISTERS" = "Neath Port Talbot",
    "SINGLETON" = "Swansea",
    "SPLOTT" = "Cardiff",
    "ST ASAPH" = "Denbighshire",
    "SULLY" = "Vale of Glamorgan",
    "SWANSEA" = "Swansea",
    "TAFFS WELL" = "Rhondda Cynon Taf",
    "TALIESYN COURT" = "Ceredigion",
    "TENBY" = "Pembrokeshire",
    "THE OLD POLICE STATION TINTERN" = "Monmouthshire",
    "THOMAS STREET" = "Carmarthenshire",
    "TONYFELIN" = "Caerphilly",
    "TONYPANDY" = "Rhondda Cynon Taf", 
    "TONYREFAIL" = "Rhondda Cynon Taf", 
    "TORFAEN" = "Torfaen",
    "TREDEGAR" = "Blaenau Gwent",
    "TREHARRIS" = "Merthyr Tydfil", 
    "TROEDYRHIW" = "Merthyr Tydfil",
    "TYNEWYDD" = "Rhondda Cynon Taf", 
    "TYWYN" = "Gwynedd",
    "UNIT 22 LAWN INDUSTRIAL ESTATE" = "Caerphilly",
    "UPLANDS" = "Swansea",
    "USK" = "Monmouthshire",
    "VALE OF GLAMORGAN" = "Vale of Glamorgan",
    "VALE" = "Glamorgan", 
    "WELSHPOOL" = "Powys",
    "WESTERN VALLEY RD ROGERSTONE" = "Newport",
    "WHITE ROSE WAY" = "Caerphilly",
    "WREXHAM" = "Wrexham",
    "Y FELINHELI" = "Gwynedd",
    "YNYS MON" = "Isle of Anglesey",
    "YSTRAD MYNACH" = "Caerphilly",
    "YSTRADGYNLAIS" = "Powys",
    "YYNYSYBWL" = "Rhondda Cynon Taf"
    
  )
  
  # Convert posttown to lowercase for matching
  posttown <- tolower(posttown)
  
  # Match posttown to the county
  corrected_county <- county
  for (town in names(posttown_to_county)) {
    if (grepl(town, posttown, ignore.case = TRUE)) {
      corrected_county <- posttown_to_county[town]
      break
    }
  }
  
  # If Glamorgan is found and it's not Vale of Glamorgan, then use corrected county
  if (grepl("glamorgan", county, ignore.case = TRUE) && !grepl("vale", county, ignore.case = TRUE)) {
    return(corrected_county)
  }
  
  # Handle other cases
  return(case_when(
    grepl("CWMBRAN", county, ignore.case = TRUE) ~ "Torfaen",
    grepl("TORFAEN", county, ignore.case = TRUE) ~ "Torfaen",
    grepl("PONTYPOOL", county, ignore.case = TRUE) ~ "Torfaen",
    grepl("YSTRAD MYNACH", county, ignore.case = TRUE) ~ "Caerphilly",
    grepl("ABERTILLERY", county, ignore.case = TRUE) ~ "Blaenau Gwent",
    grepl("YNYS MON", county, ignore.case = TRUE) ~ "Isle of Anglesey",
    grepl("PEMBROKESHIRE", county, ignore.case = TRUE) ~ "Pembrokeshire",
    grepl("CONWY", county, ignore.case = TRUE) ~ "Conwy",
    grepl("CONWAY", county, ignore.case = TRUE) ~ "Conwy",
    grepl("MERTHYR TYDFIL", county, ignore.case = TRUE) ~ "Merthyr Tydfil",
    grepl("POWYS", county, ignore.case = TRUE) ~ "Powys",
    grepl("FLINTSHIRE", county, ignore.case = TRUE) ~ "Flintshire",
    grepl("MOLD", county, ignore.case = TRUE) ~ "Flintshire",
    grepl("NEW TREDEGAR", county, ignore.case = TRUE) ~ "Caerphilly",
    grepl("TREDEGAR", county, ignore.case = TRUE) ~ "Blaenau Gwent",
    grepl("CARDIFF", county, ignore.case = TRUE) ~ "Cardiff",
    grepl("CHURCH VILLAGE", county, ignore.case = TRUE) ~ "Rhondda Cynon Taf",
    grepl("PORT TALBOT", county, ignore.case = TRUE) ~ "Neath Port Talbot",
    grepl("DENBIGH", county, ignore.case = TRUE) ~ "Denbighshire",
    grepl("BORTH", county, ignore.case = TRUE) ~ "Ceredigion",
    grepl("Ceredigion", county, ignore.case = TRUE) ~ "Ceredigion",
    grepl("SWANSEA", county, ignore.case = TRUE) ~ "Swansea",
    grepl("PRESTATYN", county, ignore.case = TRUE) ~ "Flintshire",
    grepl("LLANELLI", county, ignore.case = TRUE) ~ "Carmarthenshire",
    grepl("MAESTEG", county, ignore.case = TRUE) ~ "Bridgend",
    grepl("HAVERFORDWEST", county, ignore.case = TRUE) ~ "Pembrokeshire",
    grepl("LLANGOLLEN", county, ignore.case = TRUE) ~ "Denbighshire",
    grepl("COLWYN", county, ignore.case = TRUE) ~ "Denbighshire",
    grepl("CARMARTHEN", county, ignore.case = TRUE) ~ "Carmarthenshire",
    grepl("RHYL", county, ignore.case = TRUE) ~ "Denbighshire",
    grepl("PEMBROKE", county, ignore.case = TRUE) ~ "Pembrokeshire",
    grepl("GWYNEDD", county, ignore.case = TRUE) ~ "Gwynedd",
    grepl("ABERGELE", county, ignore.case = TRUE) ~ "Conwy",
    grepl("BRITON FERRY", county, ignore.case = TRUE) ~ "Neath Port Talbot",
    grepl("PONTYPRIDD", county, ignore.case = TRUE) ~ "Rhondda Cynon Taf",
    grepl("PENCOED", county, ignore.case = TRUE) ~ "Bridgend",
    grepl("MONMOUTHSHIRE", county, ignore.case = TRUE) ~ "Monmouthshire",
    grepl("NEATH", county, ignore.case = TRUE) ~ "Neath Port Talbot",
    grepl("BLACKWOOD", county, ignore.case = TRUE) ~ "Caerphilly",
    grepl("CAERLEON", county, ignore.case = TRUE) ~ "Newport",
    grepl("NEWPORT", county, ignore.case = TRUE) ~ "Newport",
    grepl("LAMPETER", county, ignore.case = TRUE) ~ "Ceredigion",
    grepl("CRYMYCH", county, ignore.case = TRUE) ~ "Pembrokeshire",
    grepl("ABERFAN", county, ignore.case = TRUE) ~ "Merthyr Tydfil",
    grepl("WREXHAM", county, ignore.case = TRUE) ~ "Wrexham",
    grepl("HOLYHEAD", county, ignore.case = TRUE) ~ "Isle of Anglesey",
    grepl("CHEPSTOW", county, ignore.case = TRUE) ~ "Monmouthshire",
    grepl("RHYMNEY", county, ignore.case = TRUE) ~ "Caerphilly",
    grepl("ANGLESEY", county, ignore.case = TRUE) ~ "Isle of Anglesey",
    grepl("NEWBRIDGE", county, ignore.case = TRUE) ~ "Caerphilly",
    grepl("DEESIDE", county, ignore.case = TRUE) ~ "Flintshire",
    grepl("BRYNMAWR", county, ignore.case = TRUE) ~ "Blaenau Gwent",
    grepl("Rhondda Cynon Taff", county, ignore.case = TRUE) ~ "Rhondda Cynon Taf",
    grepl("MILFORD", county, ignore.case = TRUE) ~ "Pembrokeshire",
    grepl("VALE OF GLAMORGAN", county, ignore.case = TRUE) ~ "Vale of Glamorgan",
    grepl("BARRY", county, ignore.case = TRUE) ~ "Vale of Glamorgan",
    grepl("SULLY", county, ignore.case = TRUE) ~ "Vale of Glamorgan",
    grepl("FERNDALE", county, ignore.case = TRUE) ~ "Rhondda Cynon Taf",
    
    TRUE ~ as.character(county)
  ))
}

# Function to assign practice to county
assign_county <- function(postcode, county, posttown) {
  
  # Welsh county dataframe
  welsh_county_code <- c("W06000001", "W06000019", "W06000013", "W06000018", "W06000015", "W06000010", "W06000008", "W06000003", "W06000004", "W06000005", "W06000014", "W06000002", "W06000024", "W06000021", "W06000012", "W06000022", "W06000009", "W06000023", "W06000016", "W06000011", "W06000020", "W06000006")
  welsh_county <- c("Isle of Anglesey", "Blaenau Gwent", "Bridgend", "Caerphilly", "Cardiff", "Carmarthenshire", "Ceredigion", "Conwy", "Denbighshire", "Flintshire", "Vale of Glamorgan", "Gwynedd", "Merthyr Tydfil", "Monmouthshire", "Neath Port Talbot", "Newport", "Pembrokeshire", "Powys", "Rhondda Cynon Taf", "Swansea", "Torfaen", "Wrexham")
  welsh_postcode <- c("LL58|LL59|LL60|LL61|LL62|LL64|LL65|LL66|LL67|LL68|LL69|LL70|LL71|LL72|LL73|LL74|LL75|LL76|LL77|LL78", "NP2|NP3|NP23", "CF31|CF32|CF33|CF34|CF35|CF36", "CF46|CF81|CF82|CF83|NP11", "CF3|CF5|CF83", "SA4|SA14|SA15|SA16|SA17|SA18|SA19|SA20|SA31|SA32|SA33|SA34|SA38|SA39|SA40|SA44|SA48|SA66", "SA38|SA40|SA43|SA44|SA45|SA46|SA47|SA48|SY20|SY23|SY24|SY25", "LL16|LL21|LL22|LL24|LL25|LL26|LL27|LL28|LL29|LL30|LL31|LL32|LL33|LL34|LL57", "CH7|LL11|LL15|LL16|LL17|LL18|LL19|LL20|LL21|LL22", "CH1|CH4|CH5|CH6|CH7|CH8|LL11|LL12|LL18|LL19", "CF1|CF5|CF32|CF35|CF61|CF62|CF63|CF64|CF71", "LL21|LL23|LL33|LL35|LL36|LL37|LL38|LL39|LL40|LL41|LL42|LL43|LL44|LL45|LL46|LL47|LL48|LL49|LL51|LL52|LL53|LL54|LL55|LL57|SY20", "CF46|CF47|CF48", "NP4|NP6|NP7", "SA8|SA9|SA10|SA11|SA12|SA13|SA18", "CF3|NP1|NP2|NP3|NP10|NP19|NP20", "SA34|SA35|SA36|SA37|SA41|SA42|SA43|SA61|SA62|SA63|SA64|SA65|SA66|SA67|SA68|SA69|SA70|SA71|SA72|SA73", "CF44|CF48|HR3|HR5|LD1|LD2|LD3|LD4|LD5|LD6|LD7|LD8|NP7|NP8|SA9|SA10|SY5|SY10|SY15|SY16|SY17|SY18|SY19|SY20|SY21|SY22", "CF37|CF38|CF39|CF40|CF41|CF42|CF43|CF44|CF45|CF72", "SA1|SA2|SA3|SA4|SA5|SA6|SA7|SA18", "NP4|NP44", "LL11|LL12|LL13|LL14|LL20|SY13|SY14")
  welsh_county_df <- data.frame(welsh_county_code, welsh_county, welsh_postcode, stringsAsFactors = FALSE)
  
  # Apply the standardized county function
  standardized_county <- standardize_county(county, posttown)

  # If county was successfully standardized or for special cases
  if (!is.na(standardized_county) && standardized_county != county) {
    return(standardized_county)
  }
  
  # For preserved counties, NULL values, or when standardized county is the same as input
  postcode_prefix <- substr(postcode, 1, min(nchar(postcode), 4))
  for (i in 1:nrow(welsh_county_df)) {
    if (grepl(postcode_prefix, welsh_county_df$welsh_postcode[i])) {
      return(welsh_county_df$welsh_county[i])
    }
  }
  
  return(NA)
}

# Function to retrieve county performance for CHD
retrieve_county_performance_chd <- function() {
  
  # County and centile query
  query <- "
            SELECT ad.county, ad.postcode, ad.posttown, qa.centile
            FROM address AS ad
            JOIN qof_achievement AS qa ON ad.practiceid = qa.orgcode
            WHERE qa.indicator = 'CHD001'
            "
  
  # Execute query
  county_centile_data <- dbGetQuery(con, query)
  
  # Standardize county names and apply assign_county function
  county_centile_data$county <- mapply(assign_county, county_centile_data$postcode, county_centile_data$county, county_centile_data$posttown)
  
  # Aggregate centile scores by county
  county_centile_data <- county_centile_data %>%
    group_by(county) %>%
    summarize(average_centile = mean(centile, na.rm = TRUE)) %>%
    ungroup()
  
  # Read the Welsh county shapefile
  welsh_shapefile_path <- "LAD_MAY_2021_UK_BFC.shp"
  welsh_counties <- st_read(welsh_shapefile_path, quiet = TRUE)
  
  # Welsh county codes
  welsh_county_code <- c("W06000001", "W06000019", "W06000013", "W06000018", "W06000015", 
                         "W06000010", "W06000008", "W06000003", "W06000004", "W06000005", 
                         "W06000014", "W06000002", "W06000024", "W06000021", "W06000012", 
                         "W06000022", "W06000009", "W06000023", "W06000016", "W06000011", 
                         "W06000020", "W06000006")
  
  # Visualize only Welsh counties on the map
  welsh_counties <- welsh_counties %>%
    filter(LAD21CD %in% welsh_county_code)
  
  # Combine query data with shapefile data
  combined_data <- welsh_counties %>%
    left_join(county_centile_data, by = c("LAD21NM" = "county")) %>%
    mutate(is_missing = is.na(average_centile))
  
  return(list(combined_data = combined_data, county_centile_data = county_centile_data))
}

# Function to plot county performance for CHD
plot_county_performance_chd <- function(combined_data, county_centile_data) {
  
  # Plot county centile data
  plot1 <- ggplot(combined_data) +
    geom_sf(aes(fill = average_centile), color = "white", size = 0.2) +
    scale_fill_viridis_c(option = "C", direction = -1, na.value = "grey", end = 0.9, name = "Avg Centile Score") +
    labs(title = "Average CHD Performance by County in Wales") +
    theme_void() + 
    theme(legend.position = "bottom",
          plot.title = element_text(hjust = 0.5),
          legend.key.width = unit(2, 'cm')) 
  
  print(plot1)
  
  # Arrange centile scores by descending order
  county_centile_data <- county_centile_data %>%
    filter(!is.na(county)) %>%
    arrange(desc(average_centile)) 
  
  # Convert to percentages
  county_centile_data$average_centile <- percent(county_centile_data$average_centile)
  
  # Print centile scores
  cat("Summary information:\n===================\n \n")
  print(county_centile_data, n = Inf)
}

# Fetch centile for CHD of selected practice
user_chd_performance_query <- function(selected_practice_id) {
  query <- sprintf("
                   SELECT q.centile
                   FROM qof_achievement q
                   INNER JOIN address a ON q.orgcode = a.practiceid
                   WHERE q.indicator = 'CHD001'
                   AND a.practiceid = '%s'", selected_practice_id)
  
  user_chd_performance <- dbGetQuery(con, query)
  
  # Check if the result is empty
  if (nrow(user_chd_performance) == 0) {
    # Set centile to NA if no data is found
    user_chd_performance <- data.frame(centile = NA)
  }
  
  return(user_chd_performance)
}

# Function to retrieve postcode, county, and posttown for a given practice ID
fetch_practice_location_info <- function(selected_practice_id) {
  query <- sprintf("
               SELECT postcode, county, posttown
               FROM address
               WHERE practiceid = '%s'", selected_practice_id)
  
  location_info <- dbGetQuery(con, query)
  
  return(location_info)
}

# Function for Q2.1
select_gp_info_chd <- function(){
  repeat{
    # Prompt the user to enter a postcode
    user_postcode_raw <- readline(prompt = "Enter the postcode of the GP of interest: ")
    
    # Convert postcode to upper case
    user_postcode <- toupper(user_postcode_raw)
    
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
        repeat{
          for (i in 1:nrow(similar_practices)) {
            cat(sprintf("%d. %s\n", i, similar_practices$street[i]))
          }
          selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select and press Enter: "))
          
          if (selection >= 1 && selection <= nrow(similar_practices)) {
            break
          } else {
            cat("Invalid selection. Please try again.\n \n")
          }
        }
        
        selected_practice_id <- similar_practices$practiceid[selection]
        selected_practice_name <- similar_practices$street[selection]
        cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
        
      
        # Initialize variables with NA to ensure they exist
        selected_county_avg_chd <- NA
        practice_chd_percentage <- NA
        county_avg_chd_percentage <- NA
        
        # Fetch centile for CHD of selected practice
        user_chd_performance <- user_chd_performance_query(selected_practice_id)
        
        # Check if the result is empty
        if (nrow(user_chd_performance) == 0 || is.na(user_chd_performance$centile[1])) {
          user_chd_performance <- data.frame(centile = NA)
        } else {
          # Convert to percentage
          practice_chd_percentage <- user_chd_performance$centile[1] * 100
        }
        
        # Fetch postcode, county and posttown information for the selected practice ID
        location_info <- fetch_practice_location_info(selected_practice_id)

        # Assign practiceID of selected practice to county using assign_county function
        if (nrow(location_info) > 0) {
          standardized_county <- standardize_county(location_info$county[1], location_info$posttown[1])
          true_county_name <- assign_county(location_info$postcode[1], standardized_county, location_info$posttown[1])

          # Fetch average centile of that selected practice's county
          county_performance_data <- retrieve_county_performance_chd()
          if (!is.null(county_performance_data) && nrow(county_performance_data$county_centile_data) > 0 && !is.na(true_county_name)) {
            selected_county_avg_chd <- county_performance_data$county_centile_data %>%
              filter(county == true_county_name) %>%
              .$average_centile
            if (!is.na(selected_county_avg_chd)) {
              county_avg_chd_percentage <- selected_county_avg_chd * 100
            }
          }
          
          
          # Check to ensure CHD performance data is not missing
          if (!is.na(user_chd_performance$centile) && !is.na(selected_county_avg_chd)) {

            cat("\nSummary information:\n===================\n")
            cat(sprintf("CHD performance for selected practice: %.2f%%\n", practice_chd_percentage))
            cat(sprintf("Average CHD performance for %s: %.2f%%\n", true_county_name, county_avg_chd_percentage))
            
            # Define and plot data
            data_to_plot <- data.frame(
              Entity = c(selected_practice_name, paste(true_county_name)),
              CHD_Percentage = c(practice_chd_percentage, county_avg_chd_percentage)
            )
            data_to_plot$Entity <- factor(data_to_plot$Entity, levels = data_to_plot$Entity)
            
            print(
              ggplot(data_to_plot, aes(x = Entity, y = CHD_Percentage, fill = Entity)) +
                geom_bar(stat = "identity", width = 0.5) +
                scale_fill_brewer(palette = "Pastel1") +
                labs(title = paste("CHD Performance:", selected_practice_name, "vs.", true_county_name),
                     y = "CHD Performance (%)",
                     x = "") +
                theme_minimal() +
                geom_text(aes(label = sprintf("%.2f%%", CHD_Percentage)), vjust = -0.5)
            )
          } else {
            cat("Missing CHD performance data for selected practice or county average.\n")
          }
        } else {
          print("No location information found for the selected practice.")
        }
        
      } else {
        cat("No practices found with a similar postcode.\n")
      }
    } else if (nrow(practices) == 1) {
      # Fetch the postcode of the selected practice
      selected_practice_id <- practices$practiceid[1]
      selected_practice_name <- practices$street[1]
      cat(sprintf("Selected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      selected_practice_postcode <- dbGetQuery(con, sprintf("
                                                            SELECT postcode 
                                                            FROM address 
                                                            WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      
      
      # Initialize variables with NA to ensure they exist
      selected_county_avg_chd <- NA
      practice_chd_percentage <- NA
      county_avg_chd_percentage <- NA
      
      # Fetch centile for CHD of selected practice
      user_chd_performance <- user_chd_performance_query(selected_practice_id)
      
      # Check if the result is empty
      if (nrow(user_chd_performance) == 0 || is.na(user_chd_performance$centile[1])) {
        user_chd_performance <- data.frame(centile = NA)
      } else {
        # Convert to percentage
        practice_chd_percentage <- user_chd_performance$centile[1] * 100
      }
      
      # Fetch postcode, county and posttown information for the selected practice ID
      location_info <- fetch_practice_location_info(selected_practice_id)
      
      # Assign practiceID of selected practice to county using assign_county function
      if (nrow(location_info) > 0) {
        standardized_county <- standardize_county(location_info$county[1], location_info$posttown[1])
        true_county_name <- assign_county(location_info$postcode[1], standardized_county, location_info$posttown[1])

        # Fetch avg centile of that selected practice's county
        county_performance_data <- retrieve_county_performance_chd()
        if (!is.null(county_performance_data) && nrow(county_performance_data$county_centile_data) > 0 && !is.na(true_county_name)) {
          selected_county_avg_chd <- county_performance_data$county_centile_data %>%
            filter(county == true_county_name) %>%
            .$average_centile
          if (!is.na(selected_county_avg_chd)) {
            county_avg_chd_percentage <- selected_county_avg_chd * 100
          }
        }
        
        # Check if the CHD performance data is not missing
        if (!is.na(user_chd_performance$centile) && !is.na(selected_county_avg_chd)) {
          
          cat("\nSummary information:\n===================\n")
          cat(sprintf("CHD performance for selected practice: %.2f%%\n", practice_chd_percentage))
          cat(sprintf("Average CHD performance for %s: %.2f%%\n", true_county_name, county_avg_chd_percentage))
          
          # Define and plot data
          data_to_plot <- data.frame(
            Entity = c(selected_practice_name, paste(true_county_name)),
            CHD_Percentage = c(practice_chd_percentage, county_avg_chd_percentage)
          )
          data_to_plot$Entity <- factor(data_to_plot$Entity, levels = data_to_plot$Entity)
          
          print(
            ggplot(data_to_plot, aes(x = Entity, y = CHD_Percentage, fill = Entity)) +
              geom_bar(stat = "identity", width = 0.5) +
              scale_fill_brewer(palette = "Pastel1") +
              labs(title = paste("CHD Performance:", selected_practice_name, "vs.", true_county_name),
                   y = "CHD Performance (%)",
                   x = "") +
              theme_minimal() +
              geom_text(aes(label = sprintf("%.2f%%", CHD_Percentage)), vjust = -0.5)
          )
        } else {
          cat("Missing CHD performance data for selected practice or county average.\n")
        }
      } else {
        print("No location information found for the selected practice.")
      }
      
      
    } else {
      cat("\nMultiple practices found for the provided postcode. Please select one from the list below:\n")
      repeat {
        for (i in 1:nrow(practices)) {
          cat(sprintf("%d: %s\n", i, practices$street[i]))
        }
        selection <- as.integer(readline(prompt = "Enter the number of the practice you want to select: "))
        
        # Validate the selection
        if (selection >= 1 && selection <= nrow(practices)) {
          break
        } else {
          cat("Invalid selection. Please try again.\n \n")
        }
      }
      
      selected_practice_id <- practices$practiceid[selection]
      selected_practice_name <- practices$street[selection]
      cat(sprintf("\nSelected practice: %s. Please wait a few seconds...\n", selected_practice_name))
      
      # Initialize variables with NA to ensure they exist
      selected_county_avg_chd <- NA
      practice_chd_percentage <- NA
      county_avg_chd_percentage <- NA
      
      # Fetch centile for CHD of selected practice
      user_chd_performance <- user_chd_performance_query(selected_practice_id)
      
      # Check if the result is empty
      if (nrow(user_chd_performance) == 0 || is.na(user_chd_performance$centile[1])) {
        user_chd_performance <- data.frame(centile = NA)
      } else {
        # Convert to percentage
        practice_chd_percentage <- user_chd_performance$centile[1] * 100
      }
      
      # Fetch postcode, county and posttown information for the selected practice ID
      location_info <- fetch_practice_location_info(selected_practice_id)
      
      # Assign practiceID of selected practice to county using assign_county function
      if (nrow(location_info) > 0) {
        standardized_county <- standardize_county(location_info$county[1], location_info$posttown[1])
        true_county_name <- assign_county(location_info$postcode[1], standardized_county, location_info$posttown[1])

        # Fetch avg centile of that selected practice's county
        county_performance_data <- retrieve_county_performance_chd()
        if (!is.null(county_performance_data) && nrow(county_performance_data$county_centile_data) > 0 && !is.na(true_county_name)) {
          selected_county_avg_chd <- county_performance_data$county_centile_data %>%
            filter(county == true_county_name) %>%
            .$average_centile
          if (!is.na(selected_county_avg_chd)) {
            county_avg_chd_percentage <- selected_county_avg_chd * 100
          }
        }
        
        # Check if the CHD performance data is not missing
        if (!is.na(user_chd_performance$centile) && !is.na(selected_county_avg_chd)) {
          
          cat("\nSummary information:\n===================\n")
          cat(sprintf("CHD performance for selected practice: %.2f%%\n", practice_chd_percentage))
          cat(sprintf("Average CHD performance for %s: %.2f%%\n", true_county_name, county_avg_chd_percentage))
          
          # Define and plot data
          data_to_plot <- data.frame(
            Entity = c(selected_practice_name, paste(true_county_name)),
            CHD_Percentage = c(practice_chd_percentage, county_avg_chd_percentage)
          )
          data_to_plot$Entity <- factor(data_to_plot$Entity, levels = data_to_plot$Entity)
          
          print(
            ggplot(data_to_plot, aes(x = Entity, y = CHD_Percentage, fill = Entity)) +
              geom_bar(stat = "identity", width = 0.5) +
              scale_fill_brewer(palette = "Pastel1") +
              labs(title = paste("CHD Performance:", selected_practice_name, "vs.", true_county_name),
                   y = "CHD Performance (%)",
                   x = "") +
              theme_minimal() +
              geom_text(aes(label = sprintf("%.2f%%", CHD_Percentage)), vjust = -0.5)
          )
        } else {
          cat("Missing CHD performance data for selected practice or county average.\n")
        }
      } else {
        print("No location information found for the selected practice.")
      }
    }
    
    # After fetching and displaying the information, prompt for next action
    cat("
  Please make a selection:
  =======================
  1. Select another practice
  2. Return to Main Menu
  ")
    
    next_action_input <- readline(prompt = "Enter the number of your selection and press Enter: ")
    next_action <- as.integer(next_action_input)
    
    # Check if the conversion to integer resulted in NA due to invalid input
    if (is.na(next_action)) {
      cat("Invalid selection. Returning to Main Menu...\n")
      break 
    } else if (next_action == 2) {
      break 
    } else if (next_action != 1) {
      cat("Invalid selection. Returning to Main Menu...\n")
      break
    }
  }
}

# Function to interpret cluster results
interpret_clusters <- function(cluster_percentages, threshold) {
  if (any(cluster_percentages > threshold)) {
    interpretation <- cat("\nThere is a dominating cluster, indicating a specific strategy in treating CHD. This suggests little diversity in the management of this disease across practices.\n")
  } else {
    interpretation <- cat("\nThere is no dominating cluster, indicating that there is no universal strategy adopted in treating CHD. This suggests substantial diversity in the management of this disease across practices.\n")
  }
  return(interpretation)
}

# Function to interpret correlation between clusters
interpret_cluster_correlation <- function(cluster_number, correlation, variable1, variable2) {
  # Define thresholds for strong/weak correlation
  strong_threshold <- 0.7
  weak_threshold <- 0.3
  
  # Determine the strength of the correlation
  strength <- ifelse (abs(correlation) > strong_threshold, "STRONG",
                     ifelse (abs(correlation) < weak_threshold, "WEAK", "MODERATE"))
  
  # Determine the direction of the correlation
  direction <- ifelse (correlation > 0, "POSITIVE", "NEGATIVE")
  
  # Construct the sentence
  sentence <- sprintf("\nFor Cluster %d: there is a %s and %s correlation between %s and %s (%.3f).\n This suggests that within this cluster, practices that spend %s on %s tend to have %s performance.\n",
                      cluster_number, direction, strength, variable1, variable2, correlation, 
                      ifelse (correlation > 0, "more", "less"), variable1, 
                      ifelse (correlation > 0, "better", "worse"))
  return(sentence)
}

# Function for Q2 Sub-Menu: 
select_efficiency_info <- function() {
  cat("
  
Welcome to the Performance and Spend Sub-Menu:
=============================================

Coronary heart disease (CHD) is a chronic condition and one of the leading causes of death worldwide, resulting in thousands of hospitalizations in Wales per year. 

One of the most common treatments for heart disease is the use of beta blockers, a preventative measure against myocardial infarction (MI). These block cell receptors that - if bound and activated - would normally result in the release of hormones responsible for increasing heart rate and thereby potential health consequences.

Use the menu options below to explore the performance and spend data for CHD at practices across Wales.

    Performance and Spend Sub-Menu:
    ==============================
    1. Management of CHD by county
    2. Spend efficiency
    3. Identify outliers
    4. Return to Main Menu
    5. Exit
  ")
  
  choice <- as.integer(readline(prompt = "Enter the number of your selection and press Enter: "))
  
  switch(choice,
         { # Option 1. Management of CHD by county
           
           cat("
  
Management of CHD by county:
===========================

The 'performance' of CHD for a practice is a centile ranking based on the frequency distribution of CHD across all practices in Wales. As this centile has a 1:1 correlation with the rate of CHD, higher performance indicates a higher rate of CHD at that practice or county.

  ")
           cat("Performing analysis. Please wait...\n \n")
           
           # Plot choropleth map of CHD centile scores
           county_performance_data_chd <- retrieve_county_performance_chd()
           plot_county_performance_chd(county_performance_data_chd$combined_data, county_performance_data_chd$county_centile_data)
           
           # Plot user selection vs county
           # NOTE: I wanted to compare the user's selected practice CHD centile performance to its respective county CHD performance but I just could not get it to work as intended. While some practices do work (such as ST. LUKE'S SURGERY which can be searched for with the postcode: NP11 5GX), displaying the centile performance comparison as a bar chart as intended, many simply return NA values. I have implemented NA checks so that the code still runs, but having double-checked specific practices that are returning 'NA' and finding that they do have CHD centile values, I am unsure where precisely the issue is occurring. Therefore the accuracy of the analysis cannot be reliable.
           # NOTE: My aim was to use this county information to control for geographic location (as a rough proxy for demographic confounders not present in the database) to explore spend efficiency on CHD-related drugs as an alternative to just comparing to bigger/smaller practices, and provide more granularity than dividing by region (i.e. by the first two letters of the postcode) or by simply comparing to Wales as a whole.
           cat("
  
Please enter the postcode of the practice you wish to investigate.
  ")
           select_gp_info_chd()
         },
         { # Option 2. Spend efficiency
           cat("Performing analysis. Please wait...\n \n")
           
           # Query to find the top 5 beta blockers by spend
           top_beta_blockers_query <- "
                                      SELECT bnfname, SUM(actcost) AS total_spend
                                      FROM gp_data_up_to_2015
                                      WHERE bnfcode LIKE '0204000%' 
                                      GROUP BY bnfname
                                      ORDER BY total_spend DESC
                                      LIMIT 5;
                                    "
           
           # Query to find the bottom 5 beta blockers by spend
           bottom_beta_blockers_query <- "
                                          SELECT bnfname, SUM(actcost) AS total_spend
                                          FROM gp_data_up_to_2015
                                          WHERE bnfcode LIKE '0204000%' 
                                          GROUP BY bnfname
                                          ORDER BY total_spend ASC
                                          LIMIT 5;
                                        "
           
           top_beta_blockers <- dbGetQuery(con, top_beta_blockers_query)
           bottom_beta_blockers <- dbGetQuery(con, bottom_beta_blockers_query)
           
           # Print the results
           colnames(top_beta_blockers) <- c('Drug Type', 'Total Spend (£)')
           cat("\nTop 5 Beta Blockers by Spend:\n============================\n")
           print(top_beta_blockers)
           
           colnames(bottom_beta_blockers) <- c('Drug Type', 'Total Spend (£)')
           cat("\nBottom 5 Beta Blockers by Spend:\n================================\n")
           print(bottom_beta_blockers)
           
           # Query relationship between beta blocker spend and CHD performance
           cat("\nPerforming correlation analysis...\n")
           query <- "
                    SELECT 
                        gp.practiceid,
                        SUM(gp.actcost) AS total_spend_on_beta_blockers,
                        q.centile AS performance_centile
                    FROM 
                        gp_data_up_to_2015 AS gp
                    JOIN 
                        qof_achievement AS q ON gp.practiceid = q.orgcode
                    WHERE 
                        gp.bnfcode LIKE '0204000%' AND
                        q.indicator = 'CHD001'
                    GROUP BY 
                        gp.practiceid, q.centile
                    "
           
           # Execute the query
           data <- dbGetQuery(con, query)
           
           # Calculate Spearman's rank correlation for spend vs performance
           kendall_chd_spend <- cor.test(data$total_spend_on_beta_blockers, data$performance_centile, method = "kendall")
           
           # Print the results
           print(kendall_chd_spend)
           
           # Check if correlation tests were performed and interpret results
           if (!is.null(kendall_chd_spend)) {
             kendall_chd_spend_interpretation <- interpret_correlation(kendall_chd_spend)
           } else {
             cat("Spend data not sufficient for correlation test.\n \n")
           }
           
           # Summary information
           cat("Summary information:\n===================\n \n")
           cat("Spend and CHD performance centile:", kendall_chd_spend_interpretation, "\n \n")
           
           # Plot the results
           print(
             ggplot(data, aes(x = total_spend_on_beta_blockers, y = performance_centile)) +
               geom_point(alpha = 0.6) +  
               geom_smooth(method = "lm", formula = y ~ x, color = "blue", se = FALSE) +  
               labs(title = "Relationship between Beta Blockers Spend and CHD Performance",
                    x = "Total Spend on Beta Blockers (£)",
                    y = "CHD Performance Centile") +
               theme_minimal()
           )
           
         },
         { # Option 3. Identify outliers

           # Cluster analysis query
           cat("\nPerforming cluster analysis. Please wait...\n")
           query <- "
                    SELECT
                    gp.practiceid,
                    SUM(gp.actcost) AS total_spend_on_beta_blockers,
                    SUM(gp.quantity) AS total_quantity_of_chd_medication,
                    SUM(gp.items) AS number_of_chd_related_prescriptions,
                    qof.centile AS performance_centile
                FROM
                    gp_data_up_to_2015 AS gp
                JOIN
                    qof_achievement AS qof ON gp.practiceid = qof.orgcode
                WHERE
                    gp.bnfcode LIKE '0204000%'
                    AND qof.indicator = 'CHD001'
                GROUP BY
                    gp.practiceid, qof.centile"
           
           cluster_data <- dbGetQuery(con, query)
           
           # Normalize the data
           data_normalized <- as.data.frame(scale(cluster_data[,c("total_spend_on_beta_blockers", "total_quantity_of_chd_medication", "number_of_chd_related_prescriptions", "performance_centile")]))
           
           # Perform k-means clustering
           set.seed(100)
           k_means_result <- kmeans(data_normalized, centers = 3)
           
           # Attach cluster assignment to original data
           cluster_data$cluster <- k_means_result$cluster
           
           # Analyze the clusters
           cluster_data %>%
             group_by(cluster) %>%
             summarise(across(everything(), mean, na.rm = TRUE))
           
           # Scatter plot for spend vs. performance centile
           plot1 <- ggplot(cluster_data, aes(x = total_spend_on_beta_blockers, y = performance_centile, color = factor(cluster))) +
            geom_point() +
            labs(title = "Spend vs. Performance",
                x = "Total Spend on Beta-Blockers",
                y = "Performance Centile",
                color = "Cluster") +
            theme_minimal()
           
           # Scatter plot for total quantity of CHD medication vs. performance centile
           plot2 <- ggplot(cluster_data, aes(x = total_quantity_of_chd_medication, y = performance_centile, color = factor(cluster))) +
               geom_point() +
               labs(title = "Medication Quantity vs. Performance",
                    x = "Quantity of CHD medication",
                    y = "Performance Centile",
                    color = "Cluster") +
               theme_minimal()
           
           # Scatter plot for number of CHD-related prescriptions vs. performance centile
           plot3 <- ggplot(cluster_data, aes(x = number_of_chd_related_prescriptions, y = performance_centile, color = factor(cluster))) +
               geom_point() +
               labs(title = "Prescriptions vs. Performance",
                    x = "Number of CHD-related Prescriptions",
                    y = "Performance Centile",
                    color = "Cluster") +
               theme_minimal()
           
           # MDS Plot
           mds_df <- as.data.frame(cmdscale(dist(data_normalized), k = 2))
           mds_df$cluster <- cluster_data$cluster
           
           plot4 <- ggplot(mds_df, aes(x = V1, y = V2, color = factor(cluster))) +
             geom_point() +
             labs(title = "Multidimensional Scaling",
                  x = "Dimension 1",
                  y = "Dimension 2",
                  color = "Cluster") +
             theme_minimal()
           
           # Combine plots
           plot1 <- plot1 + theme(legend.position = "none")
           plot2 <- plot2 + theme(legend.position = "right") + guides(color=guide_legend("Cluster"))
           plot3 <- plot3 + theme(legend.position = "none")
           plot4 <- plot4 + theme(legend.position = "none")
           
           print((plot1 | plot2) / (plot3 | plot4))
          
           # Summarize the cluster data
           cat("\n \n")
           cluster_summary <- cluster_data %>%
             group_by(cluster) %>%
             summarise(
               mean_spend = mean(total_spend_on_beta_blockers, na.rm = TRUE),
               median_spend = median(total_spend_on_beta_blockers, na.rm = TRUE),
               mean_performance_centile = mean(performance_centile, na.rm = TRUE),
               median_performance_centile = median(performance_centile, na.rm = TRUE),
               mean_quantity_chd_medication = mean(total_quantity_of_chd_medication, na.rm = TRUE),
               median_quantity_chd_medication = median(total_quantity_of_chd_medication, na.rm = TRUE),
               mean_number_prescriptions = mean(number_of_chd_related_prescriptions, na.rm = TRUE),
               median_number_prescriptions = median(number_of_chd_related_prescriptions, na.rm = TRUE)
             )
           cat("\nCluster Summary:\n===============\n")
           colnames(cluster_summary) <- c('Cluster', 'Mean spend', 'Median spend', 'Mean performance centile', 'Median performance centile', 'Mean quantity CHD medication')
           print(cluster_summary)
           
           # Count the number of practices in each cluster and calculate the percentage of total practices
           cluster_distribution <- cluster_data %>%
             group_by(cluster) %>%
             summarise(count = n()) %>%
             mutate(percentage = (count / sum(count)) * 100)
           
           # Print the distribution
           cat("\nCluster Distribution:\n====================\n")
           colnames(cluster_distribution) <- c('Cluster', 'Total practices', '% of total practices')
           print(cluster_distribution)
           
           # Define a threshold to interpret cluster results
           dominating_cluster_threshold <- 50
           
           # Calculate correlations within each cluster
           cluster_correlations <- cluster_data %>%
             group_by(cluster) %>%
             summarise(
               spend_performance_correlation = cor(total_spend_on_beta_blockers, performance_centile, use = "complete.obs"),
               spend_quantity_correlation = cor(total_spend_on_beta_blockers, total_quantity_of_chd_medication, use = "complete.obs"),
               quantity_performance_correlation = cor(total_quantity_of_chd_medication, performance_centile, use = "complete.obs")
             )
           
           # Apply the function to each row in the cluster_correlations dataframe
           cat("\nSummary information:\n===================\n")
           interpretations <- lapply(1:nrow(cluster_correlations), function(i) {
             row <- cluster_correlations[i, ]
             cluster_number <- row$cluster
             spend_performance_corr <- interpret_cluster_correlation(cluster_number, row$spend_performance_correlation, "beta-blockers", "performance centile")
             spend_quantity_corr <- interpret_cluster_correlation(cluster_number, row$spend_quantity_correlation, "beta-blockers", "total quantity of CHD medication")
             quantity_performance_corr <- interpret_cluster_correlation(cluster_number, row$quantity_performance_correlation, "total quantity of CHD medication", "performance centile")
             c(spend_performance_corr, spend_quantity_corr, quantity_performance_corr)
           })
           
           # Print the interpretations
           cat(paste(unlist(interpretations), sep="\n", collapse="\n"))
           
           # Apply the cluster function to interpret the clusters
           interpretation <- interpret_clusters(cluster_summary$cluster_percentage, dominating_cluster_threshold)
           
           # Print the cluster interpretation
           cat("\n \n")
           print(interpretation)
          
           # Visualize outliers
           variables_to_plot <- c("total_spend_on_beta_blockers", "performance_centile", "total_quantity_of_chd_medication", "number_of_chd_related_prescriptions")
           
           for (variable in variables_to_plot) {
             ggplot(cluster_data, aes_string(x = "factor(cluster)", y = variable)) +
               geom_boxplot() +
               labs(title = paste("Box plot of", variable, "by cluster"),
                    x = "Cluster",
                    y = variable) +
               theme_minimal()
             print(last_plot())
           }
          
           # Fetch GP surgery names
           gp_data <- dbGetQuery(con, 
                                 "SELECT DISTINCT gp.practiceid, ad.street 
                                 FROM gp_data_up_to_2015 as gp
                                 JOIN address AS ad ON gp.practiceid = ad.practiceid")
           
           gp_data <- unique(gp_data)
           
           # Identify outliers
           identify_outliers <- function(data, column, gp_data) {
             q1 <- quantile(data[[column]], 0.25)
             q3 <- quantile(data[[column]], 0.75)
             iqr <- q3 - q1
             
             lower_bound <- q1 - 1.5 * iqr
             upper_bound <- q3 + 1.5 * iqr
             
             outliers <- data[data[[column]] < lower_bound | data[[column]] > upper_bound, ]
             
             # Join practice names
             outliers_with_names <- merge(outliers, gp_data, by = "practiceid", all.x = TRUE)
             
             return(outliers_with_names)
           }
           
           # Apply the identify_outliers function to each cluster and variable
           outliers_list <- lapply(variables_to_plot, function(variable) {
             lapply(unique(cluster_data$cluster), function(cluster) {
               cluster_subset <- cluster_data[cluster_data$cluster == cluster, ]
               outliers <- identify_outliers(cluster_subset, variable, gp_data)
               if (nrow(outliers) > 0) {
                 cat("\n \n")
                 cat(paste("Outliers in cluster", cluster, "for", variable, ":\n"))
                 
                 # Create temporary copy with renamed columns
                 temp_outliers <- outliers
                 colnames(temp_outliers)[colnames(temp_outliers) == 'practiceid'] <- 'Practice ID'
                 colnames(temp_outliers)[colnames(temp_outliers) == 'street'] <- 'GP Surgery'
                 
                 print(temp_outliers[, c("Practice ID",'GP Surgery', variable)])
               }
             })
           })
           
           # Provide user with choice to exit or return to Main Menu
           user_choice <- end_of_operation_choice()
           
           # Handle the user's choice
           if (user_choice == 1) {
             # Return to Main Menu
             return(TRUE)
           } else if (user_choice == 2) {
             # Exit the program by returning FALSE, which will break the main loop
             cat("Exiting program...\n")
             return(FALSE)
           } else {
             cat("Invalid choice. Returning to the Main Menu...\n")
             return(TRUE)
           }
         },
         { # Return to Main Menu
           cat("Returning to Main Menu...\n \n")
           return(TRUE)
         },
         { # Exit
           .GlobalEnv$.keep_running <- FALSE
           cat("Exiting program...\n \n")
           return(FALSE)
         },
         { # Default case for unexpected values
           cat("Invalid selection. Please try again.\n")
           return(TRUE)
         }
  )
}


###### PROGRAM ######

# Introduction to the GP Drug Finder Program

main_menu <- function() {
  cat("
                                                     
  Welcome to the GP Researcher program!
  ====================================
  
  Main Menu:
  =========
  1. Select a GP surgery for prescription, hypertension, and obesity information
  2. Compare Metformin prescription rates with hypertension and obesity rates
  3. Select a diabetic drug to compare with hypertension and obesity rates
  4. Performance and Spend Sub-Menu
  5. Exit
  ")
  
  choice <- as.integer(readline(prompt = "Enter the number of your selection and press Enter: "))
  
  if (!is.na(choice)) {
    switch(choice,
           { # Option 1. Select a GP surgery for prescription, hypertension, and obesity information
             cat("You selected option 1\n")
             cat("Please wait a few seconds...\n \n")
             select_gp_info()
           },
           { # Option 2. Compare Metformin prescription rates with hypertension and obesity rates
             cat("You selected option 2\n")
             cat("Please wait a few seconds...\n \n")
             select_metformin_drug_info()
           },
           { # Option 3. Select a diabetic drug to compare with hypertension and obesity rates
             cat("You selected option 3\n")
             cat("Please wait a few seconds...\n \n")
             select_diabetic_drug_info()
             
           },
           { # Option 4. Performance and Spend Sub-Menu
             cat("You selected option 4\n")
             cat("Please wait a few seconds...\n \n")
             select_efficiency_info()
           },
           { # Option 5. Exit
             cat("Exiting program...\n")
             return(FALSE)
           },
           { # Default case for unexpected values
             cat("Invalid selection. Please try again.\n")
             return(TRUE)
           })
    return(TRUE)
  } else {
    cat("Invalid input. Please enter a number.\n")
    return(TRUE)
  }
}

# Main loop
repeat {
  if (!.keep_running) {
    cat("Exiting program...\n")
    break
  }
  
  should_continue <- main_menu()
  if (!should_continue || !.keep_running) {
    cat("Thank you for using the GP Researcher program! Goodbye.\n")
    break
  }
}

# Disconnect from the database
dbDisconnect(con)
