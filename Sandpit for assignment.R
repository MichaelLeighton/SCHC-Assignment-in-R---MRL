library(RPostgreSQL)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)


# Global variable to control the main loop
.keep_running <- TRUE

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
  
  # Execute the query
  result <- dbGetQuery(con, query)
  
  # Extract the total prescriptions count
  total_prescriptions <- ifelse(nrow(result) > 0, result$total_prescriptions, 0)
  
  # Categorize the practice based on the median threshold
  practice_size <- if (total_prescriptions > median_threshold) 'Big' else 'Small'
  
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
    WHERE orgcode = '%s' AND indicator IN ('HYP001', 'HYP006')
    GROUP BY indicator", practice_id)
  data <- dbGetQuery(con, query)
  return(data)
}

# Function to calculate mean hypertension rate for all practices in Wales
fetch_mean_hypertension_rate_wales <- function(con) {
  query <- "
    SELECT indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator IN ('HYP001', 'HYP006')
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
  practice_sizes$size_category <- ifelse(practice_sizes$total_prescriptions > median_threshold, 'Big', 'Small')
  same_size_practices <- practice_sizes %>% 
    filter(size_category == practice_size) %>% 
    .$practiceid
  query <- sprintf("
    SELECT indicator, AVG(ratio) * 100 AS percentage
    FROM qof_achievement
    WHERE indicator IN ('HYP001', 'HYP006') AND orgcode IN ('%s')
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
  
  # Check for empty data frames before proceeding
  if (nrow(practice_data) == 0 || nrow(wales_data) == 0 || nrow(same_size_data) == 0) {
    cat("No data available for one or more categories. Cannot proceed with visualization.\n")
    return()
  }
  
  # Assign labels if all data frames have rows
  practice_data$Category <- "Selected Practice"
  wales_data$Category <- "Wales Average"
  same_size_data$Category <- sprintf("%s Practices Average", practice_size)
  
  # Combine the data
  combined_data <- rbind(practice_data, same_size_data, wales_data)
  combined_data$Category <- factor(combined_data$Category, levels = c("Selected Practice", sprintf("%s Practices Average", practice_size), "Wales Average"))
  
  cat("\nComparison of Hypertension Rate:\n===============================\n")
  print(combined_data)
  cat("\nDefinitions:\n===========\nHYP001: The contractor establishes and maintains a register of patients with established hypertension.\nHYP006: The percentage of patients with hypertension in whom the last blood pressure reading (measured in the preceding 12 months) is 150/90 mmHg or less.\n")
  
  # Visualize the data
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
  practice_sizes$size_category <- ifelse(practice_sizes$total_prescriptions > median_threshold, 'Big', 'Small')
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
  
  # Check for empty data frames before proceeding
  if (nrow(practice_data) == 0 || nrow(wales_data) == 0 || nrow(same_size_data) == 0) {
    cat("No data available for one or more categories. Cannot proceed with visualization.\n")
    return()
  }
  
  # Assign labels if all data frames have rows
  practice_data$Category <- "Selected Practice"
  wales_data$Category <- "Wales Average"
  same_size_data$Category <- sprintf("%s Practices Average", practice_size)
  
  # Combine data and adjust category levels for plotting
  combined_data <- rbind(practice_data, same_size_data, wales_data)
  combined_data$Category <- factor(combined_data$Category, levels = c("Selected Practice", sprintf("%s Practices Average", practice_size), "Wales Average"))
  
  cat("\nComparison of Obesity Rate:\n==========================\n")
  print(combined_data)
  cat("\nDefinitions:\n===========\nOB001W: The contractor establishes and maintains a register of patients aged 16 or over with a BMI greater than or equal to 30 in the preceding 15 months.\n")
  
  # Visualize the data
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
      
      # Fetch the postcode of the selected practice
      selected_practice_postcode <- dbGetQuery(con, sprintf("SELECT postcode FROM address WHERE practiceid = '%s'", selected_practice_id))$postcode
      
      # Execute combined function for top 10 drugs, top 5 drug categories, hypertension & obesity data
      combined_drug_hypertension_obesity_data(con, selected_practice_postcode, selected_practice_id)
    }
    
    # After fetching and displaying the information, prompt for next action
    cat("
  Please make a selection:
  =======================
  1. Select another practice
  2. Return to Main Menu
  ")

    next_action <- as.integer(readline(prompt = "Enter the number of your selection and press Enter: "))
    
    if (next_action == 2) {
      break  # Exit the repeat loop to return to the main menu
    } else if (next_action != 1) {
      cat("Invalid selection. Returning to main menu.\n")
      break
    }
  }
}

# Function to interpret correlation results
interpret_correlation <- function(cor_test_result) {
  # Determine the strength of the correlation based on the absolute value of the correlation coefficient
  cor_strength <- abs(cor_test_result$estimate)
  cor_significance <- cor_test_result$p.value
  
  # Define thresholds for the strength of correlation
  strength <- ifelse(cor_strength < 0.1, "very weak",
                     ifelse(cor_strength < 0.3, "weak",
                            ifelse(cor_strength < 0.5, "moderate",
                                   "strong")))
  
  # Determine if the correlation is significant
  significance <- ifelse(cor_significance < 0.05, "statistically significant", "not statistically significant")
  
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
    second_entry <- if (i <= length(second_col)) second_col[i] else ""
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
  
  choice <- as.integer(readline(prompt = "Enter the number of your selection and press Enter: "))
  
  if (choice == 2) {
    .GlobalEnv$.keep_running <- FALSE
  }
  
  return(choice)
}

# Function to allow the user to select a diabetic drug vs hypertension/obesity rates 
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
  
  # Combine the two columns. Since we're not padding, no need to adjust for length differences
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
  
  cat("Performing analysis. This may take a few moments...\n \n")
  
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
                                  WHERE indicator IN ('HYP001', 'HYP006')
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
  
  # Check if correlation tests were performed and interpret results if they were
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
    # Exit the program by returning FALSE, which will break the main loop
    cat("Exiting program...\n")
    return(FALSE)
  } else {
    cat("Invalid choice. Returning to the main menu.\n")
    return(TRUE)
  }
  
}

###### PROGRAM ######

# Introduction to the GP Drug Finder Program

main_menu <- function() {
  cat("
                                                     
  Welcome to the GP Researcher program!
  ====================================
  
  Main Menu:
  =========
  1. Select a GP for prescription, hypertension, and obesity information
  2. Compare Metformin prescription rates with hypertension and obesity rates
  3. Select a diabetic drug to compare with hypertension and obesity rates
  4. Placeholder
  5. Exit
  ")
  
  choice <- as.integer(readline(prompt = "Enter the number of your selection and press Enter: "))
  
  if (!is.na(choice)) {
    switch(choice,
           { # Option 1
             cat("You selected option 1\n")
             cat("Please wait a few seconds...\n \n")
             select_gp_info()
           },
           { # Option 2
             cat("You selected option 2\n")
             cat("Please wait a few seconds...\n \n")
             
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
                                      WHERE indicator IN ('HYP001', 'HYP006')
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
            
            # Apply the function to obesity and hypertension correlation results
            obesity_interpretation <- interpret_correlation(kendall_test_metformin_obesity)
            hypertension_interpretation <- interpret_correlation(kendall_test_metformin_hypertension)
            
            # Print the interpretations
            cat("Summary information:\n===================\n \n")
            cat("Obesity and Metformin:", obesity_interpretation, "\n \n")
            cat("Hypertension and Metformin:", hypertension_interpretation, "\n \n")
            
            # Compare the two correlations and print a statement about which is stronger
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
            
            # Handle the user's choice
            if (user_choice == 1) {
              # Return to Main Menu
              return(TRUE)
            } else if (user_choice == 2) {
              # Exit the program by returning FALSE, which will break the main loop
              cat("Exiting program...\n")
              return(FALSE)
            } else {
              cat("Invalid choice. Returning to the main menu.\n")
              return(TRUE)
            }
    
           },
           { # Option 3
             cat("You selected option 3\n")
             cat("Please wait a few seconds...\n \n")
             
             select_diabetic_drug_info()
             
           },
           { # Option 4
             cat("You selected option 4\n")
             cat("Please wait a few seconds...\n \n")
             # Current placeholder for Part 2 of Assignment
           },
           { # Option 5
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

