# Runs this query, retrieving a sample of logs, against the webrequests table,
# looking for a user-specified search type. Then cleans itself up.
get_data <- function(search_term){
  results <- wmf::hive_query(paste0("
                                    ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
                                    CREATE TEMPORARY FUNCTION search_type AS 'org.wikimedia.analytics.refinery.hive.SearchClassifierUDF';
                                    USE wmf;
                                    SELECT dt AS timestamp,
                                    client_ip AS ip_address,
                                    geocoded_data['country_code'] AS country,
                                    user_agent,
                                    user_agent_map['browser_family'] AS browser,
                                    user_agent_map['browser_major'] AS browser_version,
                                    uri_query AS query
                                    FROM webrequest
                                    WHERE year = 2015
                                    AND month = 08
                                    AND ((day = 03 AND hour = 07) OR (day = 06 AND hour = 14) OR (day = 12 AND hour = 22) OR (day = 16 AND hour = 04))
                                    AND search_type(uri_path, uri_query) = '", search_term, "'
                                    AND webrequest_source IN('text','mobile');"))
  results$timestamp <- wmf::from_log(results$timestamp)
  return(results[!is.na(results$timestamp),])
}

# Extract and graph the format info
format_info <- function(dataset, search_term){
  query_data <- urltools::url_parameters(dataset$query, "format")$format
  results <- as.data.frame(table(query_data), stringsAsFactors = FALSE)
  results$proportion <- results$Freq/sum(results$Freq)
  results$type <- search_term
  write.table(x = results, file = paste0(search_term, "_format_data.tsv"), row.names = FALSE, sep = "\t")
}

# Extract the country data
country_info <- function(dataset, search_term){
  results <- as.data.frame(table(dataset$country), stringsAsFactors = FALSE)
  results$proportion <- results$Freq/sum(results$Freq)
  results$type <- search_term
  write.table(x = results, file = paste0(search_term, "_country_data.tsv"), row.names = FALSE, sep = "\t")
}

# Get and write summary statistics
summary_stats <- function(dataset, search_term){
  
  # Calculate herfindahl value, entries and uniques
  unique_tuples <- unname(table(paste(dataset$ip_address, dataset$user_agent)))
  
  results <- data.frame(variable = c("Concentration", "Unique tuples", "Actions"),
                        value = c(
                          sum((unique_tuples/sum(unique_tuples))^2)^1,
                          length(unique_tuples),
                          sum(unique_tuples)
                        ),
                        type = search_term,
                        stringsAsFactors = FALSE)
  write.table(x = results, file = paste0(search_term, "_summary_stats.tsv"), row.names = FALSE, sep = "\t")
}

# Wrap it all together
central <- function(search_term){
  data <- get_data(search_term)
  format_info(data, search_term)
  country_info(data, search_term)
  summary_stats(data, search_term)
  return(TRUE)
}

# Visualise country information
top_country_info <- function(){
  
  # Read in and format
  files <- list.files(pattern = "country_data\\.tsv")
  results <- do.call("rbind", lapply(files, readr::read_delim, delim = "\t"))
  results <- results[!results$Var1 == "--",]
  results <- as.data.table(results)
  
  # Identify the top countries for each type and plot
  plots <- lapply(unique(results$type), function(x){
    local_copy <- results[results$type == x,]
    to_plot <- local_copy[order(local_copy$Freq, decreasing = T)][1:10]
    plot <- ggplot(to_plot, aes(x = reorder(Var1,proportion), y = proportion)) +
      geom_bar(stat="identity", position = "dodge", fill = "forestgreen") +
      theme_fivethirtyeight() + scale_x_discrete() + scale_y_continuous(labels=percent) +
      labs(title = paste0("Top Countries for '",x,"' Search\nAPI Requests")) + coord_flip()
    return(plot)
  })
  
  plots <- arrangeGrob(grobs = plots)
  png("nation_plot.png", width = 858, height = 621)
  grid.arrange(plots)
  garbage <- dev.off()
}

# Visualise format info
format_info <- function(){
  files <- list.files(pattern = "format_data\\.tsv")
  results <- do.call("rbind", lapply(files, readr::read_delim, delim = "\t"))
  results <- results[results$query_data %in% c("json","php","txt","xml"),]
  plot <- ggplot(results, aes(x = reorder(query_data, proportion), y = proportion, fill = factor(type))) +
    geom_bar(stat="identity", position = "dodge") +
    theme_fivethirtyeight() + scale_x_discrete() + scale_y_continuous(labels=percent) +
    labs(title = "Requested Data Format for Search API Requests") + coord_flip()
  ggsave(filename = "data_format_plot.png", plot = plot)
  return(plot)
}

# Look at the HH-concentration
concentration_info <- function(){
  files <- list.files(pattern = "summary_stats\\.tsv")
  results <- do.call("rbind", lapply(files, readr::read_delim, delim = "\t"))
  
  plot <- ggplot(results[results$variable == "Concentration",],
                 aes(x = reorder(type, value), y = value)) +
    geom_bar(stat="identity", position = "dodge", fill = "forestgreen") +
    theme_fivethirtyeight() + scale_x_discrete() + scale_y_continuous() +
    labs(title = "Concentration of requests by {IP, User Agent} tuples") + coord_flip()
  ggsave(filename = "concentration_plot.png", plot = plot)
  return(plot)
}

# Use information
use_info <- function(){
  files <- list.files(pattern = "summary_stats\\.tsv")
  results <- do.call("rbind", lapply(files, readr::read_delim, delim = "\t"))
  plot <- ggplot(results[results$variable == "Actions",],
                 aes(x = reorder(type, value), y = as.integer(value))) +
    geom_bar(stat="identity", position = "dodge", fill = "forestgreen") +
    theme_fivethirtyeight() + scale_x_discrete() + scale_y_continuous()+
    labs(title = "Requests by API method") + coord_flip()
  ggsave(filename = "usage_plot.png", plot = plot)
  return(plot)
}
