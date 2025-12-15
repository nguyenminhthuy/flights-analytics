library(tidyverse)
library(tidyr)
library(dplyr)
library(ggplot2)
library(readr)
library(data.table)
library(plotly)
library(lubridate)

df = fread("data/flights_sample_2m.csv", header=TRUE)

############################################
# PREPARATION
############################################
dow_levels <- c("Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday")

df <- df |>
  mutate(
    FL_DATE = ymd(FL_DATE),
    YEAR = year(FL_DATE),
    MONTH = month(FL_DATE),
    QUARTER = quarter(FL_DATE),
    
    # wday(): Sunday = 1 → Monday = 2 → ... → Saturday = 7
    DAY_NUM = wday(FL_DATE), 
    # Chuyển về Monday = 1, ... Sunday = 7
    DAY_NUM = ifelse(DAY_NUM == 1, 7, DAY_NUM - 1),
    DAY_OF_WEEK = factor(dow_levels[DAY_NUM], 
                         levels = dow_levels, 
                         ordered = TRUE),
    
    DISTANCE_CAT = cut(
      DISTANCE,
      breaks = c(-Inf, 500, 1500, Inf),
      labels = c("Short-haul", "Medium-haul", "Long-haul")
    ),
    
    DEP_HOUR = factor(floor(CRS_DEP_TIME / 100), levels = 0:23),
    
    SEASON = case_when(
      MONTH %in% c(12, 1, 2) ~ "Winter",
      MONTH %in% c(3, 4, 5) ~ "Spring",
      MONTH %in% c(6, 7, 8) ~ "Summer",
      TRUE ~ "Fall"
    )
  )

############################################
# OVERVIEW DATASET
############################################
n_flight_delay <- sum(df$ARR_DELAY > 15, na.rm=TRUE)
n_flight_ontime <- sum(df$ARR_DELAY <= 15, na.rm = TRUE)

n_flight_cancel <- sum(df$CANCELLED == 1, na.rm = TRUE)
n_flight_divert <- sum(df$DIVERTED == 1, na.rm = TRUE)

n_flights <- nrow(df)

lst_airline <- sort(unique(df$AIRLINE))
n_airlines  <- length(lst_airline)

n_short_distance  <- sum(df$DISTANCE_CAT == "Short-haul", na.rm = TRUE)
n_medium_distance <- sum(df$DISTANCE_CAT == "Medium-haul", na.rm = TRUE)
n_long_distance   <- sum(df$DISTANCE_CAT == "Long-haul", na.rm = TRUE)

# Print output
cat("===== Overall Dataset =====\n")
cat("Total flights:", format(n_flights, big.mark=","), "\n")
cat("Total airlines:", n_airlines, "\n")
cat("There are", format(n_flight_delay,  big.mark=","), "flights delayed\n")
cat("There are", format(n_flight_cancel, big.mark=","), "flights cancelled\n")
cat("There are", format(n_flight_divert, big.mark=","), "flights diverted\n")
cat("There are", format(n_flight_ontime, big.mark=","), "on-time flights\n")
cat("There are", format(n_short_distance,  big.mark=","), "short-haul flights\n")
cat("There are", format(n_medium_distance, big.mark=","), "medium-haul flights\n")
cat("There are", format(n_long_distance,   big.mark=","), "long-haul flights\n")

############################################
# DESCRIPTIVE ANALYSIS
############################################
df_flights <- copy(df)

df_clean <- df_flights |>
  filter(ARR_DELAY > 15)

fig_1 <- plot_ly(
    df_clean, 
    x = ~ARR_DELAY, type = "histogram",
    marker = list(
      color = "aquamarine",
      line = list(color = "black", width = 0.7)
    )) |>
  layout(title = "Distribution of Arrival Delays",
         xaxis = list(title = "Arrival Delay (minutes)", range = c(-60, 180)),
         yaxis = list(title = "Number of flights"),
         bargap=0
    ) |>
  config(responsive = TRUE)
fig_1

############################################

############################################
group_by_year <- function(df, value_col, new_name){
  df |>
    group_by(YEAR) |>
    summarise(
      !!new_name := n(),
      .groups = "drop"
    ) |> ungroup()
}

plot_yearly_flights_by_airline <- function(df, airline_name){
  airline_df <- df |>
    filter(AIRLINE == airline_name)
  
  airline_yearly <- group_by_year(airline_df, "FL_DATE", "Total_Flights")
  
  delayed_df <- airline_df |> filter(ARR_DELAY > 15)
  ontime_df <- airline_df |> filter(ARR_DELAY <= 15)
  cancel_df <- airline_df |> filter(CANCELLED == 1)
  divert_df <- airline_df |> filter(DIVERTED == 1)
  
  delayed_yearly <- group_by_year(delayed_df, "FL_DATE", "Delayed_Flights")
  ontime_yearly <- group_by_year(ontime_df, "FL_DATE", "OnTime_Flights")
  cancel_yearly <- group_by_year(cancel_df, "FL_DATE", "Cancelled_Flights")
  divert_yearly <- group_by_year(divert_df, "FL_DATE", "Diverted_Flights")
  
  dfs <- list(airline_yearly, delayed_yearly, ontime_yearly, cancel_yearly, divert_yearly)
  final_df <- Reduce(
    function(l, r) left_join(l, r, by = "YEAR"),
    dfs
  )
  final_df[is.na(final_df)] <- 0
  
  final_df <- final_df |>
    mutate(
      Arrival_Delay_Rate = round((Delayed_Flights / Total_Flights *100), 2),
      OnTime_Rate = round((OnTime_Flights / Total_Flights *100), 2),
      Cancel_Rate = round((Cancelled_Flights / Total_Flights *100), 2),
      Divert_Rate = round((Diverted_Flights / Total_Flights *100), 2)
    )
  
  # --- Plot ---
  fig <- plot_ly(
    data = airline_yearly,
    x = ~YEAR, y = ~Total_Flights,
    type = "bar", color = ~as.factor(YEAR),
    height=450
  ) |>
    layout(
      title = paste0("Total number of flights of ", airline_name, " (2019–2023)"),
      xaxis = list(type = "category", title = "YEAR"),
      yaxis = list(title = "Total Flights")
      #height = 450
    ) |> config(responsive = TRUE)
  return(list(data = final_df, plot = fig))
}

plot_yearly_flights_by_airline(df_flights, "Envoy Air")

############################################

############################################
group_by_quarter <- function(df, value_col, new_name){
  df |>
    group_by(YEAR, QUARTER) |>
    summarise(
      !!new_name := n(),
      .groups = "drop"
    ) |> ungroup()
}

plot_quarter_flights_by_airline <- function(df, airline_name){
  airline_df <- df |>
    filter(AIRLINE == airline_name)
  
  airline_quarter <- group_by_quarter(airline_df, "FL_DATE", "Total_Flights")
  
  delayed_df <- airline_df |> filter(ARR_DELAY > 15)
  ontime_df <- airline_df |> filter(ARR_DELAY <= 15)
  cancel_df <- airline_df |> filter(CANCELLED == 1)
  divert_df <- airline_df |> filter(DIVERTED == 1)
  
  delayed_quarter <- group_by_quarter(delayed_df, "FL_DATE", "Delayed_Flights")
  ontime_quarter <- group_by_quarter(ontime_df, "FL_DATE", "OnTime_Flights")
  cancel_quarter <- group_by_quarter(cancel_df, "FL_DATE", "Cancelled_Flights")
  divert_quarter <- group_by_quarter(divert_df, "FL_DATE", "Diverted_Flights")
  
  dfs <- list(airline_quarter, delayed_quarter, ontime_quarter, cancel_quarter, divert_quarter)
  final_df <- Reduce(
    function(l, r) left_join(l, r, by = c("YEAR", "QUARTER")),
    dfs
  )
  final_df[is.na(final_df)] <- 0
  
  final_df <- final_df |>
    mutate(
      Arrival_Delay_Rate = round((Delayed_Flights / Total_Flights *100), 2),
      OnTime_Rate = round((OnTime_Flights / Total_Flights *100), 2),
      Cancel_Rate = round((Cancelled_Flights / Total_Flights *100), 2),
      Divert_Rate = round((Diverted_Flights / Total_Flights *100), 2)
    )
  
  # --- Plot ---
  fig <- plot_ly(
    data = airline_quarter,
    x = ~QUARTER, y = ~Total_Flights,
    type = "scatter", color = ~as.factor(YEAR), mode = "lines+markers"
  ) |>
    layout(
      title = paste0("Quarterly Flights Trend of ", airline_name, " (2019–2023)"),
      xaxis = list(type = "category", title = "Quarter"),
      yaxis = list(title = "Total Flights")
    ) |> config(responsive = TRUE)
  return(list(data = final_df, plot = fig))
}

plot_quarter_flights_by_airline(df_flights, "Envoy Air")

############################################

############################################
group_by_monthly <- function(df, value_col, new_name){
  df |>
    group_by(YEAR, MONTH) |>
    summarise(
      !!new_name := n(),
      .groups = "drop"
    ) |> ungroup()
}

plot_monthly_flights_by_airline <- function(df, airline_name){
  airline_df <- df |>
    filter(AIRLINE == airline_name)
  
  airline_monthly <- group_by_monthly(airline_df, "FL_DATE", "Total_Flights")
  
  delayed_df <- airline_df |> filter(ARR_DELAY > 15)
  ontime_df <- airline_df |> filter(ARR_DELAY <= 15)
  cancel_df <- airline_df |> filter(CANCELLED == 1)
  divert_df <- airline_df |> filter(DIVERTED == 1)
  
  delayed_monthly <- group_by_monthly(delayed_df, "FL_DATE", "Delayed_Flights")
  ontime_monthly <- group_by_monthly(ontime_df, "FL_DATE", "OnTime_Flights")
  cancel_monthly <- group_by_monthly(cancel_df, "FL_DATE", "Cancelled_Flights")
  divert_monthly <- group_by_monthly(divert_df, "FL_DATE", "Diverted_Flights")
  
  dfs <- list(airline_monthly, delayed_monthly, ontime_monthly, cancel_monthly, divert_monthly)
  final_df <- Reduce(
    function(l, r) left_join(l, r, by = c("YEAR", "MONTH")),
    dfs
  )
  final_df[is.na(final_df)] <- 0
  
  # --- Tính tỷ lệ ---
  final_df <- final_df |>
    mutate(
      Arrival_Delay_Rate = round((Delayed_Flights / Total_Flights *100), 2),
      OnTime_Rate = round((OnTime_Flights / Total_Flights *100), 2),
      Cancel_Rate = round((Cancelled_Flights / Total_Flights *100), 2),
      Divert_Rate = round((Diverted_Flights / Total_Flights *100), 2)
    )
  
  # --- Plot ---
  fig <- plot_ly(
    data = airline_monthly,
    x = ~MONTH, y = ~Total_Flights,
    type = "scatter", color = ~as.factor(YEAR), mode = "lines+markers"
  ) |>
    layout(
      title = paste0("Monthly Flights Trend of ", airline_name, " (2019–2023)"),
      xaxis = list(type = "category", title = "Month"),
      yaxis = list(title = "Total Flights")
    ) |> config(responsive = TRUE)
  return(list(data = final_df, plot = fig))
}

plot_monthly_flights_by_airline(df_flights, "Envoy Air")

############################################

############################################
group_by_dow <- function(df, value_col, new_name){
  df |>
    group_by(YEAR, DAY_OF_WEEK) |>
    summarise(
      !!new_name := n(),
      .groups = "drop"
    ) |> ungroup()
}

plot_weekly_flights_by_airline <- function(df, airline_name){
  airline_df <- df |>
    filter(AIRLINE == airline_name)
  
  airline_dow <- group_by_dow(airline_df, "FL_DATE", "Total_Flights")
  
  delayed_df <- airline_df |> filter(ARR_DELAY > 15)
  ontime_df <- airline_df |> filter(ARR_DELAY <= 15)
  cancel_df <- airline_df |> filter(CANCELLED == 1)
  divert_df <- airline_df |> filter(DIVERTED == 1)
  
  delayed_dow <- group_by_dow(delayed_df, "FL_DATE", "Delayed_Flights")
  ontime_dow <- group_by_dow(ontime_df, "FL_DATE", "OnTime_Flights")
  cancel_dow <- group_by_dow(cancel_df, "FL_DATE", "Cancelled_Flights")
  divert_dow <- group_by_dow(divert_df, "FL_DATE", "Diverted_Flights")
  
  dfs <- list(airline_dow, delayed_dow, ontime_dow, cancel_dow, divert_dow)
  final_df <- Reduce(
    function(l, r) left_join(l, r, by = c("YEAR", "DAY_OF_WEEK")),
    dfs
  )
  final_df[is.na(final_df)] <- 0
  
  final_df <- final_df |>
    mutate(
      Arrival_Delay_Rate = round((Delayed_Flights / Total_Flights *100), 2),
      OnTime_Rate = round((OnTime_Flights / Total_Flights *100), 2),
      Cancel_Rate = round((Cancelled_Flights / Total_Flights *100), 2),
      Divert_Rate = round((Diverted_Flights / Total_Flights *100), 2)
    )
  
  # --- Plot ---
  fig <- plot_ly(
    data = airline_dow,
    x = ~DAY_OF_WEEK, y = ~Total_Flights,
    type = "scatter", color = ~as.factor(YEAR), mode = "lines+markers"
  ) |>
    layout(
      title = paste0("Weekly Flights Trend of ", airline_name, " (2019–2023)"),
      xaxis = list(type = "category", title = "Day of Week"),
      yaxis = list(title = "Total Flights")
    ) |> config(responsive = TRUE)
  return(list(data = final_df, plot = fig))
}

plot_weekly_flights_by_airline(df_flights, "Envoy Air")

############################################

############################################
plot_delay_by_dep_hh <- function(df, airline_name){
  airline_df <- df |> filter(AIRLINE == airline_name)
  
  delay_avg_hourly <- airline_df |>
    group_by(YEAR, DEP_HOUR) |>
    summarise(Avg_Departure_Delay = round(mean(DEP_DELAY, na.rm=TRUE), 1),
              .groups = "drop")
  
  fig <- plot_ly(
    data = delay_avg_hourly,
    x = ~DEP_HOUR, y = ~Avg_Departure_Delay,
    type = "bar", color = ~as.factor(YEAR)
  ) |>
    layout(
      barmode="stack", # stacked bar
      title = paste0("Average Departure Delay of ", airline_name, " by Departure Hour"),
      xaxis = list(title = "DEP_HOUR"),
      yaxis = list(title = "Average Departure Delay")
    ) |> config(responsive = TRUE)
  return(list(data = delay_avg_hourly, plot = fig))
}

plot_delay_by_dep_hh(df_flights, "Envoy Air")

############################################

############################################
plot_delay_by_seasonal <- function(df, airline_name) {
  airline_df <- df |> filter(AIRLINE == airline_name)
  
  delay_seasonal <- airline_df |>
    group_by(YEAR, SEASON) |>
    summarise(Avg_Departure_Delay = round(mean(DEP_DELAY, na.rm = TRUE), 1),
              .groups = "drop")
  
  # --- Pivot để heatmap dùng được ---
  pivot <- delay_seasonal |>
    pivot_wider(names_from = YEAR, values_from = Avg_Departure_Delay)
  
  # --- Chuyển sang matrix để plotly heatmap ---
  seasons <- c("Winter", "Summer", "Spring", "Fall")  # thứ tự từ trên xuống
  pivot_matrix <- as.matrix(pivot[match(seasons, pivot$SEASON), -1, drop = FALSE])
  
  fig <- plot_ly(
    x = colnames(pivot_matrix), y = seasons, z = pivot_matrix,
    type = "heatmap", colorscale = "YlOrRd", reversescale=TRUE,
    text = round(pivot_matrix, 1),
    texttemplate = "%{text}"
  ) |>
    layout(
      title = paste0("Average Departure Delay by Season — ", airline_name),
      xaxis = list(title = "YEAR", type = "category"),
      yaxis = list(title = "SEASON")
    ) |>
    config(responsive = TRUE)
  return(list(data = delay_seasonal, plot = fig))
}

plot_delay_by_seasonal(df_flights, "Envoy Air")

############################################

############################################
calc_delay_cancel_rate <- function(df, group_col) {
  df |>
    group_by(across(all_of(group_col))) |>
    summarise(
      total_flights = n(),
      delay_flights = sum(DEP_DELAY > 0, na.rm = TRUE),
      cancelled_flights = sum(CANCELLED, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      delay_rate = round(delay_flights / total_flights * 100, 3),
      cancel_rate = round(cancelled_flights / total_flights * 100, 3)
    ) |>
    arrange(desc(delay_rate))
}

print_top_delay_cancel <- function(df, group_col, label) {
  rates <- calc_delay_cancel_rate(df, group_col)
  
  cat("\n=== ", label, " có tỉ lệ DELAY cao nhất ===\n", sep = "")
  print(head(rates |> arrange(desc(delay_rate)), 5))
  
  cat("\n=== ", label, " có tỉ lệ CANCEL cao nhất ===\n", sep = "")
  print(head(rates |> arrange(desc(cancel_rate)), 5))
}

print_top_delay_cancel(df, "AIRLINE", "Hãng bay")
print_top_delay_cancel(df, "ORIGIN", "Sân bay")

############################################

############################################
analyze_flights_routes <- function(df, group_col, group_value, top_n = 5) {
  df_sub <- df |> filter(.data[[group_col]] == group_value)
  
  top_flights <- df_sub |>
    group_by(FL_NUMBER) |>
    summarise(Total_Flights = n(), .groups = "drop") |>
    arrange(desc(Total_Flights)) |>
    head(top_n)
  
  cat("\n=== Top", top_n, "chuyến bay đông nhất của", group_value, "(", group_col, ") ===\n")
  print(top_flights)
  
  df_sub <- df_sub |>
    mutate(ROUTE = paste(ORIGIN_CITY, "→", DEST_CITY))
  
  top_routes <- df_sub |>
    group_by(ROUTE) |>
    summarise(Total_Flights = n(), .groups = "drop") |>
    arrange(desc(Total_Flights)) |>
    head(top_n)
  
  cat("\n=== Top", top_n, "route phổ biến nhất của", group_value, "(", group_col, ") ===\n")
  print(top_routes)
  
  delay_routes <- df_sub |>
    group_by(ROUTE) |>
    summarise(Avg_Departure_Delay = round(mean(DEP_DELAY, na.rm = TRUE), 1),
              .groups = "drop") |>
    arrange(desc(Avg_Departure_Delay)) |>
    head(top_n)
  
  cat("\n=== Top", top_n, "route có delay trung bình cao nhất của", group_value, "(", group_col, ") ===\n")
  print(delay_routes)
}

analyze_flights_routes(df_flights, "AIRLINE", "SkyWest Airlines Inc.")
analyze_flights_routes(df_flights, "ORIGIN", "JFK")

############################################

############################################
growth_analysis <- function(df, group_col, group_value) {
  grp <- df |>
    filter(.data[[group_col]] == group_value) |>
    group_by(YEAR) |>
    summarise(Group_Flights = n(), .groups = "drop")
  
  industry <- df |>
    group_by(YEAR) |>
    summarise(Industry_Flights = n(), .groups = "drop")
  
  out <- left_join(grp, industry, by = "YEAR")
  
  out <- out |>
    arrange(YEAR) |>
    mutate(
      Group_Growth = round((Group_Flights / lag(Group_Flights) - 1) * 100, 2),
      Industry_Growth = round((Industry_Flights / lag(Industry_Flights) - 1) * 100, 2)
    )
  
  fig <- plot_ly(out, x = ~YEAR, height = 450) |>
    add_lines(y = ~Group_Growth, name = paste0(group_value, " Growth"), 
              marker = list(symbol = "circle")) |>
    add_lines(y = ~Industry_Growth, name = "Industry Growth", 
              marker = list(symbol = "circle")) |>
    layout(
      title = paste0("Recovery Rate Comparison: ", group_value, " (", group_col, ") vs Industry"),
      yaxis = list(title = "Growth Rate (%)"),
      xaxis = list(title = "YEAR", type = "category"),
      legend = list(title = list(text = ""))
    ) |>
    config(responsive = TRUE)
  return(list(data = out, plot = fig))
}

growth_analysis(df_flights, "ORIGIN", "MDW")


