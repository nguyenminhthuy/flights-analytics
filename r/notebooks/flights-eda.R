library(arrow)
library(fs)
library(tidyverse)
library(dplyr)
library(plotly)
library(leaflet)
library(leaflet.extras)

#==================================#
# 1. LOAD DATA
# Load CSV/Parquet files into Polars DataFrames
# If Parquet exists, read it directly; otherwise read CSV, write Parquet, then read
#==================================#
csv_to_parquet <- function(csv_dir, parquet_dir, tables) {
  csv_dir <- path(csv_dir)
  parquet_dir <- path(parquet_dir)
  dir_create(parquet_dir)
  
  dfs <- list()
  
  for (table in tables) {
    parquet_path <- path(parquet_dir, paste0(table, ".parquet"))
    
    if (!file_exists(parquet_path)) {
      message("Creating parquet for: ", table)
      
      open_dataset(
        path(csv_dir, paste0(table, ".csv")),
        format = "csv"
      ) |>
        write_dataset(
          parquet_dir,
          format = "parquet",
          basename_template = paste0(table, ".parquet")
        )
    } 
    
    # read parquet
    dfs[[table]] <- read_parquet(parquet_path)
    message(
      table, ": ",
      nrow(dfs[[table]]), " rows, ",
      ncol(dfs[[table]]), " cols"
    )
  }
  dfs
}

tables <- c(
  "flights_sample_3m",
  "airports_clean"
)

dfs <- csv_to_parquet(
  csv_dir = "../../data/csv",
  parquet_dir = "../../data/parquet",
  tables = tables
)

df_flights  <- dfs$flights_sample_3m
df_airports <- dfs$airports_clean

#==================================#
# 2. FEATURE ENGINEERING & DATA CLEANING
# 2.1 Feature Engineering
# Time & Distance Features
#==================================#
dow_levels <- c("Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday")

df_flights <- df_flights |>
  mutate(
    FL_DATE = ymd(FL_DATE),
    YEAR = year(FL_DATE),
    MONTH = month(FL_DATE),
    QUARTER = quarter(FL_DATE),
    DAY_OF_WEEK = wday(FL_DATE), 
    
    DISTANCE_CAT = cut(
      DISTANCE,
      breaks = c(-Inf, 500, 1500, Inf),
      labels = c("Short-haul", "Medium-haul", "Long-haul")
    ),
    
    SEASON = case_when(
      MONTH %in% c(12, 1, 2) ~ "Winter",
      MONTH %in% c(3, 4, 5) ~ "Spring",
      MONTH %in% c(6, 7, 8) ~ "Summer",
      TRUE ~ "Autumn"
    )
  )

#----------------------------------#
# Adding Geolocation Columns
#----------------------------------#
airports_ll <- df_airports |>
  filter(!is.na(IATA), IATA != "") |>
  select(IATA, LAT, LON)

df_flights <- df_flights |>
  left_join(airports_ll, join_by(ORIGIN == IATA)) |>
  rename(
    ORIGIN_LAT = LAT,
    ORIGIN_LON = LON
  ) |>
  left_join(airports_ll, join_by(DEST == IATA)) |>
  rename(
    DEST_LAT = LAT,
    DEST_LON = LON
  )

#==================================#
# 2.2 Check for missing values
#==================================#
check_missing_cols <- function(df){
  missing_cols <- c()
  missing_counts <- c()
  no_missing_cols <- c()
  
  for(col in names(df)){
    na_count <- sum(is.na(df[[col]]))
    if(na_count > 0){
      missing_cols <- c(missing_cols, col)
      missing_counts <- c(missing_counts, na_count)
    }
    else{
      no_missing_cols <- c(no_missing_cols, col)
    }
  }
  
  cat("Columns WITH missing values:\n")
  if (length(missing_cols) > 0) {
    for (i in seq_along(missing_cols)) {
      cat("-", missing_cols[i], ":", missing_counts[i], "\n")
    }
  } else {
    cat("None\n")
  }
  
  cat("\nColumns WITHOUT missing values:\n")
  if (length(no_missing_cols) > 0) {
    print(no_missing_cols)
  } else {
    cat("None\n")
  }
}

check_missing_cols(df_flights)

#----------------------------------#
# Remove na
#----------------------------------#
df_flights <- df_flights |>
  drop_na(ORIGIN_LAT, ORIGIN_LON,
          DEST_LAT, DEST_LON)

#==================================#
# 2.3 Format numbers
#==================================#
format_compact <- function(n) {
  if (n >= 1e6) {
    x <- n / 1e6
    return(sub("\\.0M$", "M", sprintf("%.2fM", x)))
  }
  if (n >= 1e3) {
    x <- n / 1e3
    return(sub("\\.0K$", "K", sprintf("%.2fK", x)))
  }
  as.character(n)
}

#==================================#
# 3. EDA
# 3.1 Homepage
# Summary
#==================================#
pct_vs_baseline <- function(df, year, baseline, col) {
  n_year <- df |>
    filter(YEAR == year) |>
    pull({{col}})
  
  n_base <- df |>
    filter(YEAR == baseline) |>
    pull({{col}})
  
  round((n_year - n_base) / n_base * 100, 2)
}

#----------------------------------#
flight_yearly <- df_flights |>
  group_by(YEAR) |>
  summarise(n_flights = n(), .groups = "drop")

# Cancelled
df_cancel <- df_flights |>
  filter(CANCELLED == 1) |>
  group_by(YEAR) |>
  summarise(n_cancelled = n(), .groups = "drop")

# Diverted
df_divert <- df_flights |>
  filter(DIVERTED == 1) |>
  group_by(YEAR) |>
  summarise(n_diverted = n(), .groups = "drop")

# Operated flights (without cancelled/diverted)
df_operated <- df_flights |>
  filter(CANCELLED == 0, DIVERTED == 0) |>
  group_by(YEAR) |>
  summarise(n_operated = n(), .groups = "drop")

# On-time / Delayed in operated flights
df_ontime_delay <- df_flights |>
  filter(CANCELLED == 0, DIVERTED == 0) |>
  mutate(
    n_ontime = ifelse(DEP_DELAY <= 15, 1, 0),
    n_delayed = ifelse(DEP_DELAY > 15, 1, 0)
  ) |>
  group_by(YEAR) |>
  summarise(
    n_ontime = sum(n_ontime),
    n_delayed = sum(n_delayed),
    .groups = "drop"
  )

# Tính % vs baseline 2019
pct_23_19_flight <- pct_vs_baseline(flight_yearly, 2023, 2019, n_flights)
pct_23_19_cancel <- pct_vs_baseline(df_cancel, 2023, 2019, n_cancelled)
pct_23_19_divert <- pct_vs_baseline(df_divert, 2023, 2019, n_diverted)
pct_23_19_operated <- pct_vs_baseline(df_operated, 2023, 2019, n_operated)
pct_23_19_ontime <- pct_vs_baseline(df_ontime_delay, 2023, 2019, n_ontime)
pct_23_19_delayed <- pct_vs_baseline(df_ontime_delay, 2023, 2019, n_delayed)

# Tổng số liệu hiện tại
total_flights <- nrow(df_flights)
total_airlines <- n_distinct(df_flights$AIRLINE)
total_airports <- n_distinct(df_flights$ORIGIN)
total_routes <- nrow(df_flights |>
                       distinct(ORIGIN, DEST))

total_flights_fmt <- format_compact(total_flights)

cat(sprintf("U.S. Flight Operations: 2019–2023\n"))
cat(sprintf("Flights volume vs 2019: %s%%\n", pct_23_19_flight))
cat(sprintf("Cancelled vs 2019: %s%%\n", pct_23_19_cancel))
cat(sprintf("Diverted vs 2019: %s%%\n", pct_23_19_divert))
cat(sprintf("Operated vs 2019: %s%%\n", pct_23_19_operated))
cat(sprintf("  - On-time vs 2019: %s%%\n", pct_23_19_ontime))
cat(sprintf("  - Delayed vs 2019: %s%%\n", pct_23_19_delayed))
cat(sprintf("Total flights: %s\n", total_flights_fmt))
cat(sprintf("Total airlines: %s\n", total_airlines))
cat(sprintf("Total airports: %s\n", total_airports))
cat(sprintf("Total routes: %s\n", total_routes))

#==================================#
# Chart: Weekly Distribution
#==================================#
dow_map <- c(
  "1" = "Monday", "2" = "Tuesday", "3" = "Wednesday",
  "4" = "Thursday", "5" = "Friday",
  "6" = "Saturday", "7" = "Sunday"
)

df_dow <- df_flights |>
  group_by(YEAR, DAY_OF_WEEK) |>
  summarise(n_flights = n(), .groups = "drop") |>
  mutate(
    DAY  = dow_map[as.character(DAY_OF_WEEK)],
    YEAR = as.character(YEAR)
  ) |>
  arrange(YEAR, DAY_OF_WEEK)

year_colors <- c(
  "2019" = "#47c6d1",
  "2020" = "#de506f",
  "2021" = "#e657e3",
  "2022" = "#deab4e",
  "2023" = "#4df06b"
)

fig_dow <- plot_ly(
  df_dow,
  x = ~DAY, y = ~n_flights,
  color = ~YEAR, colors = year_colors,
  type = "bar", opacity = 0.8
) |>
  layout(
    barmode = "stack",
    showlegend = FALSE,
    xaxis = list(title = ""),
    yaxis = list(title = "", showgrid = FALSE),
    title = list(
      text = paste0(
        "WEEKLY DISTRIBUTION",
        "<br><span style='font-size:14px;color:#666;font-style:italic'>",
        "Number of flights</span>"
      ),
      x = 0.5,
      xanchor = "center",
      font = list(size = 16, color = "#333", family = "Comic Sans MS")
    ),
    margin = list(l = 10, r = 10, t = 70, b = 10),
    paper_bgcolor = "#f5f5f5",
    plot_bgcolor  = "#f5f5f5"
  ) |>
  config(responsive = TRUE)

fig_dow

#==================================#
# Chart: Airline Rankings
#==================================#
df_air_rank <- df_flights |>
  group_by(AIRLINE) |>
  summarise(n_flights = n(), .groups = "drop") |>
  arrange(desc(n_flights)) |>
  mutate(
    # ép factor trước khi plot để reorder
    AIRLINE = factor(AIRLINE, levels = AIRLINE),
    text = sapply(n_flights, format_compact)
  )

fig_air_rank <- plot_ly(
  df_air_rank,
  x = ~n_flights,
  y = ~AIRLINE,
  type = "bar",
  orientation = "h",
  text = ~text,
  textposition = "outside",
  cliponaxis = FALSE,
  opacity = 0.8,
  marker = list(color = "#59cf5a"),
  height = 500
) |>
  layout(
    yaxis = list(
      title = "",
      autorange = "reversed" # đảo trục y
    ),
    xaxis = list(
      title = "",
      showgrid = FALSE
    ),
    title = list(
      text = paste0(
        "AIRLINE RANKING",
        "<br><span style='font-size:14px;color:#666;font-style:italic'>",
        "by number of flights</span>"
      ),
      x = 0.5,
      xanchor = "center",
      font = list(size = 16, color = "#333", family = "Comic Sans MS")
    ),
    margin = list(l = 200, r = 50, t = 70, b = 10),
    paper_bgcolor = "#f5f5f5",
    plot_bgcolor  = "#f5f5f5"
  ) |>
  config(responsive = TRUE)

fig_air_rank

#==================================#
# Chart: Monthly Departures
#==================================#
df_monthly_departures <- df_flights |>
  group_by(MONTH) |>
  summarise(n_flights = n(), .groups = "drop") |>
  arrange(MONTH) |>
  mutate(text = sapply(n_flights, format_compact))

fig_monthly <- plot_ly(
  df_monthly_departures,
  x = ~MONTH,
  y = ~n_flights,
  type = "scatter",
  mode = "lines+markers+text",
  text = ~text,
  textposition = "top center",
  cliponaxis = FALSE,
  opacity = 0.8,
  line = list(color = "#c82fd6", width = 3),
  marker = list(size = 8),
  height = 400
) |>
  layout(
    yaxis = list(title = ""),
    xaxis = list(
      title = "",
      showgrid = FALSE,
      type = "category"   # giống fig.update_xaxes(type="category")
    ),
    title = list(
      text = paste0(
        "MONTHLY DEPARTURES",
        "<br><span style='font-size:14px;color:#666;font-style:italic'>",
        "by number of flights</span>"
      ),
      x = 0.5,
      xanchor = "center",
      font = list(size = 16, color = "#333", family = "Comic Sans MS")
    ),
    margin = list(l = 10, r = 10, t = 70, b = 10),
    paper_bgcolor = "#f5f5f5",
    plot_bgcolor  = "#f5f5f5"
  ) |>
  config(responsive = TRUE)

fig_monthly

#==================================#
# Chart: Flight Distance Distribution
#==================================#
df_distance_departures <- df_flights |>
  group_by(DISTANCE_CAT) |>
  summarise(n_flights = n(), .groups = "drop") |>
  arrange(DISTANCE_CAT)

fig_distance <- plot_ly(
  df_distance_departures,
  x = ~DISTANCE_CAT,
  y = ~n_flights,
  type = "bar",
  color = ~DISTANCE_CAT,
  colors = c("#f76af7", "#e8b76d", "#51d6b3"),
  opacity = 0.8,
  height = 400
) |>
  layout(
    showlegend = FALSE,
    xaxis = list(title = ""),
    yaxis = list(title = ""),
    title = list(
      text = paste0(
        "FLIGHT DISTANCE DISTRIBUTION",
        "<br><span style='font-size:14px;color:#666;font-style:italic'>",
        "by number of flights</span>"
      ),
      x = 0.5,
      xanchor = "center",
      font = list(size = 16, color = "#333", family = "Comic Sans MS")
    ),
    margin = list(l = 10, r = 10, t = 70, b = 10),
    paper_bgcolor = "#f5f5f5",
    plot_bgcolor  = "#f5f5f5"
  ) |>
  config(responsive = TRUE)

fig_distance

#==================================#
# Chart: National Coverage
#==================================#
df_flights <- df_flights |>
  mutate(
    ORIGIN_AIRPORT = paste0(ORIGIN_CITY, " (", ORIGIN, ")")
  )

df_origin_airport <- df_flights |>
  group_by(ORIGIN_AIRPORT, ORIGIN_LAT, ORIGIN_LON) |>
  summarise(n_flights = n(), .groups = "drop") |>
  mutate(text = sapply(n_flights, format_compact))

pal <- colorNumeric(
  palette = c("#3b82f6", "#22c55e", "#f59e0b", "#ef4444"),
  domain = df_origin_airport$n_flights
)

radius_fun <- function(x) pmin(5, log(x + 1) * 4)

origin_map <- leaflet(df_origin_airport) |>
  addProviderTiles(providers$CartoDB.Positron) |>
  setView(
    lng = mean(df_origin_airport$ORIGIN_LON),
    lat = mean(df_origin_airport$ORIGIN_LAT),
    zoom = 4
  ) |>
  
  # 🔵 layer 1: halo mờ (nhòe)
  addCircleMarkers(
    lng = ~ORIGIN_LON,
    lat = ~ORIGIN_LAT,
    radius = ~radius_fun(n_flights) * 1.8,
    fillColor = ~pal(n_flights),
    fillOpacity = 0.25,
    stroke = FALSE
  ) |>
  
  # 🔴 layer 2: bubble chính (rõ)
  addCircleMarkers(
    lng = ~ORIGIN_LON,
    lat = ~ORIGIN_LAT,
    radius = ~radius_fun(n_flights),
    fillColor = ~pal(n_flights),
    fillOpacity = 0.75,
    stroke = FALSE,
    label = ~paste0(ORIGIN_AIRPORT, "<br>", text, " flights")
  )

origin_map














