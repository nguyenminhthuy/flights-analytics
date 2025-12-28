library(arrow)
library(fs)
library(tidyverse)
library(dplyr)
library(plotly)
library(leaflet)
library(leaflet.extras)
library(rlang)

options(dplyr.summarise.inform = FALSE)

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

df_flights <- dfs$flights_sample_3m
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
    ),
    ROUTE = paste0(ORIGIN_CITY, " → ", DEST_CITY),
    DEP_HOUR = as.integer(CRS_DEP_TIME %/% 100)
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
# df_flights <- df_flights |>
#   drop_na(ORIGIN_LAT, ORIGIN_LON,
#           DEST_LAT, DEST_LON)

#==================================#
# 2.3 Format numbers
#==================================#
format_compact <- function(n) {
  case_when(
    n >= 1e6 ~ sub("\\.0M$", "M", sprintf("%.2fM", n / 1e6)),
    n >= 1e3 ~ sub("\\.0K$", "K", sprintf("%.2fK", n / 1e3)),
    TRUE ~ as.character(n)
  )
}

#==================================#
# 3. EDA
# 3.1 Homepage
# Summary (Group_by --> Filter last year & first year)
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

get_operated_flights <- function(df){
  df |> filter(CANCELLED == 0 & DIVERTED == 0)
}
  
count_by_year <- function(df, condition = NULL, col_name = "n") {
  condition <- enquo(condition)
  
  if (!quo_is_null(condition)) {
    df <- filter(df, !!condition)
  }
  
  df |>
    group_by(YEAR) |>
    summarise(
      !!col_name := n(),
      .groups = "drop"
    ) |>
    arrange(YEAR)
}

ontime_delay_by <- function(df, group_cols){
  df_op <- get_operated_flights(df)
  
  df_op |>
    mutate(
      n_ontime = as.integer(DEP_DELAY <= 15),
      n_delayed = as.integer(DEP_DELAY > 15),
    ) |>
    group_by(across(all_of(group_cols))) |>
    summarise(
      n_ontime = sum(n_ontime, na.rm = TRUE),
      n_delayed = sum(n_delayed, na.rm = TRUE),
      n_operated = n(),
      avg_delay = mean(DEP_DELAY, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      pct_ontime  = n_ontime / n_operated * 100,
      pct_delayed = n_delayed / n_operated * 100
    ) |>
    arrange(across(all_of(group_cols)))
}

#----------------------------------#
flight_yearly <- count_by_year(df_flights, col_name="n_flights")

df_cancel <- count_by_year(
  df_flights,
  CANCELLED == 1,
  col_name = "n_cancelled"
)

df_divert <- count_by_year(
  df_flights,
  DIVERTED == 1,
  col_name = "n_diverted"
)

df_operated <- count_by_year(
  df_flights,
  CANCELLED == 0 & DIVERTED == 0,
  col_name = "n_operated"
)

df_ontime_delay <- ontime_delay_by(df_flights, "YEAR")

pct_23_19_flight <- pct_vs_baseline(flight_yearly, 2023, 2019, n_flights)

pct_23_19_cancel <- pct_vs_baseline(df_cancel, 2023, 2019, n_cancelled)
pct_23_19_divert <- pct_vs_baseline(df_divert, 2023, 2019, n_diverted)
pct_23_19_operated <- pct_vs_baseline(df_operated, 2023, 2019, n_operated)

pct_23_19_ontime <- pct_vs_baseline(df_ontime_delay, 2023, 2019, n_ontime)
pct_23_19_delayed <- pct_vs_baseline(df_ontime_delay, 2023, 2019, n_delayed)

total_flights <- nrow(df_flights)
total_airlines <- n_distinct(df_flights$AIRLINE)
total_airports <- n_distinct(df_flights$ORIGIN)
total_routes <- nrow(df_flights |>
                       distinct(ORIGIN, DEST))

total_flights_fmt <- format_compact(total_flights)

cat(
  "U.S. Flight Operations: 2019–2023\n",
  "Flights volume vs 2019: ", pct_23_19_flight, "\n",
  "Cancelled vs 2019: ", pct_23_19_cancel, "\n",
  "Diverted vs 2019: ", pct_23_19_divert, "\n",
  "Operated vs 2019: ", pct_23_19_operated, "\n",
  "  - On-time vs 2019: ", pct_23_19_ontime, "\n",
  "  - Delayed vs 2019: ", pct_23_19_delayed, "\n",
  "Total flights: ", total_flights_fmt, "\n",
  "Total airlines: ", total_airlines, "\n",
  "Total airports: ", total_airports, "\n",
  "Total routes: ", total_routes, "\n"
)

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

fig_1 <- plot_ly(
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

fig_1

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

fig_2 <- plot_ly(
  data = df_air_rank
) |>
  add_bars(
    x = ~n_flights,
    y = ~AIRLINE,
    orientation = "h",
    text = ~text,
    textposition = "outside",
    marker = list(color = "#59cf5a"),
    opacity = 0.8
  ) |>
  layout(
    yaxis = list(
      title = "",
      autorange = "reversed"
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

fig_2

#==================================#
# Chart: Monthly Departures
#==================================#
df_monthly_departures <- df_flights |>
  group_by(MONTH) |>
  summarise(n_flights = n(), .groups = "drop") |>
  arrange(MONTH) |>
  mutate(text = sapply(n_flights, format_compact))

fig_3 <- plot_ly(
  df_monthly_departures,
  x = ~MONTH, y = ~n_flights,
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

fig_3

#==================================#
# Chart: Flight Distance Distribution
#==================================#
df_distance_departures <- df_flights |>
  group_by(DISTANCE_CAT) |>
  summarise(n_flights = n(), .groups = "drop") |>
  arrange(DISTANCE_CAT)

fig_4 <- plot_ly(
  data = df_distance_departures
) |>
  add_bars(
    x = ~DISTANCE_CAT,
    y = ~n_flights,
    color = ~DISTANCE_CAT,
    colors = c("#f76af7", "#e8b76d", "#51d6b3"),
    opacity = 0.8
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

fig_4

#==================================#
# Chart: National Coverage
#==================================#
df_flights <- df_flights |>
  mutate(
    ORIGIN_AIRPORT = paste0(ORIGIN_CITY, " (", ORIGIN, ")")
  )

df_origin_airport <- df_flights |>
  filter(!is.na(ORIGIN_LAT) & !is.na(ORIGIN_LON)) |>   # loại bỏ dòng NA
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
    zoom = 3
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
 
#==================================#
# 3.2 Operational Performance
# 3.2.1 Overview + With/Without Filter
# On-time rate = Number of on-time flights / Total number of operated flights * 100
# Delay rate = Number of delayed flights / Total number of operated flights * 100

# This is often simply "100% - Ontime Rate - Canceled Rate - Divert Rate",
# as every flight falls into one of these categories. The average delay time in
# minutes is another common metric.

# Note: Canceled and diverted flights are typically excluded from the on-time
# performance calculation but included in the total pool of flight for overall performance metrics.

# Cancelled rate = Number of canceled flights / Total number of scheduled flights * 100
# Divert rate = Number of diverted flights / Total number of scheduled flights * 100
#==================================#
filter_by_year <- function(df, year) {
  if (!is.null(year)) {
    df <- filter(df, YEAR == year)
  }
  return(df)
}

filter_by_airline <- function(df, airline) {
  if (!is.null(airline)) {
    df <- filter(df, AIRLINE == airline)
  }
  return(df)
}

filter_by_origin_airport <- function(df, airport) {
  if (!is.null(airport)) {
    df <- filter(df, ORIGIN == airport)
  }
  return(df)
}

filter_by_season <- function(df, season) {
  if (!is.null(season)) {
    df <- filter(df, SEASON == season)
  }
  return(df)
}

#==================================#
# On-time Performance & Delays
#==================================#
ontime_delay_summary <- function(df, year = NULL, airline = NULL) {

  # Filter theo điều kiện (nếu có)
  if (!is.null(year)) {
    df <- df |> filter(YEAR == year)
  }

  if (!is.null(airline)) {
    df <- df |> filter(AIRLINE == airline)
  }

  total_flights <- nrow(df)

  # Cancelled & Diverted
  total_cancelled <- df |>
    filter(CANCELLED == 1) |> nrow()

  total_diverted <- df |>
    filter(DIVERTED == 1) |> nrow()

  # Operated flights
  total_operated <- total_flights - total_cancelled - total_diverted

  # On-time flights (<= 15 mins)
  total_on_time <- df |>
    filter(
      DEP_DELAY <= 15,
      CANCELLED == 0,
      DIVERTED == 0
    ) |> nrow()

  total_delayed <- total_operated - total_on_time

  # Tỷ lệ %
  pct_cancelled <- round(total_cancelled / total_flights * 100, 2)
  pct_diverted <- round(total_diverted / total_flights * 100, 2)
  pct_operated <- round(total_operated / total_flights * 100, 2)
  pct_on_time <- round(total_on_time / total_operated * 100, 2)
  pct_delayed <- round(total_delayed / total_operated * 100, 2)

  # Output
  cat("On-time Performance & Delays\n")

  if (is.null(year) && is.null(airline)) {
    cat("Years: 2019–2023 | Airlines: All airlines\n\n")
  } else {
    cat("Years:", year, "| Airlines:", airline, "\n\n")
  }
  
  total_flights_fmt <- format_compact(total_flights)
  
  cat("Total flights:", total_flights_fmt, "\n")
  cat("Cancelled:", pct_cancelled, "%\n")
  cat("Diverted:", pct_diverted, "%\n")
  cat("Operated flights:", pct_operated, "%\n")
  cat("  - On-time:", pct_on_time, "% of operated\n")
  cat("  - Delayed:", pct_delayed, "% of operated\n")
}

ontime_delay_summary(df_flights)
ontime_delay_summary(df_flights, year = 2019)
ontime_delay_summary(df_flights, year = 2019, airline = "United Air Lines Inc.")

#==================================#
# Chart: MONTHLY DISTRIBUTION OF ON-TIME & DELAYED FLIGHTS
#==================================#
monthly_ontime_delay <- function(df, year = NULL, airline = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  df_monthly <- ontime_delay_by(df, group_cols = "MONTH")
  
  fig <- plot_ly(
    data = df_monthly,
    x = ~MONTH, y = ~n_ontime,
    type = "bar",
    name = "On-time",
    marker = list(color = "#61c9c4")
  ) |>
    add_trace(
      y = ~n_delayed,
      name = "Delayed",
      marker = list(color = "#c3d15c")
    ) |>
    layout(
      barmode = "stack",
      showlegend = FALSE,
      xaxis = list(
        title = "Month",
        dtick = 1
      ),
      yaxis = list(
        title = "Number of operated flights"
      ),
      title = list(
        text = paste0(
          "MONTHLY DISTRIBUTION OF ON-TIME & DELAYED FLIGHTS",
          "<br><span style='font-size:14px;color:#666;font-style:italic'>",
          "Operated Flight Volume</span>"
        ),
        x = 0.5,
        xanchor = "center",
        font = list(size = 16, color = "#333", family = "Comic Sans MS")
      ),
      margin = list(l = 10, r = 10, t = 70, b = 10),
      paper_bgcolor = "#f5f5f5",
      plot_bgcolor = "#f5f5f5"
    )|>
    config(responsive = TRUE)
  fig
}

monthly_ontime_delay(df_flights)
monthly_ontime_delay(df_flights, year = 2019)
monthly_ontime_delay(df_flights, year = 2019, airline = "United Air Lines Inc.")

#==================================#
# Chart: MONTHLY FLIGHT VOLUME VS AVERAGE DELAY
#==================================#
monthly_volume_delay <- function(df, year = NULL, airline = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  df_monthly_perf <- ontime_delay_by(df, group_cols = "MONTH") |>
    select(MONTH, n_operated, avg_delay) |>
    mutate(
      text = format_compact(n_operated),
      avg_delay = round(avg_delay, 1)
    )
  
  fig <- plot_ly()
  
  # --- Bar: number of flights ---
  fig <- fig |>
    add_bars(
      data = df_monthly_perf,
      x = ~MONTH, y = ~n_operated,
      name = "Number of Flights",
      yaxis = "y",
      text = ~text,
      textposition = "outside",
      opacity = 0.65,
      marker = list(color = "#D97706"),
      hovertemplate = paste(
        "<b>Month:</b> %{x}<br>",
        "<b>Flights:</b> %{y:,}<br>",
        "<extra></extra>"
      )
    )
  
  # --- Line: average delay ---
  fig <- fig |>
    add_trace(
      data = df_monthly_perf,
      x = ~MONTH, y = ~avg_delay,
      name = "Avg Departure Delay (min)",
      type = "scatter",
      mode = "lines+markers",
      yaxis = "y2",
      line = list(color = "#2563EB", width = 2, dash = "dash"),
      marker = list(size = 8)
    )
  
  fig <- fig |>
    layout(
      xaxis = list(
        type = "category",
        showgrid = FALSE
      ),
      yaxis = list(
        title = "Number of Flights",
        showgrid = FALSE
      ),
      yaxis2 = list(
        title = "Avg Delay (minutes)",
        overlaying = "y",
        side = "right",
        showgrid = FALSE
      ),
      showlegend = FALSE,
      title = list(
        text = "MONTHLY FLIGHT VOLUME VS AVERAGE DELAY",
        x = 0.5,
        xanchor = "center",
        font = list(size = 16, color = "#333", family = "Comic Sans MS")
      ),
      margin = list(l = 10, r = 10, t = 70, b = 10),
      paper_bgcolor = "#f5f5f5",
      plot_bgcolor = "#f5f5f5"
    )
  fig
}

monthly_volume_delay(df_flights)
monthly_volume_delay(df_flights, year = 2019)
monthly_volume_delay(df_flights, year = 2019, airline = "United Air Lines Inc.")

#==================================#
# Chart: DELAY CAUSES
#==================================#
delay_causes <- function(df, year = NULL, airline = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  delay_cols <- c(
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
  )
  
  cause_map <- c(
    "DELAY_DUE_LATE_AIRCRAFT" = "Late Aircraft",
    "DELAY_DUE_NAS" = "NAS",
    "DELAY_DUE_CARRIER" = "Carrier",
    "DELAY_DUE_WEATHER" = "Weather",
    "DELAY_DUE_SECURITY" = "Security"
  )
  
  df_delay_causes <- df |>
    select(all_of(delay_cols)) |>
    summarise(across(everything(), \(x) sum(x, na.rm = TRUE))) |>
    pivot_longer(
      everything(),
      names_to = "cause",
      values_to = "delay_minutes"
    ) |>
    mutate(
      pct = delay_minutes / sum(delay_minutes) * 100,
      label = cause_map[cause],
      n_blocks = round(pct)
    )
  
  # ---- Fix rounding so total = 100 ----
  diff <- 100 - sum(df_delay_causes$n_blocks)
  if (diff != 0) {
    idx <- which.max(df_delay_causes$n_blocks)
    df_delay_causes$n_blocks[idx] <- df_delay_causes$n_blocks[idx] + diff
  }
  
  df_delay_causes <- df_delay_causes |>
    arrange(desc(pct))
  
  # ---- Build waffle layout ----
  waffle <- unlist(
    mapply(
      function(label, n) rep(label, n),
      df_delay_causes$label,
      df_delay_causes$n_blocks,
      SIMPLIFY = FALSE
    )
  )
  
  rows <- 5
  cols <- 20
  x <- rep(seq_len(cols) - 1, times = rows)
  y <- rep(rev(seq_len(rows) - 1), each = cols)
  
  color_map <- c(
    "Late Aircraft" = "#FDBA74",
    "NAS" = "#93C5FD",
    "Carrier" = "#86EFAC",
    "Weather" = "#FCA5A5",
    "Security" = "#D1D5DB"
  )
  
  fig <- plot_ly(height = 320)
  
  for (cause in names(color_map)) {
    
    mask <- waffle == cause
    pct <- df_delay_causes$pct[df_delay_causes$label == cause][1]
    
    fig <- fig |>
      add_trace(
        x = x[mask], y = y[mask],
        type = "scatter",
        mode = "markers",
        name = cause,
        marker = list(
          size = 22,
          symbol = "square",
          color = color_map[[cause]],
          line = list(width = 1, color = "#f5f5f5")
        ),
        hovertemplate = paste0(
          "<b>", cause, "</b><br>",
          sprintf("%.1f", pct), "% of total delay<br>",
          "<extra></extra>"
        )
      )
  }
  
  fig <- fig |>
    layout(
      title = list(
        text = paste0(
          "Flight Delay Breakdown by Causes",
          "<br><span style='font-size:14px;color:#666;font-style:italic'>",
          "Share of Total Delay Minutes</span>"
        ),
        x = 0.5,
        xanchor = "center"
      ),
      legend = list(
        orientation = "h",
        yanchor = "top", y = 1.2,
        xanchor = "center", x = 0.5,
        font = list(size = 12)
      ),
      margin = list(l = 20, r = 20, t = 140, b = 20),
      xaxis = list(visible = FALSE, range = c(-0.5, 19.5)),
      yaxis = list(visible = FALSE, range = c(-0.5, 4.5)),
      paper_bgcolor = "#f5f5f5",
      plot_bgcolor = "#f5f5f5",
      showlegend = TRUE
    )|>
    config(responsive = TRUE)
  fig
}

delay_causes(df_flights)
delay_causes(df_flights, year = 2019)
delay_causes(df_flights, year = 2019, airline = "United Air Lines Inc.")

#==================================#
# 3.2.2 Local Patterns + With/Without Filter
# Summary
#==================================#
local_patterns <- function(df, year = NULL, airline = NULL,
                           origin_airport = NULL, season = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  if (!is.null(origin_airport)) {
    df <- filter_by_origin_airport(df, origin_airport)
  }
  
  if (!is.null(season)) {
    df <- filter_by_season(df, season)
  }
  
  total_flights <- nrow(df)
  
  df_operated <- df |>
    filter(CANCELLED == 0, DIVERTED == 0)
  
  avg_dep <- df_operated |>
    summarise(avg = mean(DEP_DELAY, na.rm = TRUE)) |>
    pull(avg) |>
    round(2)
  
  avg_arr <- df_operated |>
    summarise(avg = mean(ARR_DELAY, na.rm = TRUE)) |>
    pull(avg) |>
    round(2)
  
  total_flights_fmt <- format_compact(total_flights)
  
  cat("Total flights:", total_flights_fmt, "\n")
  cat("Dep Delay:", avg_dep, "minutes\n")
  cat("Arr Delay:", avg_arr, "minutes\n")
}

local_patterns(df_flights)
local_patterns(df_flights, year = 2019)
local_patterns(df_flights, year = 2019, airline = "United Air Lines Inc.")
local_patterns(df_flights, year = 2019, airline = "United Air Lines Inc.", 
               origin_airport = "DEN")
local_patterns(df_flights, year = 2019, airline = "United Air Lines Inc.", 
               origin_airport = "DEN", season = "Summer")

#==================================#
# Table: AIRPORT DELAY STABILITY
#==================================#
airport_delay_stability <- function(df, min_flights = 1000, year = NULL,
                                    airline = NULL, season = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  if (!is.null(season)) {
    df <- filter_by_season(df, season)
  }
  
  df_airport <- df |>
    filter(CANCELLED == 0, DIVERTED == 0) |>
    group_by(ORIGIN) |>
    summarise(
      n_flights = n(),
      avg_arr_delay = mean(ARR_DELAY, na.rm = TRUE),
      std_arr_delay = sd(ARR_DELAY, na.rm = TRUE),
      .groups = "drop"
    ) |>
    filter(n_flights >= min_flights) |>
    arrange(desc(std_arr_delay))
  
  return(df_airport)
}

df_stability = airport_delay_stability(df_flights, year = 2019)
head(df_stability, 10)

#==================================#
# Table: ROUTING RANKING
#==================================#
routing_ranking <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  if (!is.null(year)) {
    df <- filter_by_year(df, year)
  }
  
  if (!is.null(airline)) {
    df <- filter_by_airline(df, airline)
  }
  
  if (!is.null(season)) {
    df <- filter_by_season(df, season)
  }
  
  # lọc flight hợp lệ
  df <- filter(
    df,
    CANCELLED == 0,
    DIVERTED == 0
  )
  
  # group & summarize
  result <- df |>
    group_by(ROUTE) |>
    summarise(
      n_flights = n(),
      avg_arr_delay = mean(ARR_DELAY, na.rm = TRUE),
      .groups = "drop"
    ) |>
    arrange(desc(n_flights))
  
  return(result)
}

df_route = routing_ranking(df_flights, year = 2019, airline = "United Air Lines Inc.")
head(df_route, 10)

#==================================#
# Chart: TIME OF DAY 
# --> AVG ARRIVAL DELAY BY DEPARTURE HOUR
#==================================#
time_of_day <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  data <- df
  
  if (!is.null(year)) {
    data <- filter_by_year(data, year)
  }
  
  if (!is.null(airline)) {
    data <- filter_by_airline(data, airline)
  }
  
  if (!is.null(season)) {
    data <- filter_by_season(data, season)
  }
  
  # ---- Aggregate ----
  delay_avg_hourly <- data |>
    group_by(YEAR, DEP_HOUR) |>
    summarise(
      Avg_Departure_Delay = round(mean(DEP_DELAY, na.rm = TRUE), 1),
      .groups = "drop"
    ) |>
    arrange(YEAR, DEP_HOUR)
  
  # ---- Plot ----
  fig <- plot_ly(
    data = delay_avg_hourly,
    x = ~DEP_HOUR,
    y = ~Avg_Departure_Delay,
    color = ~as.factor(YEAR),
    type = "scatter",
    mode = "lines+markers",
    line = list(shape = "hv")  # step-like
  )
  
  fig <- fig |>
    layout(
      showlegend = FALSE,
      xaxis = list(
        dtick = 1,
        type = "category"
      ),
      yaxis = list(title = NULL),
      title = list(
        text = "AVERAGE DEPARTURE DELAY BY TIME OF DAY",
        x = 0.5
      ),
      margin = list(l = 10, r = 10, t = 70, b = 10),
      paper_bgcolor = "#f5f5f5",
      plot_bgcolor = "#f5f5f5",
      title = list(
        text = "AVERAGE DEPARTURE DELAY BY TIME OF DAY",
        x = 0.5,
        font = list(
          size = 16,
          color = "#333",
          family = "Comic Sans MS"
        )
      )
    ) |>
    config(responsive = TRUE)
  fig
}

time_of_day(df_flights, year = 2019, airline = "United Air Lines Inc.")

#==================================#
# Map: Arrival delay by airport
#==================================#
arrival_delay_folium <- function(
    df,
    year = NULL,
    airline = NULL,
    season = NULL,
    volume_quantile = 0.7,
    tiles = "CartoDB.Positron"
) {
  
  if (!is.null(year)) df <- filter_by_year(df, year)
  if (!is.null(airline)) df <- filter_by_airline(df, airline)
  if (!is.null(season)) df <- filter_by_season(df, season)
  
  df <- df |>
    filter(
      CANCELLED == 0,
      DIVERTED == 0,
      !is.na(ARR_DELAY),
      !is.na(DEST_LAT),
      !is.na(DEST_LON)
    )
  
  airport_perf <- df |>
    group_by(DEST, DEST_LAT, DEST_LON) |>
    summarise(
      n_flights = n(),
      med_arr_delay = round(median(ARR_DELAY), 1),
      .groups = "drop"
    )
  
  q <- quantile(airport_perf$n_flights, volume_quantile)
  airport_perf <- airport_perf |> filter(n_flights >= q)
  
  pal <- colorNumeric(
    palette = c("yellow", "blue", "red"),
    domain = airport_perf$med_arr_delay
  )
  
  radius_fun <- function(x) scales::rescale(x, to = c(6, 14))
  
  leaflet(airport_perf) |>
    addProviderTiles(tiles) |>
    
    addCircleMarkers(
      lng = ~DEST_LON,
      lat = ~DEST_LAT,
      radius = ~radius_fun(n_flights),
      fillColor = ~pal(med_arr_delay),
      fillOpacity = 0.75,
      color = NA,
      
      popup = ~paste0(
        "<b>", DEST, "</b><br>",
        "Median delay: ", med_arr_delay, " min<br>",
        "Flights: ", format(n_flights, big.mark = ",")
      ),
      
      clusterOptions = markerClusterOptions(
        disableClusteringAtZoom = 5,
        maxClusterRadius = 60,
        spiderfyOnMaxZoom = TRUE
      )
    )
}

m = arrival_delay_folium(df_flights, year = 2019, season="Winter")
m

#==================================#
# 3.2.3 Local Patterns + With/Without Filter
# - Performance is evaluated relative to peer airlines under the selected filters.
# - Results reflect comparative delay behavior rather than overall service quality.
# Chart: INFLUENCE OF VARIOUS DELAYS
#==================================#
influence_of_delays <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  delay_cols <- c(
    "DEP_DELAY",
    "ARR_DELAY",
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
  )
  
  if (!is.null(year)) 
    df <- filter_by_year(df, year)
  
  if (!is.null(airline)) 
    df <- filter_by_airline(df, airline)
  
  if (!is.null(season)) 
    df <- filter_by_season(df, season)
  
  df <- df |> filter(DEP_DELAY > 0)
  
  values <- df |>
    select(all_of(delay_cols)) |>
    summarise(across(everything(), \(x) sum(x, na.rm = TRUE))) |>
    unlist(use.names = FALSE)
  
  rel <- values / max(values, na.rm = TRUE)
  z <- scales::rescale(rel, to = c(0, 1))
  
  # ---- màu Spectral giống matplotlib ----
  pal <- grDevices::colorRampPalette(
    RColorBrewer::brewer.pal(11, "Spectral")
  )(100)
  
  col_idx <- round(z * 99) + 1
  cell_colors <- pal[col_idx]
  
  rgb <- grDevices::col2rgb(cell_colors) / 255
  luminance <- 0.299 * rgb[1, ] + 0.587 * rgb[2, ] + 0.114 * rgb[3, ]
  
  text_color <- ifelse(luminance < 0.5, "white", "black")
  
  plot_df <- data.frame(
    y = factor(delay_cols, levels = rev(delay_cols)),
    x = " ",
    z = z,
    label = sprintf("%.2f", rel),
    text_color = text_color
  )
  
  title_parts <- c("Influence of Delay Factors")
  if (!is.null(year)) title_parts <- c(title_parts, paste("Year:", year))
  if (!is.null(airline)) title_parts <- c(title_parts, paste("Airline:", airline))
  if (!is.null(season)) title_parts <- c(title_parts, paste("Season:", season))
  
  fig <- plot_ly()
  
  # ---- HEATMAP (chỉ heatmap có z) ----
  fig <- fig |>
    add_heatmap(
      data = plot_df,
      x = ~x, y = ~y, z = ~z,
      colorscale = "Spectral",
      zmin = 0, zmax = 1,
      showscale = TRUE
    )
  
  # ---- TEXT TRẮNG ----
  fig <- fig |>
    add_trace(
      data = subset(plot_df, text_color == "white"),
      x = ~x, y = ~y,
      text = ~label,
      type = "scatter",
      mode = "text",
      textfont = list(color = "white", size = 12),
      showlegend = FALSE
    )
  
  # ---- TEXT ĐEN ----
  fig <- fig |>
    add_trace(
      data = subset(plot_df, text_color == "black"),
      x = ~x, y = ~y,
      text = ~label,
      type = "scatter",
      mode = "text",
      textfont = list(color = "black", size = 12),
      showlegend = FALSE
    )
  
  fig <- fig |>
    layout(
      title = paste(title_parts, collapse = " | "),
      xaxis = list(showticklabels = FALSE),
      yaxis = list(title = ""),
      margin = list(l = 120, r = 40, t = 60, b = 40)
    ) |>
    config(responsive = TRUE)
  fig
}

influence_of_delays(df_flights, year = 2019)

#==================================#
# Chart: Marginal Effect of Delay Components on Arrival Delay
#==================================#
delay_factor_interaction <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  delay_cols <- c(
    "DEP_DELAY",
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
  )
  
  if (!is.null(year)) df <- filter_by_year(df, year)
  if (!is.null(airline)) df <- filter_by_airline(df, airline)
  if (!is.null(season)) df <- filter_by_season(df, season)
  
  long_df <- df |>
    pivot_longer(
      cols = all_of(delay_cols),
      names_to = "factor",
      values_to = "delay_value"
    ) |>
    mutate(
      delay_bin = case_when(
        delay_value <= 0  ~ "0",
        delay_value <= 5  ~ "1–5",
        delay_value <= 15 ~ "5–15",
        delay_value <= 30 ~ "15–30",
        TRUE              ~ ">30"
      )
    )
  
  summary <- long_df |>
    group_by(factor, delay_bin) |>
    summarise(
      mean_arr_delay = mean(ARR_DELAY, na.rm = TRUE),
      n_flights = n(),
      .groups = "drop"
    ) |>
    filter(n_flights > 1000)
  
  summary$delay_bin <- factor(
    summary$delay_bin,
    levels = c("0", "1–5", "5–15", "15–30", ">30")
  )
  
  factors <- unique(summary$factor)
  
  plots <- lapply(factors, function(f) {
    df_f <- summary |> filter(factor == f)
    
    plot_ly(
      df_f,
      x = ~delay_bin, y = ~mean_arr_delay,
      type = "scatter",
      mode = "lines+markers",
      showlegend = FALSE
    )
  })
  
  n_cols <- 3
  n_rows <- ceiling(length(plots) / n_cols)
  
  fig <- subplot(
    plots,
    nrows = n_rows,
    shareX = TRUE,
    shareY = TRUE,
    titleX = TRUE,
    titleY = TRUE
  )
  
  # ---- thêm tiêu đề cho từng facet ----
  annotations <- lapply(seq_along(factors), function(i) {
    list(
      text = factors[i],
      x = ((i - 1) %% n_cols + 0.5) / n_cols,
      y = 1 - (floor((i - 1) / n_cols) / n_rows),
      xref = "paper",
      yref = "paper",
      showarrow = FALSE,
      font = list(size = 13)
    )
  })
  
  fig <- fig |>
    layout(
      title = "Marginal Effect of Delay Components on Arrival Delay",
      annotations = annotations,
      margin = list(t = 90)
    ) |>
    config(responsive = TRUE)
  fig
}

delay_factor_interaction(df_flights, year = 2019, season = "Spring")

#==================================#
# Chart: Reliability vs Performance of Delay Factors
#==================================#
reliability_vs_performance <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  delay_cols <- c(
    "DEP_DELAY",
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
  )
  
  if (!is.null(year)) df <- filter_by_year(df, year)
  if (!is.null(airline)) df <- filter_by_airline(df, airline)
  if (!is.null(season)) df <- filter_by_season(df, season)
  
  long_df <- df |>
    pivot_longer(
      cols = all_of(delay_cols),
      names_to = "factor",
      values_to = "delay_value"
    ) |>
    filter(delay_value > 0)
  
  summary <- long_df |>
    group_by(factor) |>
    summarise(
      mean_arr_delay = mean(ARR_DELAY, na.rm = TRUE),
      std_arr_delay  = sd(ARR_DELAY, na.rm = TRUE),
      n_flights      = n(),
      .groups = "drop"
    ) |>
    filter(n_flights > 200)
  
  fig <- plot_ly(
    data = summary,
    x = ~mean_arr_delay,
    y = ~std_arr_delay,
    type = "scatter",
    mode = "markers",
    size = ~n_flights,
    color = ~factor,
    sizes = c(10, 50),
    marker = list(opacity = 0.75),
    text = ~paste("Factor:", factor,
                  "<br>Mean:", round(mean_arr_delay,1),
                  "<br>Std:", round(std_arr_delay,1),
                  "<br>N flights:", n_flights),
    hoverinfo = "text"
  )
  
  fig <- layout(
    fig,
    title = list(text = "Reliability vs Performance of Delay Factors"),
    xaxis = list(title = "Mean Arrival Delay (minutes)"),
    yaxis = list(title = "Variability of Arrival Delay (Std Dev)"),
    legend = list(title = list(text = "Delay Factor"))
  )
  fig <- config(fig, responsive = TRUE)
}

reliability_vs_performance(df_flights, season = "Spring")

#==================================#
# 3.2.4 Discruption + With/Without Filter
# Summary
#==================================#
disruption_metrics <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  if (!is.null(year)) 
    df <- filter_by_year(df, year)
  
  if (!is.null(airline)) 
    df <- filter_by_airline(df, airline)
  
  if (!is.null(season)) 
    df <- filter_by_season(df, season)
  
  total_flights <- nrow(df)
  
  summary <- df |>
    summarise(
      cancelled = sum(CANCELLED, na.rm = TRUE),
      diverted  = sum(DIVERTED, na.rm = TRUE)
    ) |>
    mutate(
      disrupted     = cancelled + diverted,
      total_flights = total_flights
    )
  
  cancelled <- summary$cancelled
  diverted  <- summary$diverted
  disrupted <- summary$disrupted
  
  cancel_rate <- round(cancelled / total_flights * 100, 2)
  divert_rate <- round(diverted / total_flights * 100, 2)
  disruption_rate <- round(disrupted / total_flights * 100, 2)
  
  # ---- in kết quả ----
  total_flights_fmt <- format(total_flights, big.mark = ",")
  
  cat("📊 Disruption Summary\n")
  cat(strrep("-", 30), "\n")
  cat("Total flights:", total_flights_fmt, "\n")
  cat("Cancelled flights:", cancel_rate, "%\n")
  cat("Diverted flights:", divert_rate, "%\n")
  cat("Total disrupted:", disruption_rate, "%\n")
}

disruption_metrics(df_flights, season="Winter")

#==================================#
# Chart: Flight Disruption Rate Over Time (Cancelled vs Diverted)
#==================================#
plot_disruption_bar <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  if (!is.null(year)) 
    df <- df |> filter(YEAR %in% year)
  
  if (!is.null(airline)) 
    df <- df |> filter(AIRLINE %in% airline)
  
  if (!is.null(season)) 
    df <- df |> filter(SEASON %in% season)
  
  df_disruption <- df |>
    mutate(
      DISRUPTION_TYPE = case_when(
        CANCELLED == 1 ~ "Cancelled",
        DIVERTED == 1  ~ "Diverted",
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(DISRUPTION_TYPE))
  
  df_grouped <- df_disruption |>
    group_by(YEAR, SEASON, DISRUPTION_TYPE) |>
    summarise(COUNT = n(), .groups = "drop")
  
  total_per_year <- df |>
    group_by(YEAR) |>
    summarise(TOTAL = n(), .groups = "drop")
  
  df_grouped <- df_grouped |>
    left_join(total_per_year, by = "YEAR") |>
    mutate(RATE = COUNT / TOTAL * 100)
  
  p <- ggplot(
    df_grouped,
    aes(x = YEAR, y = RATE, fill = DISRUPTION_TYPE)
  ) +
    geom_col(position = "stack") +
    facet_wrap(~ SEASON, ncol = 2) +
    scale_y_continuous(labels = scales::percent_format(scale = 1)) +
    labs(
      title = "Flight Disruption Rate by Season",
      x = "Year",
      y = "Disruption Rate (%)",
      fill = "Disruption Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      legend.position = "bottom",
      strip.text = element_text(face = "bold"),
      plot.title = element_text(hjust = 0.5)
    )
  
  # ---------------- Convert to plotly ----------------
  ggplotly(p, tooltip = c("x", "y", "fill")) |>
    config(responsive = TRUE)
}

plot_disruption_bar(df_flights)

#==================================#
# Chart: Share of Disruption Causes
#==================================#
plot_cause_donut <- function(df, year = NULL, airline = NULL, season = NULL) {
  
  if (!is.null(year)) df <- filter_by_year(df, year)
  if (!is.null(airline)) df <- filter_by_airline(df, airline)
  if (!is.null(season)) df <- filter_by_season(df, season)
  
  causes <- c(
    "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER",
    "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
  )
  
  cause_labels <- c(
    DELAY_DUE_CARRIER = "Delay due Carrier",
    DELAY_DUE_WEATHER = "Delay due Weather",
    DELAY_DUE_NAS = "Delay due NAS",
    DELAY_DUE_SECURITY = "Delay due Security",
    DELAY_DUE_LATE_AIRCRAFT = "Delay due Late Aircraft"
  )
  
  df_long <- df |>
    select(all_of(causes)) |>
    pivot_longer(
      cols = everything(),
      names_to = "CAUSE",
      values_to = "DELAY_MIN"
    ) |>
    filter(DELAY_MIN > 0)
  
  df_grouped <- df_long |>
    group_by(CAUSE) |>
    summarise(TOTAL_DELAY = sum(DELAY_MIN), .groups = "drop") |>
    mutate(
      RATE = TOTAL_DELAY / sum(TOTAL_DELAY) * 100,
      CAUSE_LABEL = cause_labels[CAUSE]
    ) |>
    arrange(desc(RATE))
  
  # highlight phần lớn nhất
  pull <- rep(0, nrow(df_grouped))
  pull[which.max(df_grouped$RATE)] <- 0.1
  
  fig <- plot_ly(
    data = df_grouped,
    labels = ~CAUSE_LABEL,
    values = ~RATE,
    type = "pie",
    hole = 0.5,
    pull = pull,
    textinfo = "label+percent",
    hovertemplate = "%{label}: %{value:.2f}%<extra></extra>"
  )
  
  fig <- fig |>
    layout(
      title = list(text = "Share of Disruption Causes"),
      showlegend = FALSE
    ) |>
    config(responsive = TRUE)
  fig
}

plot_cause_donut(df_flights, season="Winter")  

