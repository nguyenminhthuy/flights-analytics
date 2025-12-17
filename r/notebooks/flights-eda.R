library(arrow)
library(fs)
library(tidyverse)
library(dplyr)

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





