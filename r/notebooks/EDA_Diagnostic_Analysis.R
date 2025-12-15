library(tidyverse)
library(tidyr)
library(dplyr)
library(ggplot2)
library(readr)
library(data.table)
library(plotly)
library(lubridate)
library(hexbin) # Hexbin plot (nhẹ hơn rất nhiề so vs scatter)

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

df_flights <- copy(df)

############################################
# DIAGNOSTIC ANALYSIS
############################################
# Trong các loại delay, loại nào chiếm nhiều thời gian nhất
# (carrier / weather / NAS / late aircraft / security)?
#-------------------------------------------
# Lấy danh sách cột delay
reason_cols <- grep("^DELAY_DUE_", names(df_flights), value = TRUE)

# Tính trung bình delay theo năm
delay_reason_yearly <- df_flights |>
  group_by(YEAR) |>
  summarise(across(all_of(reason_cols), 
                   ~ round(mean(.x, na.rm = TRUE), 1)))

# Convert sang dạng long để plotly stacked dùng tốt hơn
delay_long <- delay_reason_yearly |>
  pivot_longer(
    cols = all_of(reason_cols),
    names_to = "Reason", values_to = "Delay"
  )

# Vẽ stacked bar
plot_ly(delay_long,
        x = ~YEAR, y = ~Delay,
        color = ~Reason,
        type = "bar") |>
  layout(
    barmode = "stack",
    title = "Total Delay Minutes by Cause and Year",
    xaxis = list(type = "category", title = "Year"),
    yaxis = list(title = "Total Delay (minutes)"),
    legend = list(title = list(text = "Delay Cause"))
  )

#-------------------------------------------
# Delay do “Late Aircraft” có mối liên hệ mạnh với ARR_DELAY tổng không?
#-------------------------------------------
late_aircraft_df <- df_flights |>
  select(ARR_DELAY, DELAY_DUE_LATE_AIRCRAFT) |>
  drop_na()

late_aircraft_corr <- cor(late_aircraft_df$ARR_DELAY, 
                          late_aircraft_df$DELAY_DUE_LATE_AIRCRAFT)

corr_review <- paste(
  "Nhận xét:",
  "- Late Aircraft Delay tăng → Arrival Delay cũng tăng",
  "- corr ~ 0.504 -> trung bình - khá",
  "- R-squared ≈ corr^2 = 0.504^2 ≈ 0.254",
  "- => Khoảng 25% biến động Arrival Delay đến từ Late Aircraft Delay.\n",
  sep = "\n"
)
cat(corr_review)

# plot
fig_2 <- plot_ly(
  data = late_aircraft_df,
  x = ~DELAY_DUE_LATE_AIRCRAFT,
  y = ~ARR_DELAY,
  type = "scattergl", mode = "markers",
  opacity = 0.7, marker = list(color="tomato")
) |>
  layout(
    title = "Relationship: Late Aircraft Delay vs ARR_DELAY",
    xaxis = list(title = "Late Aircraft Delay (minutes)"),
    yaxis = list(
      title = "Arrival Delay (minutes)",
      tickformat = ",",     # Format y-axis có dấu phẩy
      separatethousands = TRUE
    )
  )

fig_2

#-------------------------------------------
# Tỉ lệ delay do thời tiết khác nhau giữa các tháng như thế nào?
#-------------------------------------------
delayed_df <- df_flights |>
  filter(ARR_DELAY > 15) |>
  group_by(YEAR, MONTH) |>
  summarise(Total_Flights = n(), .groups = "drop")

# --- Tổng số chuyến bay delay do weather ---
weather_df <- df_flights |>
  filter(DELAY_DUE_WEATHER > 0) |>
  group_by(YEAR, MONTH) |>
  summarise(Total_Flights_Weather = n(), .groups = "drop")

# --- Merge ---
final_df <- delayed_df %>%
  left_join(weather_df, by = c("YEAR", "MONTH")) %>%
  mutate(
    Total_Flights_Weather = replace_na(Total_Flights_Weather, 0),
    Delay_Rate = round(Total_Flights_Weather / Total_Flights * 100, 2)
  )

# Pivot cho heatmap
pivot <- final_df |>
  select(YEAR, MONTH, Delay_Rate) |>
  pivot_wider(
    names_from = YEAR,
    values_from = Delay_Rate
  ) |>
  arrange(MONTH)

# Convert sang matrix và xác định labels
mat <- as.matrix(pivot[,-1])
rownames(mat) <- pivot$MONTH

# --- Plotly Heatmap ---
fig_3 <- plot_ly(
  x = colnames(mat),
  y = rownames(mat),
  z = mat,
  type = "heatmap",
  colorscale = "YlOrRd",
  showscale = TRUE,
  hovertemplate = "YEAR: %{x}<br>MONTH: %{y}<br>Rate: %{z:.1f}%<extra></extra>"
) |>
  layout(
    title = "Weather Delay Rate (%)",
    xaxis = list(title = "YEAR"),
    yaxis = list(title = "MONTH")
  )

fig_3

#-------------------------------------------
late_weather_df <- df_flights |>
  select(ARR_DELAY, DELAY_DUE_WEATHER) |>
  drop_na()

late_weather_corr <- cor(late_weather_df$ARR_DELAY,
                         late_weather_df$DELAY_DUE_WEATHER)

fig_4 <- plot_ly(
  data = late_weather_df,
  x = ~DELAY_DUE_WEATHER,
  y = ~ARR_DELAY,
  type = "scattergl", mode = "markers",
  opacity = 0.7, marker = list(color="chocolate")
) |>
  layout(
    title = "Relationship: Weather Delay vs ARR_DELAY",
    xaxis = list(title = "Weather Delay (minutes)"),
    yaxis = list(
      title = "Arrival Delay (minutes)",
      tickformat = ",",     # Format y-axis có dấu phẩy
      separatethousands = TRUE
    )
  )

fig_4
#-------------------------------------------
# Có mối tương quan giữa DISTANCE và AIR_TIME không?
#-------------------------------------------
distance_air_df <- df_flights |>
  select(DISTANCE, AIR_TIME) |>
  drop_na()

distance_air_corr <- cor(distance_air_df$DISTANCE,
                         distance_air_df$AIR_TIME)

fig_5 <- plot_ly(
  data = distance_air_df,
  x = ~DISTANCE,
  y = ~AIR_TIME,
  type = "scattergl", mode = "markers",
  opacity = 0.7, marker = list(color="purple")
) |>
  layout(
    title = "Relationship: DISTANCE vs AIR_TIME",
    xaxis = list(title = "DISTANCE"),
    yaxis = list(
      title = "AIR_TIME",
      tickformat = ",",     # Format y-axis có dấu phẩy
      separatethousands = TRUE
    )
  )

fig_5
