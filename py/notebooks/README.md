# ✈️ U.S. Flight Operations & Delay Risk Analysis

## Overview

This project analyzes U.S. civil flight operations with a focus on **delay, disruption, and explainable risk assessment**. It combines structured EDA with an **interpretable, rule-based delay risk predictor** to help users understand *why* certain flights are more delay-prone under similar historical conditions.

The goal is **understanding and explantion**, not pure prediction accuracy.

---
## Dataset

**Data Source:**  
U.S. Department of Transportation (BTS), accessed via  
[Kaggle – Flight Delay and Cancellation Dataset (2019–2023)](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023/data).

---
## Data Preparation

* Feature engineering for time, season, routes, distance, and geolocation.
* Data cleaning and validation for delays, cancellations, and diversions.

---

## Exploratory Data Analysis (EDA)

* Flight volume and delay patterns over time.
* Airline and route performance comparisons.
* Delay causes and disruption breakdowns.
* Local patterns by airport, time of day, and season.
* Geographic visualization of arrival delays.
---

## Delay Risk Prediction (No ML)

* User inputs; airline, route, flight date, time of date.
* Each factor contributes a risk based on historical behavior.
* Scores are aggregated into an overall risk level (low/medium/high).
* Output includes clear explanation of why the risk is high or low.

## Why no Machine Learning?

* The objective is **interpretability**, not black-box prediction.
* Historical flight delays are highly context-dependent and noisy.
* Rule-based scoring allows transparent logic, clear reasoning for each risk factor, and easier validation and communication.
