# Flight Delay Risk Analytics

This project estimates **flight delay / cancellation risk** using a **data-driven, rule-based approach**, with a **machine learning (ML) baseline** included strictly for benchmarking.

Two deliverables are provided:

* **R Shiny App** – interactive, explainable decision-support tool
* **Python Analysis Script** – analysis and benchmarking artifact

---
## Dataset

**Data Source:**  
U.S. Department of Transportation (BTS), accessed via  
[Kaggle – Flight Delay and Cancellation Dataset (2019–2023)](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023/data).

---
## R Shiny App (Product-Oriented)

**Purpose**
Provide an interactive dashboard for exploring flight delay risk in a **transparent and explainable** way.

**Core Logic**

* Rule-based risk scoring derived from historical delay rates
* Components include airline, route, time of day, season, and weekday/weekend
* Each component contributes a normalized score (0–100)

**Why Rule-Based?**

* Clear explanation for each prediction
* Stable and fast
* Suitable for operational and non-technical users

**What the App Outputs**

* Overall risk score (0–100)
* Risk level (Low / Medium / High)
* Breakdown of contributing factors

> The Shiny app intentionally avoids ML models to prioritize clarity and user trust.

---

## Python Script (Analysis & Benchmarking)

**Purpose**
Demonstrate analytical reasoning and technical decision-making behind the scoring logic.

**Contents**

1. Exploratory Data Analysis (EDA)
2. Rule-based risk scoring (same logic as the Shiny app)
3. ML baseline using Logistic Regression
4. Quantitative comparison and conclusions

**ML Baseline**

* Logistic Regression
* Uses the same features as the rule-based approach
* No tuning or advanced feature engineering

**Result**

* ML and rule-based scores are highly similar (e.g. 20/100 vs 23/100)
* ML is slower and less interpretable

**Decision**
The rule-based approach is retained as the core solution. The ML model serves only as a **benchmark**, confirming that the rules capture real data patterns.

---

## Key Takeaway

Instead of defaulting to machine learning, this project shows that:

* Simple, data-driven rules can match ML performance
* Explainability and stability matter for real-world use
* ML is most valuable when used as a benchmark, not by default

---

## Notes

* Shiny App = product & explanation
* Python Script = analysis & evidence

This separation reflects real-world analytics and decision-support system design.

---

## Design Rationale

* Uses a rule-based risk scoring approach in production for speed, stability, and explainability

* All scoring weights are calibrated offline in Python using historical delay distributions (median, p75, p90)

* A lightweight ML baseline is used only for validation, not for deployment

* Clear separation between offline analysis (Python) and online inference (R Shiny) for maintainability
