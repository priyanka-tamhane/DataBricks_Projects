# IPL Data Engineering & Analytics Project

## 📌 Project Overview

This project builds an end-to-end **IPL (Indian Premier League) Data Engineering and Analytics pipeline** using **Databricks Free Edition, PySpark, Delta tables, and SQL**.

The objective was to transform raw IPL match and ball-by-ball data into clean, analytics-ready datasets and answer business-oriented questions through SQL and a Databricks dashboard.

The project follows a simplified **Medallion Architecture**:

**Raw Data → Bronze → Silver → Gold → SQL Analytics → Dashboard**

---

## 🎯 Project Objectives

The project was designed to demonstrate:

- Data ingestion using PySpark
- Schema definition and data validation
- Data cleaning and transformation
- Handling historical team-name changes
- Creating Silver and Gold Delta tables
- Understanding Spark joins and execution plans
- Using Photon and Adaptive Query Execution
- Building business-oriented SQL analytics
- Creating visualizations and a final dashboard
- Connecting analytical queries directly to persisted tables

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Databricks Free Edition** | Development and execution environment |
| **PySpark** | Data ingestion, transformation and validation |
| **Apache Spark** | Distributed data processing |
| **Delta Lake / Delta Tables** | Persistent Silver and Gold datasets |
| **SQL** | Business and analytical queries |
| **Photon** | Accelerated query execution |
| **Databricks SQL Dashboard** | Visualization and business reporting |
| **Kaggle IPL Dataset** | Source data |

---

## 📂 Source Data

The project uses IPL data containing:

### `matches.csv`

Match-level information including:

- Match ID
- Season
- Date
- Teams
- Venue
- Toss information
- Winner
- Result
- Result margin
- Player of the match
- Match officials

### `deliveries.csv`

Ball-by-ball information including:

- Match ID
- Inning
- Batting team
- Bowling team
- Over
- Ball
- Batter
- Bowler
- Runs scored
- Extra runs
- Wickets
- Dismissal information

The dataset covers **17 IPL seasons from 2008 to 2024**.

---

# 🥉 Bronze Layer

The Bronze layer represents the initial ingestion of the source data.

The raw datasets were loaded into Databricks and basic ingestion checks were performed.

The main principle at this stage was to preserve the source information while establishing a reliable starting point for downstream transformations.

---

# 🥈 Silver Layer

The Silver layer contains cleaned and validated datasets.

## `silver_matches`

Important transformations included:

- Cleaning the season field
- Converting the match date to a proper date type
- Handling missing values
- Deriving `match_outcome`
- Validating match-level records

### Important columns

```text
id
season
date
match_type
team1
team2
winner
result
result_margin
target_runs
target_overs
super_over
method
season_clean
match_outcome
```

Match outcome categories were validated as:

```text
Won
Tie
No Result
```

The dataset contains:

**1,095 matches**

---

## `silver_deliveries`

The ball-by-ball data was cleaned and enriched.

Important derived columns included:

- `is_legal_delivery`
- `is_legal_delivery_flag`
- `is_boundary`
- `is_six`
- `is_four`

The `is_legal_delivery_flag` was particularly useful for calculating:

- Balls faced
- Legal deliveries bowled
- Overs
- Economy rate

The final delivery dataset contains:

**260,920 deliveries**

---

# 🥇 Gold Layer

The Gold layer contains analytics-ready datasets designed around business questions rather than raw source structure.

## `gold_delivery_match`

This table combines delivery-level information with relevant match-level attributes.

It provides a convenient analytical foundation containing information such as:

- Match
- Season
- Venue
- Teams
- Winner
- Match outcome
- Batter
- Bowler
- Runs
- Extras
- Wickets
- Legal deliveries
- Boundaries

This table was heavily used for SQL analytics.

---

## `gold_batting_stats`

Player-level batting statistics were created including:

```text
batter
matches_played
total_runs
balls_faced
fours
sixes
dismissals
strike_rate
```

Strike rate was calculated using:

**Strike Rate = (Total Runs / Balls Faced) × 100**

A zero value was used where balls faced was not greater than zero to avoid division-by-zero issues.

---

## `gold_bowling_stats`

Bowling statistics were created including measures such as:

- Runs conceded
- Legal deliveries
- Overs
- Wickets
- Economy rate

Special attention was given to calculating **runs conceded correctly**, particularly for wides and no-balls.

---

## `gold_team_performance`

Team-level performance statistics were created including:

- Matches played
- Matches won
- Matches lost
- Ties
- No results

Historical team names were standardized during the analytical process where appropriate.

Examples:

```text
Delhi Daredevils → Delhi Capitals
Kings XI Punjab → Punjab Kings
Royal Challengers Bangalore → Royal Challengers Bengaluru
Deccan Chargers → Sunrisers Hyderabad
```

Historical franchises that were genuinely different teams were not incorrectly merged.

---

# ⚙️ Spark Processing Concepts Explored

During the project, Spark execution concepts were also investigated using physical execution plans.

## Broadcast Hash Join

Initially, Spark/Photon selected a:

```text
PhotonBroadcastHashJoin
```

for the delivery-to-match join because the match dataset was small enough to be broadcast.

## Sort Merge Join

After changing the join strategy, the physical plan showed:

```text
SortMergeJoin
```

This demonstrated how Spark can use different join strategies depending on data size, statistics and execution configuration.

The physical plan also showed:

- Shuffle Exchange
- Shuffle Map Stages
- Sort operations
- Photon execution
- Adaptive Spark Plan

The project was developed on **Databricks Free Edition / serverless compute**, so some Spark configurations and RDD-based approaches are restricted.

---

# 📊 Business & Analytics Questions

Once the Gold layer was complete, the project moved from data engineering into business analytics.

## 1. Top Scoring Batsman per Season

The top run scorer was identified for each IPL season using:

```sql
DENSE_RANK() OVER (
    PARTITION BY season
    ORDER BY total_runs DESC
)
```

Example results include:

| Season | Batsman | Runs |
|---:|---|---:|
| 2008 | SE Marsh | 616 |
| 2009 | ML Hayden | 572 |
| 2010 | SR Tendulkar | 618 |
| 2011 | CH Gayle | 608 |
| 2012 | CH Gayle | 733 |
| 2013 | MEK Hussey | 733 |
| 2016 | V Kohli | 973 |
| 2022 | JC Buttler | 863 |
| 2023 | Shubman Gill | 890 |
| 2024 | V Kohli | 741 |

---

## 2. Powerplay Economical Bowlers

Powerplay was defined as the first six overs:

```text
over < 6
```

Since the dataset's over numbering starts at zero, this represents:

```text
0, 1, 2, 3, 4, 5
```

Economy was calculated using legal deliveries:

**Economy = Runs Conceded / Overs**

A minimum legal-delivery threshold was used to avoid ranking bowlers with very small sample sizes.

---

## 3. Average Runs in Wins

The score made by the winning team was calculated at match level and then averaged by team.

The analysis required identifying deliveries where:

```text
batting_team = winner
```

and aggregating the team's total runs for each match.

The results were visualized using a scatter plot.

---

## 4. Scores by Venue

Average team scores were calculated by venue.

The resulting data was visualized using a horizontal bar chart to make venue names easier to read.

---

## 5. Dismissal Kind Analysis

Dismissals were analyzed by dismissal type, including:

```text
caught
bowled
run out
lbw
stumped
caught and bowled
hit wicket
retired hurt
retired out
obstructing the field
```

The analysis included dismissal counts and dismissal percentages.

A bar chart was used to visualize the distribution.

---

# 📈 Final Dashboard

A Databricks SQL Dashboard was created to present the main KPIs and analytical results.

## KPI Summary

| KPI | Value |
|---|---:|
| 🏏 Total Matches | **1,095** |
| 👥 Total Teams | **13** |
| 🏃 Total Runs | **347,756** |
| 🎯 Total Wickets | **12,950** |
| 🏆 Total Seasons | **17** |

## Dashboard Visualizations

The dashboard contains:

- Top Scoring Batsman per Season
- Powerplay Economical Bowlers
- Average Runs in Wins
- Scores by Venue
- Dismissal Kind Analysis

The dashboard queries the underlying Silver/Gold tables rather than relying on hardcoded results.

When the underlying tables are updated, the dashboard can reflect the latest values after the associated queries/dashboard are refreshed.

---

# 🔍 Data Validation

Validation was performed throughout the pipeline.

Examples included:

- Row-count validation
- Duplicate checks
- Null checks
- Schema validation
- Match outcome validation
- Delivery validation
- Legal-delivery validation
- Join validation
- Gold-layer count validation

Key validated counts include:

```text
Matches       : 1,095
Deliveries    : 260,920
Seasons       : 17
Teams         : 13
Total Runs    : 347,756
Total Wickets : 12,950
```

---

# 🏗️ Architecture

```text
                    IPL Raw Dataset
                          │
                          ▼
                 ┌─────────────────┐
                 │  Bronze Layer   │
                 │   Raw Ingestion │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  Silver Layer   │
                 │ Clean + Validate│
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   Gold Layer    │
                 │ Analytics Ready │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   SQL Analytics │
                 │ Business Queries│
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Databricks SQL  │
                 │    Dashboard    │
                 └─────────────────┘
```

---

# 💡 Key Learnings

This project helped connect individual Data Engineering concepts into a complete workflow.

### PySpark

- DataFrame transformations
- `withColumn`
- `when`
- Aggregations
- Window functions
- Joins
- Schema handling
- Data validation

### Spark

- Lazy evaluation
- Partitions
- Shuffles
- Broadcast joins
- Sort Merge Joins
- Physical execution plans
- Adaptive Query Execution
- Photon

### Databricks

- Unity Catalog
- Volumes
- Delta tables
- Serverless compute
- SQL Editor
- SQL Dashboards
- Performance analysis

### SQL

- CTEs
- Aggregations
- Window functions
- `DENSE_RANK`
- Conditional aggregation
- Analytical queries
- Business-oriented metrics

---

# 🚀 Future Improvements

Possible future enhancements include:

- Automating ingestion using Databricks Jobs
- Adding incremental processing
- Implementing data quality checks with expectations
- Adding more historical IPL seasons
- Creating season/team filters in the dashboard
- Adding player comparison analytics
- Adding team-vs-team analysis
- Adding win percentage and net run rate analysis
- Creating automated dashboard refresh schedules
- Deploying the pipeline using Azure services

---

# 📌 Final Outcome

The project demonstrates an end-to-end Data Engineering workflow:

**Ingest → Clean → Validate → Transform → Aggregate → Analyze → Visualize**

The most important takeaway is that the Gold layer was designed around **business usability**, allowing SQL queries to answer meaningful questions without repeatedly rebuilding transformations from raw data.

This project is part of my continued learning journey toward becoming an **Azure Data Engineer**, with a focus on **Azure, Databricks, PySpark, Spark and SQL**.

---

## 👤 Project Focus

**Domain:** Sports Analytics  
**Dataset:** IPL 2008–2024  
**Processing:** PySpark / Spark  
**Storage:** Delta Tables  
**Analytics:** SQL  
**Visualization:** Databricks SQL Dashboard  
**Architecture:** Medallion Architecture

