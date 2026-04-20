# 🗼 Paris Housing Analytics Pipeline

![Paris Housing Dashboard](images/dashboard-hero.png)

**End-to-end Batch Data Pipeline + Interactive Dashboard** for analyzing real estate prices across all 20 arrondissements of Paris.

This project demonstrates a complete modern data engineering workflow — from raw data ingestion to a production-ready interactive dashboard.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Key Features](#-key-features)
- [Architecture](#️-architecture)
- [Tech Stack](#️-tech-stack)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Dashboard](#-dashboard)
- [Key Insights](#-key-insights)
- [Deployment](#-deployment)
- [Future Improvements](#-future-improvements)
- [Author](#-author)

---

## 📊 Project Overview

The goal of this project was to apply everything learned during the Data Engineering course: building a **fully automated data pipeline** and a **professional dashboard**.

| Property | Details |
|----------|---------|
| **Dataset** | 1,200 synthetic property records in Paris (CSV) |
| **Pipeline type** | Batch (can be scheduled daily) |
| **Data Lake** | Parquet files (partitioned by date) |
| **Data Warehouse** | SQLite with pre-aggregated tables |
| **Dashboard** | Interactive Streamlit application |

---

## ✨ Key Features

- Full batch ETL pipeline with validation and logging
- Feature engineering (`Price_per_sqm`, `Decade_Built`, `Price_Bracket`, etc.)
- Pre-aggregated tables for fast dashboard queries
- Professional Streamlit dashboard with **two mandatory tiles**
- Sidebar filters and multiple visualizations
- Ready for production (Airflow, BigQuery, Spark, etc.)

---

## 🏗️ Architecture

```
Raw CSV → [ingest.py] → Data Lake (Parquet) → [transform.py] → Data Warehouse (SQLite) → Streamlit Dashboard
```

---

## 🛠️ Tech Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Language | Python 3.11 | — |
| Data Lake | Apache Parquet | Local → easily migrated to S3/GCS |
| Data Warehouse | SQLite | Easy to replace with BigQuery |
| Processing | pandas + pyarrow | Scalable to Spark |
| Orchestration | Python script | Ready for Airflow |
| Dashboard | Streamlit + Plotly | Beautiful UI |

---

## 📁 Project Structure

```bash
paris-housing-pipeline/
├── data/                          # raw CSV + generated files
├── pipeline/
│   ├── ingest.py
│   ├── transform.py
│   └── orchestrate.py
├── dashboard/
│   └── app.py
├── images/                        # put screenshots here
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start

```bash
cd paris-housing-pipeline
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

cd pipeline
python orchestrate.py

cd ../dashboard
streamlit run app.py
```

---

## 📈 Dashboard

Two mandatory tiles (as per the course assignment):

- **Tile 1 – Categorical**: Average price by arrondissement (bar chart)
- **Tile 2 – Temporal**: Average price by decade built (line chart)

Additional charts: property type distribution, condition analysis, size vs price scatter, KPI cards and filters.

### 📸 Dashboard Screenshot

<img width="1642" height="858" alt="image" src="https://github.com/user-attachments/assets/fa69c01a-3042-4eb6-9b64-0b84f74c8a7a" />

<img width="1548" height="484" alt="image" src="https://github.com/user-attachments/assets/836395d3-f536-4a6e-873c-d1cacb33aa4b" />

<img width="1555" height="485" alt="image" src="https://github.com/user-attachments/assets/15f20be5-9629-41c0-bfed-85f254e616ce" />


<img width="1563" height="545" alt="image" src="https://github.com/user-attachments/assets/e4026ef1-c2af-45f3-8ca8-86c48456921d" />

---

## 🔍 Key Insights

- **Most expensive districts**: 11e, 19e, 8e
- Strong correlation between size and price
- Properties in "Good" condition are ~10% more expensive
- 1980s buildings show the highest average prices

---

## 🌐 Deployment

Easiest way — **Streamlit Cloud**:

1. Push this repository to GitHub
2. Go to [https://share.streamlit.io](https://share.streamlit.io)
3. Create new app → select your repo → `dashboard/app.py`

---

## 🚀 Future Improvements

- [ ] Migrate to BigQuery / Snowflake
- [ ] Add Apache Airflow DAG
- [ ] Add data quality tests
- [ ] Docker + CI/CD

---

## 👤 Author

**Artem Berdnikov**  
Data Engineering Course — Final Project  
April 2026

---

⭐ If you like this project — please star the repository!
