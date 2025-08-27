
# Data Warehouse & Analytics Project (PostgreSQL)
Welcome to the Data Warehouse and Analytics Project repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

## 🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers
<img width="1027" height="523" alt="data flow digram" src="https://github.com/user-attachments/assets/c2f21394-9210-4839-bc79-f148a105dbfc" />
1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into Postgres Database.
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

## 📌 Project Overview
This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.

2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
   
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
   
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.
   
🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

- SQL Development
- Data Architect
- Data Engineering
- ETL Pipeline Developer
- Data Modeling
- Data Analytics
  
## 🎯 Objectives
- Design a data warehouse schema (Star or Snowflake schema).

- Integrate data from multiple sources (CSV, Excel, or external databases).

- Implement ETL processes directly in PostgreSQL using SQL and scripts.

- Perform advanced analytics with PostgreSQL functions and window queries.
  
## 🛠️ Tools & Technologies
- PostgreSQL (Database & ETL using COPY and SQL scripts)

- SQL (DDL, DML, Analytical Queries)

- CSV / Excel (Source data)

- Power BI / Tableau (Optional visualization)

## 🚀 Features
- Dimensional Modeling (Star schema with Fact and Dimension tables)

- ETL Pipeline to clean, transform, and load data into warehouse tables

- Aggregated Reports for KPIs

- Sample Analytical Queries (Trends, comparisons, rankings, growth analysis)

 ## 📊 Example Queries
- Top 10 products by sales

- Year-over-year revenue growth

- Regional sales performance

- Customer purchase frequency

## 📦 How to Use
1. Clone this repository:
   ```bash
   git clone https://github.com/mauricebongani/data-warehouse-sqlserver.git
2. Restore the SQL Server database from schema/ scripts.

3. Run ETL processes to populate the warehouse.

4. Execute queries in queries/ for analytics.

## 📜 License
This project is licensed under the MIT License.

## 👋 About Me  

Hi, I'm Maurice — a Computer Science graduate and aspiring **Data Engineer | Data Analyst** with a passion for building data-driven solutions. I enjoy transforming raw data into valuable insights that support decision-making. Always learning and exploring better ways to engineer data pipelines and uncover business intelligence.  


---

