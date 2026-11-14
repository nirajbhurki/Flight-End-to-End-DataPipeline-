# ✈️ Flight End-to-End Data Pipeline

An end-to-end data engineering project that builds a modern
flight-booking data pipeline using **Databricks, Apache Spark, PySpark,
Delta Lake, Delta Live Tables (DLT), and dbt**. The project demonstrates
how raw airline data can be ingested, cleaned, transformed, modeled, and
prepared for analytics using the Medallion Architecture.



## 📌 Project Overview

The goal of this project is to create a structured and scalable data
pipeline for flight and booking-related datasets. It processes data
related to flights, bookings, passengers/customers, and airports through
multiple stages.

The pipeline follows the **Bronze → Silver → Gold** Medallion
Architecture:

-   **Bronze:** Ingests source CSV files into Delta tables.
-   **Silver:** Cleans, validates, standardizes, and manages changes in
    the data.
-   **Gold:** Builds fact and dimension tables for analytical use.
-   **dbt:** Adds a SQL-based analytics modeling layer on Databricks.

Databricks is used as the core data engineering platform for distributed
data processing, storage, transformation, and pipeline development.

## 🏗️ Architecture & Data Flow

``` text
Source CSV Files
(Flights, Bookings, Customers/Passengers, Airports)
                  |
                  v
        Databricks / Raw Storage
                  |
                  v
       BRONZE LAYER – Auto Loader
   Incremental ingestion into Delta tables
                  |
                  v
       SILVER LAYER – DLT
  Data cleaning, validation and CDC handling
                  |
                  v
        GOLD LAYER – Dimensional Model
       Fact and Dimension Delta tables
                  |
                  v
             dbt on Databricks
       SQL-based analytical models
                  |
                  v
       Analytics / BI-ready datasets
```

## ⚙️ How the Pipeline Works

### 1. Source Data and Setup

The project works with CSV datasets representing different parts of
airline operations, such as:

-   Flight information
-   Booking information
-   Passenger or customer details
-   Airport information

The source files are organized in the Databricks environment so that the
ingestion process can access them. Project setup and source parameters
are configured through Databricks notebooks.

### 2. Bronze Layer -- Data Ingestion

The Bronze layer is responsible for bringing raw source data into the
Lakehouse.

**Tools and concepts:** - Databricks Auto Loader - Apache Spark
Structured Streaming - PySpark - Delta Lake - Schema inference and
schema evolution

Auto Loader is used to detect and ingest source files incrementally. The
ingested data is stored in Delta tables, creating a raw foundation for
downstream processing.

**Output:** Raw Bronze Delta tables for the different source datasets.

### 3. Silver Layer -- Data Cleaning and Transformation

The Silver layer converts raw data into cleaner, more reliable datasets.

**Processing includes:** - Data type handling and standardization -
Handling missing, invalid, or inconsistent values - Applying data
quality expectations and validation rules - Managing data changes
through Change Data Capture (CDC) - Creating refined Delta tables for
downstream use

The transformation logic is implemented using **Databricks Delta Live
Tables (DLT)**, allowing the pipeline to define and manage dependencies
between transformed datasets.

**Output:** Cleaned and validated Silver tables.

### 4. Gold Layer -- Dimensional Data Modeling

The Gold layer prepares the refined data for reporting and analytical
queries.

The project creates a dimensional model using fact and dimension tables.
Dimension tables provide descriptive context, while the fact table
stores measurable business events and connects to the relevant
dimensions.

The model includes tables such as:

-   `DimPassengers` -- passenger-related descriptive information
-   `DimFlights` -- flight-related information
-   `DimAirports` -- airport details
-   `FactBookings` -- booking-related records and measures

The Gold layer uses joins, surrogate keys, incremental processing logic,
and `MERGE` operations where required to maintain the dimensional
tables.

**Output:** Analytics-ready fact and dimension tables stored as Delta
tables.

### 5. dbt -- Analytical Modeling

The project also uses **dbt (Data Build Tool)** with Databricks to
demonstrate SQL-based data modeling.

The dbt layer connects to a Databricks SQL Warehouse and allows
analytical models to be created and managed using SQL.

Typical workflow:

1.  Configure the dbt connection to Databricks.
2.  Define SQL models.
3.  Choose appropriate materializations, such as tables or views.
4.  Execute models using `dbt run`.
5.  Use the resulting models as a foundation for analytics and BI.

This separates analytical transformations from the underlying ingestion
and data-processing logic.

## 🧰 Technology Stack

  Data Engineering Platform   : Databricks        
  Programming                 : Python, PySpark, SQL        
  Processing Engine           : Apache Spark       
  Data Ingestion              : Databricks Auto Loader      
  Streaming                   : Spark Structured Streaming     
  Data Transformation         : Delta Live Tables (DLT)       
  Storage Format              : Delta Lake       
  Architecture                : Medallion Architecture (Bronze, Silver, Gold)      
  Data Modeling               : Fact and Dimension Modeling      
  Analytics Engineering       : dbt      
  Query & Analytics           : Databricks SQL Warehouse       
  Version Control             : Git, GitHub       

## 📂 Project Components

The repository contains the project resources used to configure the
environment and implement the different pipeline stages. The main
components cover:

-   Environment and source-data setup
-   Source parameter configuration
-   Bronze-layer ingestion
-   Silver-layer transformations using DLT
-   Gold-layer dimension table creation
-   Gold-layer fact table creation
-   Databricks project export (`.dbc`), where included


## 🎯 Key Learning Outcomes

Through this project, I gained practical experience with:

-   Building an end-to-end data pipeline on Databricks.
-   Ingesting files incrementally using Auto Loader.
-   Processing data with Apache Spark and PySpark.
-   Implementing the Medallion Architecture.
-   Building data quality and transformation workflows with DLT.
-   Understanding CDC concepts and handling data changes.
-   Designing fact and dimension tables for analytical workloads.
-   Using Delta Lake and `MERGE` operations.
-   Connecting dbt with Databricks for SQL-based transformations.
-   Organizing raw data into analytics-ready datasets.

## 👨‍💻 Author

**Niraj Bhurki**

-   GitHub: [NirajB25](https://github.com/NirajB25)
-   Project Repository: [Flight End-to-End Data
    Pipeline](https://github.com/NirajB25/Flight-End-to-End-DataPipeline)

