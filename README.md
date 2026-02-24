[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README_ru.md)

# Data Warehouse & ETL Pipeline

## Project Overview

This project is extracts raw data from an operational database, processes it through transit layers, and loads it into a dimensional Data Warehouse for Business Intelligence reporting.

The entire infrastructure is containerized using **Docker Compose**.

### Tech Stack

-   **Database:** PostgreSQL 15
    
-   **Manage:** Apache Airflow 2.8
    
-   **Visualization:** Metabase
    
-   **Infrastructure:** Docker, Docker Compose
    
-   **Languages:** Python 3, SQL, Bash
    

----------

## Architecture & Key Engineering Decisions

### 1. Schema-based Logical Separation

The architecture utilizes PostgreSQL schemas (mrr, stg, dwh) within a single DWH database.

-   _Why:_ Creating separate physical databases in Postgres requires `postgres_fdw` for cross-database queries, which adds network overhead and kills performance. Schema-based separation provides clean logical boundaries while allowing for high-speed, native SQL transformations.
    

### 2. Delta Load & Idempotency

-   Data is extracted from the Source DB to the MRR layer using a High Water Mark  approach. The Airflow PythonOperator queries the `dwh.high_water_mark` table and fetches only new or updated `sales` records from the Source.
    
-   Transit layers (`mrr`, `stg`) are strictly truncated at the start of each run to prevent data duplication. Final loading into the `dwh` layer uses 
`INSERT ... ON CONFLICT DO UPDATE`, ensuring the pipeline can be rerun safely at any time.
    

### 3. Data Quality & Dead Letter Queue 

To maintain high data integrity, a Dead Letter Queue (DLQ) pattern is implemented during the staging (STG) phase. Invalid records (e.g., sales with qty <= 0) are not silently dropped; instead, they are routed to a dedicated stg.rejected_sales table for data auditing and further analysis, ensuring zero silent data loss.
    

### 4. Event Handlers & Logging

Robust event handling and monitoring are implemented leveraging Airflow Callbacks (on_success_callback, on_failure_callback). In the event of a DAG failure, the callback function intercepts the Python exception and logs the exact error message, timestamp, and status directly into a dedicated dwh.etl_logs table for streamlined debugging and observability.
    

### 5. BI Platform Independence

The Star Schema is fully realized at the DWH level, ensuring the data model is completely BI-agnostic and ready for integration with any visualization tool. For this infrastructure, Metabase was chosen and deployed directly within the Docker network. This provides a lightweight, self-contained BI solution that integrates seamlessly with the containerized environment, bypassing the need for external cloud dependencies or complex licensing setups.

----------

## How to Run the Project

### Step 1: Start the Infrastructure

Clone the repository and spin up the Docker containers. The initialization scripts (`init_source.sql` and `init_dwh.sql`) are automatically mounted and executed by Postgres on startup.

Bash

```
docker compose up -d

```

_Services started: `pg_source` (5432), `pg_dwh` (5433), `airflow` (8080), `metabase` (3000)._

### Step 2: Configure Airflow Connections

1.  Open Airflow UI: [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080) (Credentials: `admin` / `admin`).
    
2.  Navigate to **Admin -> Connections** and add two Postgres connections:
    
   -   **Conn Id:** `pg_source_conn` 
    -  **Host:** `pg_source` 
    -  **Port:** `5432` 
    -  **Schema:** `operational_db` 
    -  **Login:** `admin` 
    -  **Password:** `password`
  ---

   -   **Conn Id:** `pg_dwh_conn` 
    -  **Host:** `pg_dwh` 
    -  **Port:** `5432` 
    -  **Schema:** `data_warehouse` 
    -  **Login:** `admin` 
    -  **Password:** `password`
        

### Step 3: Trigger the ETL Pipeline

1.  In the Airflow UI, locate the DAG `mrr_stg_dwh_pipeline`.
    
2.  Unpause the DAG and trigger it manually.
    
3.  Once completed, you can verify the execution logs in the database:
    
    SQL
    
    ```
    SELECT * FROM dwh.etl_logs ORDER BY execution_time DESC;
    
    ```
    

### Step 4: Business Logic & Stored Procedure

A stored procedure uses explicit cursors and a `TRY/CATCH` block to calculate loyalty points based on purchase quantity and updates the Fact table. To test it, run this SQL command in the `pg_dwh` database:

SQL

```
CALL dwh.process_sales_loyalty();

```

### Step 5: View the Dashboard

1.  Open Metabase: [http://localhost:3000](https://www.google.com/search?q=http://localhost:3000).
    
2.  Connect it to the `pg_dwh` database (Host: `pg_dwh`, Port: `5432`, Database: `data_warehouse`).
    
3.  Build dashboards using the `dwh` schema.
    

----------


## Screenshots

### Dashboard (example)

![Dashboard](assets/dashboard_example.png)
