# Azure-Data-Factory-ADF-Interview_Questions_Answers
Creating this repo to practice the ADF Scenario based questions


# Azure Data Factory (ADF) Real-Time Scenario Practice Lab

Welcome to the **Azure Data Factory Real-Time Ingestion & ETL Practice Lab**. This repository is dedicated to mastering Microsoft Azure Data Factory (ADF) by building end-to-end solutions for the top real-world data engineering scenarios. 

Each scenario is focused on solving enterprise-level challenges including **Incremental Updates (Delta Loads)**, **Metadata-Driven Pipelines**, **Schema Drift**, **Performance Tuning**, and **Data Lake Orchestration**.

---

## 🛠️ Unified Environment Pre-requisites

To practice these scenarios without constantly modifying your cloud resources, set up this standardized environment blueprint first.

### 1. Storage Account Architecture (ADLS Gen2)
Create a single storage container named `root` (or `data-lake`) and configure the following folder hierarchy based on the **Medallion Architecture**:

```text
root/
├── landing/               -- Unparsed API responses, external drops, and landing webhooks
│   └── json/              -- For Nested JSON drops (Scenarios 8, 18, 38, 39)
│
├── incoming/              -- Event-Based and Tumbling Window triggers landing pad
│   └── orders/            -- For Event triggers watching for files (Scenarios 2 & 3)
│
├── raw/                   -- Raw data zone for standard batch loops and schema drift tests
│   ├── sales/             -- For Batch looping over multiple files (Scenarios 10 & 14)
│   ├── customers/         -- For Testing schema drift with v1/v2 files (Scenario 24)
│   └── archive/           -- For file retention, processing history, or cleanups
│
├── silver/                -- Cleansed, transformed, and deduplicated structured data
│   ├── sales_delta/       -- Sink destination for incremental delta loads (Scenario 1)
│   ├── customer_dim/      -- For SCD Type 1/2 and Delta Merges (Scenarios 5, 6, 34)
│   └── compacted_data/    -- Target folder where small files get merged (Scenario 28)
│
└── error/                 -- Quarantine zone for faulty data
    └── bad_records/       -- For empty, corrupt, or failed validation files (Scenarios 17 & 32)

```

### 2. Relational Database Setup (Azure SQL / On-Premise)

Run the following initialization script in your SQL database to set up the control tables, metadata structures, logging tables, and mock source data required across the scenarios:

```sql
-- =========================================================================
-- 1. CONTROL, METADATA & AUDIT TABLES
-- =========================================================================

-- For Scenario 1: Watermark Table (Incremental Loads)
CREATE TABLE ControlWatermark (
    TableName VARCHAR(100) PRIMARY KEY,
    LastWatermarkValue DATETIME
);
INSERT INTO ControlWatermark (TableName, LastWatermarkValue) 
VALUES ('Sales', '2025-01-10 12:00:00');

-- For Scenario 16: Audit Logging Framework
CREATE TABLE PipelineAuditLog (
    RunId VARCHAR(100) PRIMARY KEY,
    PipelineName VARCHAR(200),
    Status VARCHAR(50),
    StartTime DATETIME2,
    EndTime DATETIME2,
    ErrorMessage VARCHAR(MAX)
);

-- For Scenario 36: Metadata-Driven Framework Configuration
CREATE TABLE PipelineConfig (
    SourceTable VARCHAR(100) PRIMARY KEY,
    TargetPath VARCHAR(200),
    LoadType VARCHAR(50),
    WatermarkColumn VARCHAR(100)
);
INSERT INTO PipelineConfig (SourceTable, TargetPath, LoadType, WatermarkColumn) VALUES 
('Sales', 'raw/sales/', 'Incremental', 'ModifiedDate'),
('Customers', 'raw/customers/', 'FullLoad', NULL);

-- For Scenario 40: Duplicate File Prevention Log
CREATE TABLE ProcessedFileLog (
    FileName VARCHAR(255) PRIMARY KEY,
    FileHash VARCHAR(64),
    ProcessedDate DATETIME DEFAULT GETDATE()
);

-- =========================================================================
-- 2. OPERATIONAL MOCK DATA TABLES
-- =========================================================================

-- Source Table: Sales (With timestamp column for Delta tracking)
CREATE TABLE Sales (
    SalesID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    Amount DECIMAL(10,2),
    ModifiedDate DATETIME DEFAULT GETDATE()
);
INSERT INTO Sales (ProductID, Amount, ModifiedDate) VALUES 
(101, 50.00, '2025-01-09 09:00:00'),   -- Historical (Skip)
(102, 120.50, '2025-01-10 11:45:00'),  -- Historical (Skip)
(103, 300.00, '2025-01-11 14:20:00'),  -- New Delta Record (Extract)
(104, 85.00, '2025-01-12 16:05:00');   -- New Delta Record (Extract)

-- Sink Table: Dimension Customer (For SCD Type 1 Upserts)
CREATE TABLE dim_customer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    Phone VARCHAR(20),
    Email VARCHAR(100),
    InsertedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);
INSERT INTO dim_customer (CustomerID, CustomerName, Phone, Email) 
VALUES (1, 'Amarnath Kambale', '9999999999', 'amarnath@example.com');

```

### 3. Advanced Compute Prep (Databricks Environment)

If you are pursuing Databricks integrations, verify that you have run these commands inside a notebook to build your baseline Delta Lake structures:

```python
# Spark SQL Environment Initialization
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Target Delta Table for Upserts/SCD1
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.customers (
    id INT,
    name STRING,
    city STRING,
    last_updated TIMESTAMP
) USING DELTA
""")

```

---

## 🚀 Repository Structure

As you build out the pipelines, organize the code files dynamically using this structure:

```text
├── .github/                  -- CI/CD workflows for Azure DevOps/GitHub Actions
├── arm_templates/            -- Exported ARM templates of the completed pipelines
├── sql_scripts/              -- DDL & DML setup scripts for relational environments
├── notebooks/                -- Databricks PySpark files used in integration scenarios
└── README.md                 -- Lab documentation and roadmap

```

---

## 📈 Scenario Practice Status Tracker

Use this checklist to track your development progress as you build out the pipelines in your Data Factory instance.

| # | Scenario Category / Description | Primary ADF Components Used | Status |
| --- | --- | --- | --- |
| **01** | Load Only New/Modified Records (Incremental Delta Load) | Lookup + Copy + Stored Procedure | ⬜ Planned |
| **02** | Trigger Pipeline Only When File Exists | Get Metadata + If Condition | ⬜ Planned |
| **03** | Trigger Pipeline When File Arrives (Event-Based) | BlobCreated Event Trigger | ⬜ Planned |
| **04** | Pass Parameters from ADF to Databricks Notebook | Notebook Activity + Base Parameters | ⬜ Planned |
| **05** | Implement SCD Type-2 (Maintain Data History) | Mapping Data Flow + Alter Row | ⬜ Planned |
| **06** | Merge Incremental Changes Using Databricks Delta | Databricks Notebook + Delta MERGE SQL | ⬜ Planned |
| **07** | Handle Schema Drift Automatically | Mapping Data Flow + Column Patterns | ⬜ Planned |
| **08** | Load Nested JSON Files | Mapping Data Flow + Flatten Transformation | ⬜ Planned |
| **09** | Optimize Copy Activity for Large Data (10M+ Rows) | Parallel Copy + DIU Tuning + Staging | ⬜ Planned |
| **10** | Process Multiple Files in Folder Dynamically | Get Metadata + ForEach + Dynamic Content | ⬜ Planned |
| **11** | Process Millions of Rows Efficiently (SQL ➔ Synapse) | PolyBase + Table Partition Ingestion | ⬜ Planned |
| **12** | Dynamic File-Name Based Ingestion (Daily Batches) | Pipeline Parameters + String Functions | ⬜ Planned |
| **13** | Call REST API with Pagination (Token Loops) | Web Activity + Until Loop + Variables | ⬜ Planned |
| **14** | Process All Files in a Folder Without Hardcoding | Get Metadata (Child Items) + ForEach | ⬜ Planned |
| **15** | Prevent Parallel Pipeline Runs (Concurrency Locks) | Pipeline Settings ➔ Concurrency = 1 | ⬜ Planned |
| **16** | Audit Logging Framework for Pipeline Tracking | Stored Procedure + On-Failure Paths | ⬜ Planned |
| **17** | Validate File Format/Extension Before Ingestion | Get Metadata + Split/EndsWith Expressions | ⬜ Planned |
| **18** | Process Large Nested JSON Files via Spark | Databricks Activity + Auto Loader | ⬜ Planned |
| **19** | Copy Securely from Private On-Prem networks to Cloud | Self-Hosted Integration Runtime (SHIR) | ⬜ Planned |
| **20** | Stored Procedure Driven ETL Framework | Stored Procedure Activity + Core DB Logic | ⬜ Planned |
| **21** | Partitioning Mapping Data Flows for Spark Compute | Optimize Tab + Key/Hash Partitions | ⬜ Planned |
| **22** | Mitigate Copy Activity Timeout Failures | Timeout Adjustments + Staged Storage Copies | ⬜ Planned |
| **23** | Capture and Process Near Real-Time Streaming Streams | Event Hub + Tumbling Window Trigger | ⬜ Planned |
| **24** | Dynamic Handling of Varying Source Schema Columns | Data Flow Ingestion + Schema Evolution | ⬜ Planned |
| **25** | Append Daily Ingestion Batches into Historical Syncs | Partitioned Sink Writing + Pre-Copy Script | ⬜ Planned |
| **26** | Automated Storage Account Cleanup Policies | Get Metadata + Date Math + Delete Activity | ⬜ Planned |
| **27** | Sanitize and Impute NULL/Missing Values | Mapping Data Flow + Derived Columns | ⬜ Planned |
| **28** | Compact Thousands of Fragmented Small File Batches | Data Flow + Re-partition File Sinks | ⬜ Planned |
| **29** | Execute Process Loop Routines on Fixed Weekly Windows | Tumbling Window Trigger + Window Boundaries | ⬜ Planned |
| **30** | Fail Pipeline Early Under Low Row-Count Conditions | Lookup (Row Counts) + If Condition + Fail | ⬜ Planned |
| **31** | Reprocess Ingestion Windows for Late-Arriving Records | Date Parameter Overrides + MERGE Logic | ⬜ Planned |
| **32** | Quarantine Corrupt, Damaged, or Zero-Byte Flat Files | Get Metadata (Size Check) + Copy (Move) | ⬜ Planned |
| **33** | Generate Parameterized Query Selection Filters | Dynamic Content + Concatenation Filters | ⬜ Planned |
| **34** | Implement SCD Type-1 (In-Place Target Overwrites) | Data Flow Alter Row / Delta Merge SQL | ⬜ Planned |
| **35** | Parallelized Execution Sub-Pipeline Fan-Outs | Lookup List + Concurrent ForEach Execution | ⬜ Planned |
| **36** | Fully Decoupled Metadata-Driven Orchestration Loop | Config Table Ingestion + Pipeline Parameter Pass | ⬜ Planned |
| **37** | Automate Hive-Style Storage Account Date Partitioning | Dynamic Dataset Paths (`year=`, `month=`) | ⬜ Planned |
| **38** | Sequence REST API Extractions with Dynamic Bearer Tokens | Web (Auth POST) + Web (Data Fetch GET) | ⬜ Planned |
| **39** | Convert Highly Deep JSON Payloads into Clean Tables | Flatten Structure Target Column Mapping | ⬜ Planned |
| **40** | Deduplicate Ingested Storage Files via Hash Logs | Processed Logs Lookup + Checksum Scans | ⬜ Planned |
| **41** | Maximize Network Output Bandwidth to Clear Bottlenecks | Snappy Compression + Tuning DIUs | ⬜ Planned |
| **42** | Accelerate Compute Clustered Pipelines with Scale Keys | Hash/Range Rescale Settings | ⬜ Planned |
| **43** | Debug Mapping Dataflows and Monitor Partitions | Data Flow Debug Mode + Inspect Profiler | ⬜ Planned |

---

## 🎯 Contribution & Goal

This repository serves as a personal workbook and reference guide for design patterns in Cloud Data Integration. Feel free to use these environmental setups to build out your own validation solutions.

If you find this schema preparation blueprint helpful for your interview or project prep, don't forget to **Star ⭐ this repository**!

```

```
