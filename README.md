# End-to-End Data Engineering Project 01

An end-to-end data pipeline that incrementally ingests data with PySpark Structured Streaming into a Databricks lakehouse. Data moves through Bronze, Silver and Gold layers using Medallion Architecture, with reusable Python classes for cleaning, deduplication and UPSERT. In the Gold layer, dbt builds a star schema with incremental models and SCD Type 2 snapshots.

**Tech Stack:** Python, PySpark, Databricks, Delta Lake, dbt, SQL
