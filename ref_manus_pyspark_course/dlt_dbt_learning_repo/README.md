# Delta Live Tables (DLT) and dbt Learning Repository

This repository is designed to help you learn and practice Delta Live Tables (DLT) and dbt for data engineering, specifically focusing on how these technologies implement the Medallion Architecture within Databricks. This builds upon your foundational PySpark knowledge.

## Repository Structure

- `data/`: Contains sample datasets and scripts to generate synthetic data.
- `dlt_pipelines/`: Python scripts for DLT pipelines (Bronze, Silver layers).
- `dbt_models/`: dbt project containing models for the Gold layer.
- `notebooks/`: Jupyter notebooks for interactive DLT/dbt development and exploration.
- `conf/`: Configuration files for DLT and dbt projects.

## Getting Started

1.  **Prerequisites**: Access to a Databricks workspace with DLT and dbt enabled. (You'll be simulating this locally where possible, but some DLT features require Databricks).
2.  **Clone this repository**: `git clone <repository_url>` (Once this is a real repo, you'll put the URL here).
3.  **Explore the content**: Review the DLT pipeline scripts, dbt models, and notebooks.
4.  **Hands-on Practice**: Adapt the provided examples to your own data and use cases.

## Medallion Architecture Layers with DLT and dbt

-   **Bronze Layer (DLT)**: Raw, untransformed data ingested into Delta tables using DLT pipelines.
-   **Silver Layer (DLT)**: Cleaned, conformed, and enriched data, transformed by DLT pipelines from the Bronze layer.
-   **Gold Layer (dbt)**: Highly aggregated and transformed data, optimized for analytics and reporting, built using dbt models on top of Silver layer Delta tables.

Let's dive into DLT and dbt!

