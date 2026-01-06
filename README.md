📊 BBU Forecasting Pipeline (Databricks Asset Bundle)

A high-performance Data Engineering and Machine Learning forecasting platform built on Azure Databricks using Databricks Asset Bundles (DABs). The system automates end-to-end retail demand forecasting for Zebra/Antuit across multiple operational cadences.

🚀 Overview

The BBU pipeline follows the Medallion Architecture (Silver & Gold layers) and supports Daily, Weekly, and Biannual workflows. It integrates scalable data ingestion, Delta Live Tables, and distributed ML training for production-grade forecasting.

🛠 Technology Stack

Infrastructure: Databricks Asset Bundles (DABs)

Data Processing: Delta Live Tables (DLT), PySpark

Machine Learning: Spark ML, CatBoost, SynapseML

Packaging: Python Wheels (Hatchling)

Deployment: Multi-environment (Dev, Stage, QA, PROD, DR)

🗂 Project Structure
├── asset_bundles/          # Core Databricks Asset Bundle configuration
│   ├── databricks.yml      # Environment targets & deployment config
│   └── resources/          # Generated Jobs and Pipelines YAML
├── src/                    # Forecasting and data processing logic
├── deploy/                 # Infrastructure-as-code generators
├── pyproject.toml          # Dependencies and build metadata
└── tests/                  # Pytest validation suite

🌍 Environments

dev: Personal sandbox (current user)

stage: Staging validation (service principal)

PROD: Production environment for Zebra Technologies

🔄 Key Workflows

Weekly Pipeline: Ingests raw data, refreshes Silver/Gold DLT tables, runs ML forecasts, and exports outbound facts

Forecasting: Segmented modeling for Aged and Scan products

Biannual Maintenance: Seasonal detection (Beach Detect, Level Shift)

Simulation & Validation: What-if scenarios and holdout testing

⚙️ Setup & Deployment
Install
pip install .[dev]

Generate Infrastructure
python deploy/deployer.py

Deploy (example: stage)
databricks bundle deploy -t stage
