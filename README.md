# COVID-19 Multinational Data Pipeline

> A production-ready data engineering pipeline that orchestrates the ingestion, transformation, and delivery of COVID-19 datasets from multiple national sources using modern ELT patterns and medallion architecture.

### 🔗 Related Repositories
- **[Exploratory Data Analysis](https://github.com/siwa-p/marvan_project_eda)** - Initial Data Exploration 
- **[RESTful API](https://github.com/MichaelGalo/marvan-covid-api)** - FastAPI service for data access

---

## 🏗️ Architecture Overview

This pipeline implements a **medallion architecture** (Bronze → Silver → Gold) with three distinct data transformation layers:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   🥉 BRONZE     │    │   🥈 SILVER      │    │   🥇 GOLD       │
│   (Raw Layer)   │───▶│  (Staged Layer) │───▶│ (Cleaned Layer) │
│                 │    │                 │    │                 │
│ • Raw API data  │    │ • Validated     │    │ • Business-ready│
│ • Minimal proc. │    │ • Standardized  │    │ • Aggregated    │
│ • Direct load   │    │ • Tested        │    │ • Timestamped   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Tech Stack

| Category | Technology | Purpose |
|----------|------------|---------|
| **Orchestration** | ![Prefect](https://img.shields.io/badge/Prefect-3E4B99?style=flat-square&logo=prefect&logoColor=white) | Workflow automation and scheduling |
| **Data Transformation** | ![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white) | SQL-based transformations and testing |
| **Data Warehouse** | ![Snowflake](https://img.shields.io/badge/Snowflake-056DD0?style=flat-square&logo=snowflake&logoColor=white) | Cloud data warehouse |
| **Object Storage** | ![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat-square&logo=minio&logoColor=white) | S3-compatible staging storage |
| **Data Processing** | ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white) | DataFrame manipulation |
| **Testing & CI/CD** | ![pytest](https://img.shields.io/badge/pytest-009FE3?style=flat-square&logo=pytest&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white) | Automated testing and data quality |

## 📊 Data Sources

The pipeline ingests COVID-19 datasets from three national government sources:

- **🇺🇸 United States**: [data.gov](https://catalog.data.gov/) - Provisional death counts and demographic data
- **🇬🇧 United Kingdom**: [data.gov.uk](https://www.data.gov.uk/) - Daily case counts and epidemiological data  
- **🇨🇦 Canada**: [open.canada.ca](https://search.open.canada.ca/opendata/) - Antibody seroprevalence and rapid test data

## ⚡ Pipeline Workflows

### 1. **Data Ingestion Flow** (Daily @ Midnight)
```python
@flow(name="data-ingestion-pipeline")
def data_ingestion_flow():
    api_result = run_api_ingestion()          # Fetch from API(s)
    snowflake_result = run_minio_to_snowflake()  # Load MinIO contents to Bronze layer
```

### 2. **Staging Flow** (Daily @ 1 AM)
```python
@flow(name="dbt-staging-pipeline") 
def dbt_staging_flow():
    run_dbt_debug()        
    run_dbt_test_staging() # Data quality tests
    run_dbt_run_staging()  # Transform to Silver layer
```

### 3. **Cleaning Flow** (Daily @ 2 AM)
```python
@flow(name="dbt-cleaning-pipeline")
def dbt_cleaning_flow():
    run_dbt_debug()         
    run_dbt_test_cleaning()
    run_dbt_run_cleaning()  # Transform to Gold layer
```

## 🎯 Key Features

### **Medallion Architecture Implementation**
- **Bronze (Raw)**: Direct API ingestion with minimal processing
- **Silver (Staged)**: Validated, standardized data with quality tests
- **Gold (Cleaned)**: Business-ready datasets with timestamp auditing

### **Robust Data Quality**
- Automated data validation using dbt tests
- Negative value detection and handling
- Data quality scoring and rating systems
- Comprehensive logging and monitoring

### **Modern Orchestration**
- Prefect with async task execution
- Cron-based scheduling with dependency management
- Error handling with configurable retries
- Task-level logging and observability

### **CI/CD Integration**
- GitHub Actions for automated testing
- Code quality checks (Black, isort, flake8)
- Pytest test automation
- Pull request validation

## 🚀 Getting Started

### Prerequisites
```bash
# Clone the repository
git clone https://github.com/MichaelGalo/marvan-covid-data-pipeline-prefect
cd marvan-covid-data-pipeline-prefect

# Install dependencies
pip install -r requirements.txt
```

### Environment Setup
```bash
# Configure environment variables

# Configure your Snowflake, MinIO, and API credentials
```

### Running the Pipeline
```bash
# Start individual flows
python src/flows/ingestion.py    
python src/flows/staging.py     
python src/flows/cleaning.py   

# Or run dbt transformations directly
cd dbt/marvan_covid
dbt run --select models/staged
dbt run --select models/cleaned
```

## 📁 Project Structure

```
├── src/
│   ├── flows/               
│   ├── data_ingestion/     
│   └── logger.py           
├── dbt/marvan_covid/         
│   ├── models/
│   │   ├── staged/      
│   │   └── cleaned/       
│   └── tests/       
├── tests/                  
└── .github/workflows/  
```

## 📈 Data Quality & Testing

- **dbt Tests**: Automated validation for negative values, data integrity
- **Pytest**: Unit tests for Python components
- **GitHub Actions**: Continuous integration with quality gates
- **Logging**: JSON-structured logs with rotation and monitoring

---

**Built by [Michael Galo](https://github.com/MichaelGalo)** with original inspiration and contributions from an airflow project by [Alex Berka](https://github.com/alexberka), [Prahlad Siwokoti](https://github.com/siwa-p) and [Daniel Wallace](https://github.com/daniel-wallace-personal). 


