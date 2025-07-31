# Azure Retail Analytics Pipeline

An end-to-end, automated ETL pipeline built on Microsoft Azure to process e-commerce data from multiple sources, model it into a star schema, and serve it to Power BI for business intelligence and analytics.

## Project Overview

This project demonstrates a complete data engineering solution designed to provide a fictional e-commerce company, "FreshMart Online," with unified analytics across their sales, product, and customer data. The pipeline automatically ingests raw data from various sources, cleans and transforms it using Python, loads it into a structured data warehouse, and makes it available for visualization in Power BI.

The primary goal is to showcase a scalable, automated, and secure data architecture using modern cloud technologies, mirroring best practices used in enterprise environments.

## Architecture Diagram

The pipeline follows a modern ETL architecture, leveraging key Azure services for each stage of the data lifecycle.

```
[Raw Data Sources]      [Azure Data Lake Storage]      [Python Transformation]      [Azure SQL Database]      [Power BI]
(CSV, JSON files) --->     (Raw Data Zone)     --->      (pandas Script)     --->   (Star Schema)    ---> (Dashboard)
                          (Staging & Landing)          (Clean & Model Data)       (Data Warehouse)       (Analytics)
```

### Data Flow:

1. **Ingestion**: Raw data files (transactions, products, customers) are uploaded to Azure Data Lake Storage.
2. **Processing**: A Python script, orchestrated by Azure Data Factory, reads the data from the lake.
3. **Transformation**: The script cleans the data, handles missing values, calculates metrics like revenue, and models the data into a star schema (1 fact table, 3 dimension tables).
4. **Loading**: The transformed, clean data is loaded into an Azure SQL Database, which acts as the analytical data warehouse.
5. **Visualization**: Power BI connects to the Azure SQL Database to create interactive dashboards and reports for business users.

## Technical Stack

- **Cloud Provider**: Microsoft Azure
- **Data Storage**: Azure Data Lake Storage Gen2
- **Data Warehouse**: Azure SQL Database
- **Orchestration**: Azure Data Factory (for future automation)
- **Transformation**: Python 3.11 with pandas
- **BI & Visualization**: Microsoft Power BI
- **Secret Management**: Environment Variables (.env) & Azure Key Vault (planned)

## Key Features

- **End-to-End ETL**: Implements the full Extract, Transform, and Load process, from raw files to a query-optimized database.
- **Dimensional Modeling**: Transforms flat files into a robust Star Schema, which is the industry standard for efficient business analytics.
- **Secure Credential Management**: Uses environment variables via a .env file for local development, preventing hardcoded secrets in the source code.
- **Scalable Architecture**: Built on cloud services that can scale to handle massive volumes of data.
- **Automation-Ready**: The core logic is encapsulated in a single script, ready to be deployed and scheduled within Azure Data Factory.

## Data Schema

The final data model is a star schema, which consists of a central fact table surrounded by descriptive dimension tables.

### FactSales (Fact Table)
`InvoiceNo`, `date_key`, `customer_key`, `product_key`, `Quantity`, `UnitPrice`, `Revenue`

### DimCustomer (Dimension Table)
`customer_key`, `Country`, `RegistrationDate`, `CustomerSegment`, etc.

### DimProduct (Dimension Table)
`product_key`, `Description`, `Category`, `Brand`, etc.

### DimDate (Dimension Table)
`date_key`, `full_date`, `year`, `month`, `day`, `quarter`, `weekday`

## Setup and Installation

To run this project locally, follow these steps:

### Prerequisites:
- An Azure account with an active subscription.
- Python 3.9+
- Microsoft ODBC Driver 18 for SQL Server
- Power BI Desktop

### 1. Clone the Repository:
```bash
git clone https://github.com/debinnn/azure-retail-analytics.git
cd azure-retail-analytics
```

### 2. Set Up Azure Resources:
- Create an Azure Data Lake Storage Gen2 account.
- Create an Azure SQL Database.
- Configure firewall rules for your SQL Server to allow connections from your local IP.
- Upload the raw data files to the appropriate directories in your data lake.

### 3. Configure Environment Variables:
Create a file named `.env` in the root of the project.
Add your credentials to the `.env` file using `.env.example` as a template:

```bash
# .env file
STORAGE_CONNECTION_STRING="<Your Azure Storage Connection String>"
CONTAINER_NAME="raw-data"
DB_SERVER="<your_server_name>.database.windows.net"
DB_DATABASE="<your_database_name>"
DB_USERNAME="<your_sql_admin_username>"
DB_PASSWORD="<your_sql_admin_password>"
DB_DRIVER="{ODBC Driver 18 for SQL Server}"
```

### 4. Install Dependencies:
```bash
pip install -r requirements.txt
```
(Note: You may need to create a requirements.txt file by running `pip freeze > requirements.txt`)

### 5. Run the Pipeline:
```bash
python transform_retail_data.py
```
This will execute the ETL process and load the data into your Azure SQL Database.

## Power BI Dashboard Showcase

*(This is a placeholder section. You should replace the text below with screenshots of your actual Power BI dashboard.)*

The final dashboard provides key insights into sales performance, customer behavior, and product trends.

**Screenshot of Main KPI Dashboard:**
*Caption: The main dashboard highlights total revenue, orders, and active customers.*

## Future Improvements

- **Full Automation**: Implement the Azure Data Factory pipeline to run the Python script on a daily schedule.
- **Incremental Loading**: Modify the script to only process new or updated data since the last run, using watermarking on the InvoiceDate.
- **CI/CD**: Create a GitHub Actions workflow to automatically test and deploy changes.
- **Monitoring & Alerting**: Use Azure Monitor to track pipeline runs and send alerts on failures.
