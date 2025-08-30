# Retail Data Pipeline: End-to-End ETL Workflow for Retail Analytics

## Overview 
The pipeline processes raw retail transaction data, performs cleaning, loads it into a MySQL database, generates location-wise and store-wise profit analyses, visualizes insights, uploads results to Amazon S3, and sends automated email notifications with reports.
Key achievements:
1. Automated ETL Process: Reduced manual data handling by 100% using Apache Airflow for orchestration.
2. Actionable Insights: Calculated profits (SP - CP) with SQL queries, visualized trends via bar charts, enabling data-driven decisions for retail operations.
3. Scalability & Reliability: Integrated cloud storage (S3) and error-handling branches for robust, production-ready workflows.
4. Real-World Impact: Demonstrates proficiency in building scalable analytics pipelines, aligning with Google Analytics principles for tracking KPIs like sales trends and profitability.

## Motivation
In retail, timely analysis of transaction data is crucial for optimizing profits and operations. This pipeline addresses common challenges like data inconsistencies (e.g., special characters in locations, currency formatting), manual reporting, and lack of automation. By leveraging tools from the Google Analytics ecosystem, it enables stakeholders to monitor daily profits by store and location, supporting strategic decisions such as inventory management and regional performance evaluation.

## Key Features
1. Data Ingestion & Cleaning: Loads raw CSV data, removes special characters (e.g., from "New York(" to "New York"), extracts numeric values from strings (e.g., "$31" to 31.0), and adds a process_date column for tracking.
2. Database Management: Creates a MySQL table (daily_transactions) if not exists and bulk-inserts cleaned data using PyMySQL.
3. Profit Analysis & Visualization: Executes SQL queries for aggregated profits (e.g., SUM(SP - CP) grouped by date/store/location), generates CSV reports and Matplotlib bar charts.
4. Cloud Integration: Uploads processed files (CSVs, PNGs) to AWS S3 using Airflow hooks for secure, scalable storage.
5. Notification System: Branches workflow to send emails via SMTP—success emails with attachments or no-data alerts—ensuring stakeholders stay informed.
6. Error Handling & Branching: Uses Airflow's BranchPythonOperator to check file existence and trigger appropriate paths.
7. Reproducibility: Daily scheduling via Airflow DAGs, with focus on modularity for easy extension (e.g., adding more analyses).

## Technologies & Tools

1. Core Stack:
- Python: For scripting, data manipulation, and custom functions.
- Apache Airflow: Workflow orchestration with DAGs, operators (PythonOperator, MySqlOperator, BranchPythonOperator).
- MySQL: Relational database for structured storage and querying.

2. Libraries:
- Pandas & NumPy: Data cleaning and transformation.
- Matplotlib: Generating visual reports (bar charts).
- boto3 (via Airflow S3Hook): AWS S3 uploads.
- smtplib & MIME: Email notifications with attachments.
- re (Regular Expressions): Parsing and cleaning strings.

Other: AWS credentials for S3, Gmail SMTP for emails (use app-specific passwords for security).

# Setup & Installation
To run this pipeline locally or in a production environment:
1. Prerequisites:
- Python 3.8+ and Apache Airflow 2.0+ installed.
- MySQL server running (configure connection ID: mysql_8 in Airflow).
- AWS account with S3 bucket (projectdata12) and credentials (connection ID: s3_conn).
- Gmail account for SMTP (update placeholders in Retail_Data_Pipeline.py).

2. Clone the Repository: git clone https://github.com/komal270302/Retail-Data-Pipeline---End-to-End-Workflow.git, 
                         cd Retail-Data-Pipeline---End-to-End-Workflow

3. Install Dependencies: pip install apache-airflow pandas numpy pymysql boto3 matplotlib

4. Configure Airflow:
- Initialize Airflow: airflow db init.
- Start scheduler and webserver: airflow scheduler & airflow webserver.
- Add connections via Airflow UI: MySQL details and AWS keys.
- Place files in Airflow's dags/ folder and update paths (e.g., /dags/retail_Project/).

5. Run the Pipeline:
- Trigger the DAG Retail_Data_Pipeline in Airflow UI.
- Monitor tasks: Data cleaning → Table creation → Loading → Analysis → S3 Upload → Email.

7. Sample Results
- Cleaned Data Example: From raw input like "New York(" and "$31", outputs standardized "New York" and 31.0.
- Profit Insights: For sample data on 2019-11-26, total profit ~$150 across stores, visualized in bar charts.
- Outputs: CSVs (e.g., location-wise-analysis_29_08_2025.csv), PNG charts, and S3 uploads.
- Email Notification: Automated alerts with attachments for easy sharing.

# Contributor 
Komal (komal202220@gmail.com)
