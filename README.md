# Serverless Spark ETL Pipeline on AWS

## 📊 Overview

This project demonstrates an **event-driven, serverless data pipeline** built entirely on AWS managed services. It showcases how modern cloud architectures eliminate manual intervention in data processing workflows by using event triggers and automated orchestration.

**Core Concept:** Transform traditional batch processing into a reactive, automated pipeline where data arrival triggers immediate processing without human intervention.

---

## 🎯 Key Concepts Demonstrated

### 1. **Event-Driven Architecture**
Rather than scheduling jobs or manually triggering processes, the pipeline uses S3 events as triggers. When new data arrives, the system automatically responds—demonstrating the shift from poll-based to push-based processing patterns.

### 2. **Serverless Computing**
No servers to manage or provision. AWS Lambda provides compute on-demand, scaling automatically from zero to handle the workload, then scaling back down. Pay only for actual execution time.

### 3. **Distributed Data Processing with Spark**
AWS Glue runs Apache Spark jobs in a fully managed environment. Spark's distributed computing framework processes large datasets in parallel, demonstrating big data processing capabilities without infrastructure management.

### 4. **Data Lake Architecture**
S3 serves as a scalable, cost-effective data lake with separate zones (landing/raw → processed/curated), following best practices for data organization and lifecycle management.

### 5. **Infrastructure as Code Principles**
The pipeline embodies cloud-native design: loosely coupled services, separation of concerns, and declarative configuration that can be version-controlled and reproduced.

---

## 🏗️ Architecture

```
Raw Data Upload (S3) 
    ↓
Event Notification (S3 Event) 
    ↓
Lambda Trigger (Serverless Function)
    ↓
AWS Glue Job (Managed Spark)
    ↓
Processed Data Output (S3 Parquet)
```

**Data Flow:**
1. CSV file uploaded → `s3://handsonfinallanding/`
2. S3 event → triggers Lambda function
3. Lambda → starts AWS Glue ETL job
4. Glue (PySpark) → reads CSV, runs Spark SQL analytics, writes Parquet
5. Results → stored in `s3://handsonfinalprocessed/`

---

## 🛠️ Technology Stack

| Component | AWS Service | Purpose |
|-----------|-------------|---------|
| **Storage** | S3 | Data lake for raw and processed data |
| **Compute** | Lambda | Serverless orchestration trigger |
| **ETL Engine** | Glue | Managed Apache Spark for data transformation |
| **Language** | PySpark | Distributed data processing with Spark SQL |
| **Security** | IAM | Role-based access control |

---

## 📋 Implementation Summary

### Resources Created:
- **2 S3 Buckets:** Landing zone and processed zone
- **IAM Role:** Glue service role with S3 access
- **Glue Job:** PySpark script for ETL processing
- **Lambda Function:** Event handler to trigger Glue
- **S3 Event Trigger:** Connects S3 to Lambda

### Pipeline Execution:
1. Upload `reviews.csv` to landing bucket
2. Automatic trigger starts processing pipeline
3. Glue job executes Spark SQL queries:
   - Daily review counts by product
   - Top 5 customers by review activity
   - Rating distribution analysis
4. Results written as Parquet files to processed bucket

### Analytical Outputs:
- **processed-data/**: Complete cleaned dataset
- **Athena Results/daily_review_counts/**: Time-series aggregation
- **Athena Results/top_5_customers/**: Customer analytics
- **Athena Results/rating_distribution/**: Statistical breakdown

---

## 📸 Implementation Evidence

Screenshots documenting the complete implementation are available showing:
- Lambda execution logs

- Glue job script and status

- Glue job monitor logs

- Final output data in processed bucket

---

## 🔑 Key Takeaways

1. **Automation over Manual Processes:** Event-driven design eliminates human intervention
2. **Scalability:** Serverless architecture scales automatically with workload
3. **Cost Efficiency:** Pay-per-use model with no idle resource costs
4. **Separation of Concerns:** Each service handles a specific responsibility
5. **Production-Ready Pattern:** This architecture scales from prototype to production workloads

---

## 🧹 Resource Cleanup

To avoid ongoing charges, delete resources in reverse order:
1. Empty and delete S3 buckets
2. Delete Lambda function
3. Delete Glue job
4. Delete IAM roles# Serverless-Spark-ETL-Handson
