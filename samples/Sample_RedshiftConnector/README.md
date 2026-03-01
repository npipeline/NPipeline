# Sample: AWS Redshift Connector

Demonstrates streaming reads from Redshift and high-throughput writes using `COPY FROM S3` with upsert support.

## Prerequisites

- AWS Redshift cluster (provisioned or serverless)
- A database with `order_events` table
- S3 bucket for COPY staging files
- IAM role attached to Redshift with S3 read/write permissions

## Setup

Create the source table:

```sql
CREATE TABLE public.order_events (
    order_id BIGINT,
    customer_id VARCHAR(50),
    product_sku VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    ordered_at TIMESTAMP,
    status VARCHAR(20)
);

-- Insert sample data
INSERT INTO public.order_events VALUES
    (1, 'C001', 'SKU-001', 2, 29.99, '2024-01-01', 'completed'),
    (2, 'C002', 'SKU-002', 1, 49.99, '2024-01-02', 'shipped');
```

Create the target table:

```sql
CREATE TABLE public.order_summaries (
    order_id BIGINT PRIMARY KEY,
    customer_id VARCHAR(50),
    revenue DECIMAL(10,2),
    item_count INT,
    ordered_at TIMESTAMP,
    status VARCHAR(20),
    processed_at TIMESTAMP
);
```

## Environment Variables

| Variable                               | Description                         |
|----------------------------------------|-------------------------------------|
| `NPIPELINE_REDSHIFT_CONNECTION_STRING` | Npgsql connection string            |
| `NPIPELINE_REDSHIFT_S3_BUCKET`         | S3 bucket used for COPY staging     |
| `NPIPELINE_REDSHIFT_IAM_ROLE`          | IAM role ARN Redshift uses for COPY |
| `AWS_DEFAULT_REGION`                   | AWS region (default: `us-east-1`)   |

## Run

```bash
export NPIPELINE_REDSHIFT_CONNECTION_STRING="Host=my-cluster.us-east-1.redshift.amazonaws.com;Port=5439;Database=analytics;Username=etl;Password=secret;SSL Mode=Require"
export NPIPELINE_REDSHIFT_S3_BUCKET="my-etl-bucket"
export NPIPELINE_REDSHIFT_IAM_ROLE="arn:aws:iam::123456789012:role/RedshiftS3AccessRole"
dotnet run
```

## What It Does

1. Reads from `order_events` table with streaming
2. Transforms each row to compute revenue
3. Writes to `order_summaries` with `COPY FROM S3` upsert on `order_id`
