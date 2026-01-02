# event_driven_scheduling
# Event-Driven Scheduling with Apache Airflow 3.x
## Assets, Triggers, and Message Queue Integration

## Table of Contents
- [Overview](#overview)
- [What's New in Airflow 3.x?](#whats-new-in-airflow-3x)
- [Core Concepts](#core-concepts)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Project Structure](#project-structure)
- [Assets Deep Dive](#assets-deep-dive)
- [Triggers Deep Dive](#triggers-deep-dive)
- [Message Queue Integration](#message-queue-integration)
- [Implementation Examples](#implementation-examples)
- [Real-World Use Cases](#real-world-use-cases)
- [Asset Watchers](#asset-watchers)
- [Monitoring & Observability](#monitoring--observability)
- [Best Practices](#best-practices)
- [Migration Guide](#migration-guide)
- [Troubleshooting](#troubleshooting)
- [Performance Optimization](#performance-optimization)

## Overview

This repository demonstrates **Event-Driven Data Orchestration** using Apache Airflow 3.x's revolutionary new features:

- **Assets** (formerly Datasets): First-class data artifacts that trigger pipelines
- **Asset Watchers**: Monitor external systems for asset changes
- **Triggers**: Lightweight, async event listeners
- **Message Queue Integration**: Native support for Kafka, RabbitMQ, AWS SQS

**Perfect for Data Engineers building:**
- Real-time data pipelines triggered by data availability
- Event-driven architectures with Kafka/messaging systems
- Cross-team data contracts and lineage tracking
- Modern data mesh architectures

## What's New in Airflow 3.x?

### Assets (Evolution of Datasets)

Assets represent **data artifacts** in your data ecosystem. When a DAG produces an asset, downstream DAGs consuming that asset automatically trigger.

**Key Features:**
- ✅ **Automatic Scheduling**: Downstream DAGs trigger when upstream assets are ready
- ✅ **Data Lineage**: Built-in lineage tracking across DAGs and teams
- ✅ **Multi-Asset Dependencies**: Trigger on multiple asset combinations
- ✅ **Asset Metadata**: Rich metadata and versioning support
- ✅ **External Assets**: Monitor assets outside Airflow (S3, databases, etc.)

### Asset Watchers

New component that monitors external systems for asset changes without running DAGs.

**Capabilities:**
- Monitor S3 buckets for new files
- Watch database tables for new records
- Listen to message queues (Kafka, RabbitMQ, SQS)
- Custom watchers for any system

### Triggers & Message Queues

Native integration with message queues for true event-driven pipelines.

**Supported Systems:**
- Apache Kafka
- RabbitMQ
- AWS SQS
- Google Pub/Sub
- Azure Service Bus

## Core Concepts

### 1. Assets

Assets represent **data** - tables, files, API endpoints, anything your pipeline produces or consumes.

```python
from airflow import Asset

# Define an asset
raw_sales_data = Asset("s3://my-bucket/raw/sales/")
processed_sales = Asset("snowflake://db/schema/fact_sales")
```

### 2. Asset Producers

DAGs that **create or update** assets.

```python
with DAG(
    dag_id='extract_sales',
    schedule='@daily',
    outlets=[raw_sales_data]  # This DAG produces this asset
) as dag:
    # Tasks that create the asset
    pass
```

### 3. Asset Consumers

DAGs that **depend on** assets. They trigger when assets are ready.

```python
with DAG(
    dag_id='transform_sales',
    schedule=[raw_sales_data],  # Triggers when this asset is ready
    outlets=[processed_sales]
) as dag:
    # Tasks that consume the asset
    pass
```

### 4. Asset Watchers

Background processes that monitor external systems for asset changes.

```python
from airflow.assets.watchers import S3AssetWatcher

# Watch S3 for new files
watcher = S3AssetWatcher(
    asset_uri="s3://bucket/path/",
    bucket="my-bucket",
    prefix="data/",
    pattern="*.parquet"
)
```

### 5. Triggers

Async event listeners that don't block worker slots.

```python
from airflow.triggers.kafka import KafkaTrigger

# Listen to Kafka topic
trigger = KafkaTrigger(
    topic='user-events',
    bootstrap_servers='localhost:9092'
)
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     External Systems                          │
│   S3 Buckets  │  Databases  │  Kafka  │  APIs  │  SFTP      │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                   Asset Watchers                              │
│  • S3AssetWatcher - monitors buckets                         │
│  • DatabaseAssetWatcher - tracks table changes               │
│  • KafkaAssetWatcher - listens to topics                     │
│  • CustomAssetWatcher - your custom logic                    │
└─────────────────────────┬────────────────────────────────────┘
                          │ Asset State Changes
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                   Asset Registry                              │
│  • Tracks asset versions                                     │
│  • Maintains lineage graph                                   │
│  • Stores metadata                                           │
└─────────────────────────┬────────────────────────────────────┘
                          │ Triggers Dependent DAGs
                          ▼
┌──────────────────────────────────────────────────────────────┐
│              Airflow Scheduler (Asset-Aware)                 │
│  • Evaluates asset dependencies                              │
│  • Triggers consumer DAGs automatically                      │
│  • Manages parallel execution                                │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                   Consumer DAGs                               │
│  • Transform data                                            │
│  • Produce new assets                                        │
│  • Create lineage chain                                      │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│              Data Warehouse / Lake                            │
│    Snowflake  │  Redshift  │  BigQuery  │  Delta Lake       │
└──────────────────────────────────────────────────────────────┘
```

## Prerequisites

### Required Software (Windows)

```powershell
# Install Python 3.11+ (Airflow 3.x requirement)
# Download from python.org

# Install Docker Desktop for Windows
# Download from docker.com

# Install Git for Windows
# Download from git-scm.com

# Install PostgreSQL 14+ (for Airflow metadata)
# Download from postgresql.org

# Verify installations
python --version  # Should be 3.11+
docker --version
git --version
```

### Python Packages for Airflow 3.x

```powershell
# Create virtual environment
python -m venv airflow3_env
.\airflow3_env\Scripts\activate

# Install Airflow 3.x (alpha/beta as of now)
pip install apache-airflow==3.0.0a1

# Install providers
pip install apache-airflow-providers-amazon==9.0.0
pip install apache-airflow-providers-postgres==6.0.0
pip install apache-airflow-providers-apache-kafka==1.0.0
pip install apache-airflow-providers-http==5.0.0

# Additional packages
pip install boto3==1.34.0
pip install pandas==2.1.0
pip install sqlalchemy==2.0.23
pip install confluent-kafka==2.3.0
```

## Installation & Setup

### Step 1: Clone Repository

```powershell
git clone https://github.com/yourusername/airflow3-event-driven.git
cd airflow3-event-driven
```

### Step 2: Setup Airflow 3.x Environment

```powershell
# Set Airflow home
$env:AIRFLOW_HOME = "$PWD\airflow_home"

# Initialize database
airflow db init

# Enable experimental features for Airflow 3.x
$env:AIRFLOW__CORE__ENABLE_ASSET_TRIGGERED_DAGS = "True"
$env:AIRFLOW__CORE__ENABLE_ASSET_WATCHERS = "True"

# Create admin user
airflow users create `
    --username admin `
    --firstname Data `
    --lastname Engineer `
    --role Admin `
    --email data-eng@company.com `
    --password admin123
```

### Step 3: Configure Asset Watchers

Edit `airflow_home/airflow.cfg`:

```ini
[assets]
# Enable asset feature
enable_asset_triggered_dags = True

# Asset watcher configuration
asset_watcher_enabled = True
asset_watcher_interval = 30  # Check every 30 seconds

# Asset registry
asset_registry_backend = postgresql
```

### Step 4: Setup Message Queue (Kafka Example)

```powershell
# Using Docker for Kafka
docker-compose up -d kafka

# Verify Kafka is running
docker ps | Select-String kafka
```

**docker-compose.yaml** for Kafka:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### Step 5: Configure Connections

```powershell
# AWS Connection for S3 Asset Watchers
airflow connections add 'aws_default' `
    --conn-type 'aws' `
    --conn-login $env:AWS_ACCESS_KEY_ID `
    --conn-password $env:AWS_SECRET_ACCESS_KEY `
    --conn-extra '{"region_name": "us-east-1"}'

# Kafka Connection
airflow connections add 'kafka_default' `
    --conn-type 'kafka' `
    --conn-host 'localhost' `
    --conn-port 9092

# Snowflake Connection
airflow connections add 'snowflake_default' `
    --conn-type 'snowflake' `
    --conn-host 'account.snowflakecomputing.com' `
    --conn-login 'username' `
    --conn-password 'password' `
    --conn-schema 'PUBLIC' `
    --conn-extra '{"database": "ANALYTICS", "warehouse": "COMPUTE_WH"}'
```

### Step 6: Start Airflow Components

```powershell
# Terminal 1: Start Webserver
airflow webserver --port 8080

# Terminal 2: Start Scheduler (with asset awareness)
airflow scheduler

# Terminal 3: Start Triggerer (for async triggers)
airflow triggerer

# Terminal 4: Start Asset Watcher (NEW in Airflow 3.x)
airflow asset-watcher

# Access UI
# Browser: http://localhost:8080
# Username: admin
# Password: admin123
```

## Project Structure

```
airflow3-event-driven/
├── dags/
│   ├── 01_producer_s3_to_staging.py        # Produces raw asset
│   ├── 02_consumer_staging_to_warehouse.py # Consumes raw, produces clean
│   ├── 03_consumer_warehouse_to_mart.py    # Consumes clean, produces mart
│   ├── 04_kafka_triggered_dag.py           # Kafka message trigger
│   ├── 05_multi_asset_consumer.py          # Multiple asset dependencies
│   └── 06_conditional_asset_dag.py         # Conditional asset logic
├── asset_watchers/
│   ├── s3_watcher.py                       # S3 bucket watcher
│   ├── database_watcher.py                 # Database table watcher
│   ├── kafka_watcher.py                    # Kafka topic watcher
│   └── custom_api_watcher.py               # Custom API endpoint watcher
├── triggers/
│   ├── kafka_trigger.py                    # Kafka message trigger
│   ├── sqs_trigger.py                      # AWS SQS trigger
│   └── custom_webhook_trigger.py           # Custom HTTP webhook trigger
├── config/
│   ├── assets.yaml                         # Asset definitions
│   ├── watchers.yaml                       # Watcher configurations
│   └── airflow.cfg                         # Airflow settings
├── plugins/
│   ├── custom_assets/
│   │   └── snowflake_asset.py              # Custom asset type
│   └── custom_operators/
│       └── asset_aware_operator.py         # Asset-aware operators
├── tests/
│   ├── test_assets.py                      # Asset tests
│   ├── test_watchers.py                    # Watcher tests
│   └── test_dag_lineage.py                 # Lineage validation
├── docker/
│   ├── docker-compose.yaml                 # Full stack setup
│   └── Dockerfile.airflow3                 # Custom Airflow 3.x image
├── scripts/
│   ├── setup_assets.ps1                    # Initialize assets
│   ├── test_kafka.ps1                      # Test Kafka integration
│   └── trigger_asset_update.ps1            # Manually trigger asset update
├── docs/
│   ├── ASSETS_GUIDE.md                     # Complete asset guide
│   ├── WATCHERS_GUIDE.md                   # Watcher patterns
│   └── MIGRATION_FROM_2X.md                # Migration guide
├── requirements.txt
├── .env.example
└── README.md
```

## Assets Deep Dive

### Basic Asset Definition

```python
from airflow import Asset, DAG
from airflow.decorators import task
from datetime import datetime

# Define assets with URIs
raw_customer_data = Asset("s3://data-lake/raw/customers/")
clean_customer_data = Asset("postgres://db/analytics/dim_customers")
customer_metrics = Asset("snowflake://prod/metrics/customer_kpis")
```

### Producer DAG

```python
from airflow import DAG, Asset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the asset this DAG produces
raw_sales_asset = Asset(
    uri="s3://my-data-lake/raw/sales/{{ ds }}/",
    extra={
        "format": "parquet",
        "schema": "sales_v1",
        "owner": "data-engineering-team"
    }
)

def extract_sales_data(**context):
    """Extract sales data from source system"""
    import boto3
    import pandas as pd
    
    # Extract logic
    df = pd.read_sql("SELECT * FROM sales WHERE date = CURRENT_DATE", conn)
    
    # Save to S3
    s3_path = f"s3://my-data-lake/raw/sales/{context['ds']}/"
    df.to_parquet(s3_path)
    
    print(f"Extracted {len(df)} records to {s3_path}")
    return s3_path

with DAG(
    dag_id='producer_extract_sales',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    outlets=[raw_sales_asset],  # This DAG produces this asset
    tags=['producer', 'raw-layer', 'sales']
) as dag:
    
    extract = PythonOperator(
        task_id='extract_sales',
        python_callable=extract_sales_data
    )
```

### Consumer DAG

```python
from airflow import DAG, Asset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Assets this DAG depends on
raw_sales_asset = Asset("s3://my-data-lake/raw/sales/{{ ds }}/")

# Assets this DAG produces
clean_sales_asset = Asset(
    uri="snowflake://analytics/clean/fact_sales",
    extra={
        "table": "fact_sales",
        "schema": "clean",
        "sla_minutes": 30
    }
)

def transform_sales_data(**context):
    """Transform raw sales data"""
    import pandas as pd
    from snowflake.connector import connect
    
    # Read from S3 (asset produced by upstream)
    s3_path = f"s3://my-data-lake/raw/sales/{context['ds']}/"
    df = pd.read_parquet(s3_path)
    
    # Transformations
    df['total_amount'] = df['quantity'] * df['unit_price']
    df['processed_at'] = datetime.now()
    df = df[df['total_amount'] > 0]  # Data quality
    
    # Load to Snowflake
    conn = connect(...)
    df.to_sql('fact_sales', conn, schema='clean', if_exists='append')
    
    print(f"Transformed and loaded {len(df)} records")

with DAG(
    dag_id='consumer_transform_sales',
    start_date=datetime(2024, 1, 1),
    schedule=[raw_sales_asset],  # Triggers when this asset is ready!
    catchup=False,
    outlets=[clean_sales_asset],  # Produces this asset
    tags=['consumer', 'clean-layer', 'sales']
) as dag:
    
    transform = PythonOperator(
        task_id='transform_sales',
        python_callable=transform_sales_data
    )
```

### Multi-Asset Consumer

```python
from airflow import DAG, Asset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Multiple asset dependencies
sales_asset = Asset("snowflake://analytics/clean/fact_sales")
customer_asset = Asset("snowflake://analytics/clean/dim_customers")
product_asset = Asset("snowflake://analytics/clean/dim_products")

# Output asset
sales_mart_asset = Asset("snowflake://analytics/marts/sales_analytics")

def build_sales_mart(**context):
    """Build data mart from multiple assets"""
    from snowflake.connector import connect
    
    conn = connect(...)
    
    # Join all three assets
    query = """
        INSERT INTO marts.sales_analytics
        SELECT 
            s.sale_id,
            c.customer_name,
            p.product_name,
            s.total_amount,
            s.sale_date
        FROM clean.fact_sales s
        JOIN clean.dim_customers c ON s.customer_id = c.customer_id
        JOIN clean.dim_products p ON s.product_id = p.product_id
        WHERE s.processed_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
    """
    
    conn.cursor().execute(query)
    print("Sales mart built successfully")

with DAG(
    dag_id='consumer_multi_asset_mart',
    start_date=datetime(2024, 1, 1),
    schedule=[sales_asset, customer_asset, product_asset],  # ALL must be ready
    catchup=False,
    outlets=[sales_mart_asset],
    tags=['consumer', 'mart-layer', 'sales']
) as dag:
    
    build_mart = PythonOperator(
        task_id='build_sales_mart',
        python_callable=build_sales_mart
    )
```

### Asset with Conditional Logic

```python
from airflow import DAG, Asset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime

raw_data_asset = Asset("s3://lake/raw/transactions/")
valid_data_asset = Asset("s3://lake/validated/transactions/")
invalid_data_asset = Asset("s3://lake/rejected/transactions/")

def check_data_quality(**context):
    """Check if data passes quality rules"""
    import pandas as pd
    
    s3_path = f"s3://lake/raw/transactions/{context['ds']}/"
    df = pd.read_parquet(s3_path)
    
    # Quality checks
    null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns))
    duplicate_percentage = df.duplicated().sum() / len(df)
    
    if null_percentage < 0.05 and duplicate_percentage < 0.01:
        return 'process_valid_data'
    else:
        return 'quarantine_invalid_data'

def process_valid(**context):
    """Process valid data"""
    print("Data quality passed - processing")
    # Processing logic

def quarantine_invalid(**context):
    """Move invalid data to quarantine"""
    print("Data quality failed - quarantining")
    # Quarantine logic

with DAG(
    dag_id='conditional_asset_processing',
    start_date=datetime(2024, 1, 1),
    schedule=[raw_data_asset],
    catchup=False,
    tags=['quality', 'conditional']
) as dag:
    
    quality_check = BranchPythonOperator(
        task_id='check_quality',
        python_callable=check_data_quality
    )
    
    process_valid = PythonOperator(
        task_id='process_valid_data',
        python_callable=process_valid,
        outlets=[valid_data_asset]
    )
    
    quarantine = PythonOperator(
        task_id='quarantine_invalid_data',
        python_callable=quarantine_invalid,
        outlets=[invalid_data_asset]
    )
    
    quality_check >> [process_valid, quarantine]
```

## Triggers Deep Dive

### Kafka Trigger Example

```python
from airflow import DAG
from airflow.decorators import task
from airflow.triggers.kafka import KafkaTrigger
from datetime import datetime

def process_kafka_event(**context):
    """Process event from Kafka"""
    event_data = context['event']
    
    print(f"Processing Kafka event: {event_data}")
    
    # Your processing logic
    # - Parse event
    # - Transform
    # - Load to warehouse

with DAG(
    dag_id='kafka_triggered_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Triggered by Kafka events only
    catchup=False,
    tags=['kafka', 'event-driven', 'real-time']
) as dag:
    
    @task(trigger_rule='all_done')
    async def wait_for_kafka_message():
        """Wait for Kafka message using async trigger"""
        trigger = KafkaTrigger(
            topic='user-events',
            bootstrap_servers='localhost:9092',
            group_id='airflow-consumer',
            auto_offset_reset='latest',
            max_messages=1
        )
        
        async for event in trigger.run():
            return event.value
    
    @task
    def process_event(event_data):
        """Process the event"""
        print(f"Event received: {event_data}")
        # Your ETL logic
    
    event = wait_for_kafka_message()
    process_event(event)
```

### AWS SQS Trigger

```python
from airflow import DAG
from airflow.decorators import task
from airflow.triggers.sqs import SqsTrigger
from datetime import datetime

with DAG(
    dag_id='sqs_triggered_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['sqs', 'aws', 'event-driven']
) as dag:
    
    @task
    async def wait_for_sqs_message():
        """Wait for SQS message"""
        trigger = SqsTrigger(
            queue_url='https://sqs.us-east-1.amazonaws.com/123456789/my-queue',
            aws_conn_id='aws_default',
            max_messages=10,
            wait_time_seconds=20
        )
        
        async for event in trigger.run():
            return event.messages
    
    @task
    def process_messages(messages):
        """Process SQS messages"""
        for msg in messages:
            print(f"Processing: {msg['Body']}")
            # Your processing logic
    
    messages = wait_for_sqs_message()
    process_messages(messages)
```

## Message Queue Integration

### Kafka Consumer DAG

```python
from airflow import DAG, Asset
from airflow.decorators import task
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime

# Asset produced after processing Kafka messages
processed_events_asset = Asset("snowflake://analytics/events/user_activities")

def process_kafka_batch(**context):
    """Process batch of Kafka messages"""
    messages = context['ti'].xcom_pull(task_ids='consume_kafka')
    
    import pandas as pd
    from snowflake.connector import connect
    
    # Convert messages to DataFrame
    events = [eval(msg.value()) for msg in messages]
    df = pd.DataFrame(events)
    
    # Load to Snowflake
    conn = connect(...)
    df.to_sql('user_activities', conn, schema='events', if_exists='append')
    
    print(f"Processed {len(df)} events from Kafka")

with DAG(
    dag_id='kafka_to_snowflake',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',  # Or use trigger for real-time
    catchup=False,
    outlets=[processed_events_asset],
    tags=['kafka', 'streaming', 'snowflake']
) as dag:
    
    consume_kafka = ConsumeFromTopicOperator(
        task_id='consume_kafka',
        topic='user-events',
        kafka_config_id='kafka_default',
        max_messages=1000,
        max_batch_size=100
    )
    
    @task
    def process_batch(**context):
        return process_kafka_batch(**context)
    
    consume_kafka >> process_batch()
```

## Asset Watchers

### S3 Asset Watcher

```python
# asset_watchers/s3_watcher.py
from airflow.assets.watchers import AssetWatcher
from airflow import Asset
import boto3

class S3AssetWatcher(AssetWatcher):
    """Watch S3 bucket for new files"""
    
    def __init__(self, bucket_name, prefix, pattern="*"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.pattern = pattern
        self.s3 = boto3.client('s3')
        self.last_modified = None
    
    def check(self):
        """Check for new files in S3"""
        response = self.s3.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=self.prefix
        )
        
        if 'Contents' not in response:
            return []
        
        new_assets = []
        for obj in response['Contents']:
            if self.last_modified is None or obj['LastModified'] > self.last_modified:
                asset_uri = f"s3://{self.bucket_name}/{obj['Key']}"
                new_assets.append(Asset(uri=asset_uri))
                self.last_modified = obj['LastModified']
        
        return new_assets

# Register watcher in Airflow
# Place this in plugins/ folder
```

### Database Table Watcher

```python
# asset_watchers/database_watcher.py
from airflow.assets.watchers import AssetWatcher
from airflow import Asset
import psycopg2

class DatabaseTableWatcher(AssetWatcher):
    """Watch database table for new records"""
    
    def __init__(self, conn_id, table_name, timestamp_column):
        self.conn_id = conn_id
        self.table_name = table_name
        self.timestamp_column = timestamp_column
        self.last_check_time = None
    
    def check(self):
        """Check for new records"""
        from airflow.hooks.postgres_hook import PostgresHook
        
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        if self.last_check_time:
            query = f"""
                SELECT COUNT(*), MAX({self.timestamp_column})
                FROM {self.table_name}
                WHERE {self.timestamp_column} > %s
            """
            cursor.execute(query, (self.last_check_time,))
        else:
            query = f"""
                SELECT COUNT(*), MAX({self.timestamp_column})
                FROM {self.table_name}
            """
            cursor.execute(query)
        
        count, max_timestamp = cursor.fetchone()
        
        if count > 0:
            self.last_check_time = max_timestamp
            asset_uri = f"postgres://{self.conn_id}/{self.table_name}"
            return [Asset(uri=asset_uri)]
        
        return []
```

### Custom API Watcher

```python
# asset_watchers/custom_api_watcher.py
from airflow.assets.watchers import AssetWatcher
from airflow import Asset
import requests

class APIEndpointWatcher(AssetWatcher):
    """Watch API endpoint for data availability"""
    
    def __init__(self, api_url, auth_token):
        self.api_url = api_url
        self.auth_token = auth_token
        self.last_version = None
    
    def check(self):
        """Check API for new data version"""
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        response = requests.get(f"{self.api_url}/status", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            current_version = data.get('data_version')
            
            if current_version != self.last_version:
                self.last_version = current_version
                asset_uri = f"api://{self.api_url}/data/v{current_version}"
                return [Asset(uri=asset_uri)]
        
        return []
```

## Real-World Use Cases

### Use Case 1: Multi-Layer Data Lake Architecture

```python
# Layer 1: Raw Data Ingestion
from airflow import DAG, Asset
from airflow.decorators import task
from datetime import datetime

# Define assets for each layer
raw_layer = Asset("s3://lake/raw/customer_data/{{ ds }}/")
bronze_layer = Asset("s3://lake/bronze/customer_data/{{ ds }}/")
silver_layer = Asset("s3://lake/silver/customer_data/{{ ds }}/")
gold_layer = Asset("snowflake://prod/gold/customer_analytics")

# DAG 1: Ingest Raw Data (Producer)
with DAG(
    dag_id='layer_01_raw_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    outlets=[raw_layer],
    tags=['raw', 'ingestion']
) as dag:
    
    @task
    def extract_from_source(**context):
        """Extract from operational database"""
        import pandas as pd
        import boto3
        
        # Extract from source
        df = pd.read_sql("SELECT * FROM customers WHERE updated_at > NOW() - INTERVAL '1 hour'", conn)
        
        # Save to raw layer
        s3_path = f"s3://lake/raw/customer_data/{context['ds']}/"
        df.to_parquet(s3_path)
        
        return f"Extracted {len(df)} records to raw layer"
    
    extract_from_source()

# DAG 2: Bronze Layer Processing (Consumer -> Producer)
with DAG(
    dag_id='layer_02_bronze_processing',
    start_date=datetime(2024, 1, 1),
    schedule=[raw_layer],  # Triggers when raw layer ready
    outlets=[bronze_layer],
    tags=['bronze', 'cleansing']
) as dag:
    
    @task
    def cleanse_data(**context):
        """Cleanse and standardize data"""
        import pandas as pd
        
        # Read from raw
        s3_path = f"s3://lake/raw/customer_data/{context['ds']}/"
        df = pd.read_parquet(s3_path)
        
        # Data cleansing
        df = df.drop_duplicates()
        df = df.dropna(subset=['customer_id', 'email'])
        df['email'] = df['email'].str.lower().str.strip()
        df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)
        
        # Save to bronze
        bronze_path = f"s3://lake/bronze/customer_data/{context['ds']}/"
        df.to_parquet(bronze_path)
        
        return f"Cleansed {len(df)} records to bronze layer"
    
    cleanse_data()

# DAG 3: Silver Layer Transformation (Consumer -> Producer)
with DAG(
    dag_id='layer_03_silver_transformation',
    start_date=datetime(2024, 1, 1),
    schedule=[bronze_layer],  # Triggers when bronze ready
    outlets=[silver_layer],
    tags=['silver', 'transformation']
) as dag:
    
    @task
    def enrich_data(**context):
        """Enrich with business logic"""
        import pandas as pd
        
        # Read from bronze
        bronze_path = f"s3://lake/bronze/customer_data/{context['ds']}/"
        df = pd.read_parquet(bronze_path)
        
        # Business transformations
        df['customer_segment'] = df['total_spend'].apply(
            lambda x: 'Premium' if x > 10000 else 'Standard' if x > 1000 else 'Basic'
        )
        df['lifetime_value'] = df['total_spend'] * df['avg_order_frequency'] * 12
        df['churn_risk'] = (datetime.now() - df['last_purchase_date']).dt.days > 90
        
        # Save to silver
        silver_path = f"s3://lake/silver/customer_data/{context['ds']}/"
        df.to_parquet(silver_path)
        
        return f"Transformed {len(df)} records to silver layer"
    
    enrich_data()

# DAG 4: Gold Layer Analytics (Consumer)
with DAG(
    dag_id='layer_04_gold_analytics',
    start_date=datetime(2024, 1, 1),
    schedule=[silver_layer],  # Triggers when silver ready
    outlets=[gold_layer],
    tags=['gold', 'analytics']
) as dag:
    
    @task
    def build_analytics(**context):
        """Build aggregated analytics"""
        import pandas as pd
        from snowflake.connector import connect
        
        # Read from silver
        silver_path = f"s3://lake/silver/customer_data/{context['ds']}/"
        df = pd.read_parquet(silver_path)
        
        # Build analytics
        analytics = df.groupby('customer_segment').agg({
            'customer_id': 'count',
            'lifetime_value': 'mean',
            'total_spend': 'sum',
            'churn_risk': 'sum'
        }).reset_index()
        
        # Load to Snowflake gold layer
        conn = connect(...)
        analytics.to_sql('customer_analytics', conn, schema='gold', if_exists='replace')
        
        return f"Built analytics for {len(analytics)} segments"
    
    build_analytics()
```

### Use Case 2: Real-Time Event Processing with Kafka

```python
from airflow import DAG, Asset
from airflow.decorators import task
from airflow.triggers.kafka import KafkaTrigger
from datetime import datetime

# Assets
raw_events_asset = Asset("kafka://user-events/raw")
processed_events_asset = Asset("snowflake://analytics/events/user_behavior")
hourly_metrics_asset = Asset("snowflake://analytics/metrics/hourly_user_metrics")

# DAG 1: Kafka Stream Consumer
with DAG(
    dag_id='kafka_stream_processor',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Event-driven via Kafka
    outlets=[processed_events_asset],
    tags=['kafka', 'real-time', 'streaming']
) as dag:
    
    @task
    async def consume_kafka_stream():
        """Consume events from Kafka"""
        trigger = KafkaTrigger(
            topic='user-events',
            bootstrap_servers='localhost:9092',
            group_id='airflow-processor',
            max_messages=1000
        )
        
        events = []
        async for event in trigger.run():
            events.append(event.value)
            if len(events) >= 1000:  # Batch size
                break
        
        return events
    
    @task
    def process_events(events):
        """Process and load events"""
        import pandas as pd
        from snowflake.connector import connect
        
        # Convert to DataFrame
        df = pd.DataFrame(events)
        
        # Add processing metadata
        df['processed_at'] = datetime.now()
        df['processing_batch_id'] = context['run_id']
        
        # Enrich events
        df['event_hour'] = pd.to_datetime(df['timestamp']).dt.floor('H')
        df['user_segment'] = df['user_id'].apply(lambda x: hash(x) % 10)
        
        # Load to Snowflake
        conn = connect(...)
        df.to_sql('user_behavior', conn, schema='events', if_exists='append')
        
        print(f"Processed {len(df)} events")
        return len(df)
    
    events = consume_kafka_stream()
    process_events(events)

# DAG 2: Hourly Aggregation (Consumer of processed events)
with DAG(
    dag_id='hourly_metrics_aggregation',
    start_date=datetime(2024, 1, 1),
    schedule=[processed_events_asset],  # Triggers after events processed
    outlets=[hourly_metrics_asset],
    tags=['aggregation', 'metrics']
) as dag:
    
    @task
    def aggregate_hourly_metrics(**context):
        """Build hourly metrics from events"""
        from snowflake.connector import connect
        
        conn = connect(...)
        
        query = """
            INSERT INTO metrics.hourly_user_metrics
            SELECT 
                DATE_TRUNC('hour', timestamp) as metric_hour,
                user_segment,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_id) as sessions,
                AVG(CASE WHEN event_type = 'purchase' THEN amount END) as avg_purchase
            FROM events.user_behavior
            WHERE processed_at > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
        """
        
        conn.cursor().execute(query)
        print("Hourly metrics aggregated")
    
    aggregate_hourly_metrics()
```

### Use Case 3: Cross-Team Data Dependencies

```python
from airflow import DAG, Asset
from airflow.decorators import task
from datetime import datetime

# Team A - Marketing produces campaign data
marketing_campaigns_asset = Asset(
    uri="snowflake://marketing/campaigns/daily_performance",
    extra={"owner": "marketing-team", "sla_hours": 2}
)

# Team B - Sales produces transaction data
sales_transactions_asset = Asset(
    uri="snowflake://sales/transactions/daily_sales",
    extra={"owner": "sales-team", "sla_hours": 1}
)

# Team C - Analytics consumes both
combined_analytics_asset = Asset(
    uri="snowflake://analytics/reports/marketing_roi",
    extra={"owner": "analytics-team", "consumers": ["executives", "marketing"]}
)

# Team A DAG
with DAG(
    dag_id='marketing_campaign_etl',
    start_date=datetime(2024, 1, 1),
    schedule='0 8 * * *',  # 8 AM daily
    outlets=[marketing_campaigns_asset],
    tags=['team-marketing', 'campaigns']
) as dag:
    
    @task
    def extract_campaign_data(**context):
        """Extract from marketing platforms"""
        # Extract from Google Ads, Facebook Ads, etc.
        import pandas as pd
        from snowflake.connector import connect
        
        # Simulate extraction
        campaigns_df = pd.DataFrame({
            'campaign_id': range(100),
            'impressions': [1000] * 100,
            'clicks': [50] * 100,
            'cost': [100] * 100
        })
        
        conn = connect(...)
        campaigns_df.to_sql('daily_performance', conn, schema='campaigns', if_exists='replace')
        
        print(f"Loaded {len(campaigns_df)} campaigns")
    
    extract_campaign_data()

# Team B DAG
with DAG(
    dag_id='sales_transaction_etl',
    start_date=datetime(2024, 1, 1),
    schedule='0 9 * * *',  # 9 AM daily
    outlets=[sales_transactions_asset],
    tags=['team-sales', 'transactions']
) as dag:
    
    @task
    def extract_sales_data(**context):
        """Extract from sales systems"""
        import pandas as pd
        from snowflake.connector import connect
        
        # Extract from Salesforce, POS systems, etc.
        sales_df = pd.DataFrame({
            'transaction_id': range(500),
            'customer_id': range(500),
            'amount': [100] * 500,
            'campaign_id': [i % 100 for i in range(500)]
        })
        
        conn = connect(...)
        sales_df.to_sql('daily_sales', conn, schema='transactions', if_exists='replace')
        
        print(f"Loaded {len(sales_df)} transactions")
    
    extract_sales_data()

# Team C DAG - Depends on BOTH Team A and Team B
with DAG(
    dag_id='marketing_roi_analytics',
    start_date=datetime(2024, 1, 1),
    schedule=[marketing_campaigns_asset, sales_transactions_asset],  # BOTH required
    outlets=[combined_analytics_asset],
    tags=['team-analytics', 'cross-functional']
) as dag:
    
    @task
    def calculate_marketing_roi(**context):
        """Calculate ROI from both data sources"""
        from snowflake.connector import connect
        
        conn = connect(...)
        
        query = """
            INSERT INTO analytics.reports.marketing_roi
            SELECT 
                c.campaign_id,
                c.campaign_name,
                c.total_cost,
                SUM(s.amount) as total_revenue,
                (SUM(s.amount) - c.total_cost) / c.total_cost as roi,
                COUNT(s.transaction_id) as conversions,
                c.clicks / NULLIF(c.impressions, 0) as ctr
            FROM marketing.campaigns.daily_performance c
            LEFT JOIN sales.transactions.daily_sales s 
                ON c.campaign_id = s.campaign_id
            WHERE c.date = CURRENT_DATE() 
                AND s.date = CURRENT_DATE()
            GROUP BY 1, 2, 3
        """
        
        conn.cursor().execute(query)
        print("Marketing ROI calculated successfully")
    
    calculate_marketing_roi()
```

### Use Case 4: CDC (Change Data Capture) Pattern

```python
from airflow import DAG, Asset
from airflow.decorators import task
from airflow.providers.postgres.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Assets
source_changes_asset = Asset("postgres://source_db/cdc/customer_changes")
warehouse_asset = Asset("snowflake://warehouse/dim_customers")

# DAG 1: Detect Changes (CDC)
with DAG(
    dag_id='cdc_change_detection',
    start_date=datetime(2024, 1, 1),
    schedule='*/5 * * * *',  # Every 5 minutes
    outlets=[source_changes_asset],
    tags=['cdc', 'change-detection']
) as dag:
    
    detect_changes = SQLExecuteQueryOperator(
        task_id='detect_changes',
        conn_id='postgres_source',
        sql="""
            INSERT INTO cdc.customer_changes (customer_id, change_type, change_data, detected_at)
            SELECT 
                customer_id,
                CASE 
                    WHEN last_modified > NOW() - INTERVAL '5 minutes' THEN 'UPDATE'
                    WHEN created_at > NOW() - INTERVAL '5 minutes' THEN 'INSERT'
                END as change_type,
                row_to_json(customers.*) as change_data,
                NOW() as detected_at
            FROM customers
            WHERE last_modified > NOW() - INTERVAL '5 minutes'
               OR created_at > NOW() - INTERVAL '5 minutes'
        """
    )

# DAG 2: Apply Changes to Warehouse (Consumer)
with DAG(
    dag_id='cdc_apply_changes',
    start_date=datetime(2024, 1, 1),
    schedule=[source_changes_asset],  # Triggers when changes detected
    outlets=[warehouse_asset],
    tags=['cdc', 'warehouse-sync']
) as dag:
    
    @task
    def apply_cdc_changes(**context):
        """Apply CDC changes to Snowflake"""
        import psycopg2
        from snowflake.connector import connect
        
        # Read changes from CDC table
        pg_conn = psycopg2.connect(...)
        changes_df = pd.read_sql("""
            SELECT * FROM cdc.customer_changes 
            WHERE processed = false
            ORDER BY detected_at
        """, pg_conn)
        
        # Connect to Snowflake
        sf_conn = connect(...)
        cursor = sf_conn.cursor()
        
        for _, change in changes_df.iterrows():
            if change['change_type'] == 'INSERT':
                # Insert new record
                cursor.execute("""
                    INSERT INTO dim_customers (customer_id, name, email, updated_at)
                    VALUES (%s, %s, %s, %s)
                """, (change['customer_id'], ...))
            
            elif change['change_type'] == 'UPDATE':
                # Update existing record
                cursor.execute("""
                    UPDATE dim_customers 
                    SET name = %s, email = %s, updated_at = %s
                    WHERE customer_id = %s
                """, (..., change['customer_id']))
        
        # Mark changes as processed
        pg_conn.cursor().execute("""
            UPDATE cdc.customer_changes 
            SET processed = true 
            WHERE change_id = ANY(%s)
        """, (list(changes_df['change_id']),))
        
        pg_conn.commit()
        sf_conn.commit()
        
        print(f"Applied {len(changes_df)} changes to warehouse")
    
    apply_cdc_changes()
```

### Use Case 5: Data Quality Gateway Pattern

```python
from airflow import DAG, Asset
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

# Assets
raw_data_asset = Asset("s3://lake/raw/daily_transactions/")
quality_passed_asset = Asset("s3://lake/validated/daily_transactions/")
quality_failed_asset = Asset("s3://lake/quarantine/daily_transactions/")
warehouse_asset = Asset("snowflake://prod/fact_transactions")

# DAG: Data Quality Gateway
with DAG(
    dag_id='data_quality_gateway',
    start_date=datetime(2024, 1, 1),
    schedule=[raw_data_asset],  # Triggered by raw data arrival
    tags=['quality', 'validation', 'gateway']
) as dag:
    
    @task
    def run_quality_checks(**context):
        """Run comprehensive data quality checks"""
        import pandas as pd
        import great_expectations as ge
        
        # Load data
        s3_path = f"s3://lake/raw/daily_transactions/{context['ds']}/"
        df = pd.read_parquet(s3_path)
        
        # Convert to Great Expectations dataset
        ge_df = ge.from_pandas(df)
        
        # Define expectations
        results = {
            'row_count': ge_df.expect_table_row_count_to_be_between(min_value=1000, max_value=1000000),
            'no_nulls': ge_df.expect_column_values_to_not_be_null('transaction_id'),
            'valid_amounts': ge_df.expect_column_values_to_be_between('amount', min_value=0, max_value=100000),
            'valid_dates': ge_df.expect_column_values_to_be_dateutil_parseable('transaction_date'),
            'unique_ids': ge_df.expect_column_values_to_be_unique('transaction_id')
        }
        
        # Check if all passed
        all_passed = all([r['success'] for r in results.values()])
        
        # Store results
        context['ti'].xcom_push(key='quality_results', value=results)
        context['ti'].xcom_push(key='all_passed', value=all_passed)
        
        return all_passed
    
    @task.branch
    def decide_path(**context):
        """Decide whether to promote or quarantine"""
        all_passed = context['ti'].xcom_pull(task_ids='run_quality_checks', key='all_passed')
        
        if all_passed:
            return 'promote_to_validated'
        else:
            return 'quarantine_data'
    
    @task(outlets=[quality_passed_asset])
    def promote_to_validated(**context):
        """Promote data to validated layer"""
        import boto3
        
        s3 = boto3.client('s3')
        source = f"raw/daily_transactions/{context['ds']}/"
        destination = f"validated/daily_transactions/{context['ds']}/"
        
        # Copy to validated
        s3.copy_object(
            CopySource={'Bucket': 'lake', 'Key': source},
            Bucket='lake',
            Key=destination
        )
        
        print(f"Data promoted to validated layer: {destination}")
    
    @task(outlets=[quality_failed_asset])
    def quarantine_data(**context):
        """Move data to quarantine"""
        import boto3
        
        s3 = boto3.client('s3')
        source = f"raw/daily_transactions/{context['ds']}/"
        destination = f"quarantine/daily_transactions/{context['ds']}/"
        
        # Move to quarantine
        s3.copy_object(
            CopySource={'Bucket': 'lake', 'Key': source},
            Bucket='lake',
            Key=destination
        )
        
        # Send alert
        print(f"⚠️ Data quality failed! Moved to quarantine: {destination}")
        # Send email/Slack notification
    
    quality_check = run_quality_checks()
    decision = decide_path()
    promote = promote_to_validated()
    quarantine = quarantine_data()
    
    quality_check >> decision >> [promote, quarantine]

# DAG 2: Load to Warehouse (only if quality passed)
with DAG(
    dag_id='load_to_warehouse',
    start_date=datetime(2024, 1, 1),
    schedule=[quality_passed_asset],  # Only triggers if quality passed
    outlets=[warehouse_asset],
    tags=['warehouse', 'production']
) as dag:
    
    @task
    def load_validated_data(**context):
        """Load quality-validated data to warehouse"""
        import pandas as pd
        from snowflake.connector import connect
        
        # Load from validated layer
        s3_path = f"s3://lake/validated/daily_transactions/{context['ds']}/"
        df = pd.read_parquet(s3_path)
        
        # Load to Snowflake
        conn = connect(...)
        df.to_sql('fact_transactions', conn, if_exists='append', index=False)
        
        print(f"Loaded {len(df)} records to warehouse")
    
    load_validated_data()
```

## Monitoring & Observability

### Asset Lineage Visualization

Airflow 3.x provides built-in asset lineage visualization in the UI.

**Access Lineage:**
1. Go to Airflow UI → **Browse** → **Assets**
2. Click on any asset to see:
   - Producer DAGs (who creates this asset)
   - Consumer DAGs (who uses this asset)
   - Lineage graph across teams
   - Asset update history

### Monitoring Asset Updates

```python
from airflow import DAG, Asset
from airflow.decorators import task
from datetime import datetime

@task
def monitor_asset_health(**context):
    """Monitor asset freshness and health"""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.db import provide_session
    
    @provide_session
    def check_asset_freshness(session=None):
        # Query asset update times
        query = """
            SELECT 
                a.uri,
                MAX(dr.execution_date) as last_update,
                CURRENT_TIMESTAMP - MAX(dr.execution_date) as age
            FROM asset a
            JOIN asset_dag_ref adr ON a.id = adr.asset_id
            JOIN dag_run dr ON adr.dag_id = dr.dag_id
            WHERE dr.state = 'success'
            GROUP BY a.uri
            HAVING CURRENT_TIMESTAMP - MAX(dr.execution_date) > INTERVAL '24 hours'
        """
        
        stale_assets = session.execute(query).fetchall()
        
        if stale_assets:
            # Alert on stale assets
            for asset in stale_assets:
                print(f"⚠️ Stale Asset: {asset.uri} - Last updated {asset.age} ago")
        
        return len(stale_assets)
    
    stale_count = check_asset_freshness()
    return f"Found {stale_count} stale assets"

with DAG(
    dag_id='asset_health_monitor',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    tags=['monitoring', 'observability']
) as dag:
    
    monitor_asset_health()
```

### Asset Metadata Tracking

```python
from airflow import DAG, Asset
from airflow.decorators import task
from datetime import datetime

# Asset with rich metadata
customer_asset = Asset(
    uri="snowflake://prod/dim_customers",
    extra={
        "owner": "data-platform-team",
        "sla_hours": 2,
        "quality_score": 0.95,
        "row_count": 1000000,
        "last_schema_change": "2024-01-01",
        "consumers": ["marketing", "sales", "analytics"],
        "pii_fields": ["email", "phone", "address"],
        "retention_days": 730
    }
)

with DAG(
    dag_id='customer_dim_etl',
    schedule='@daily',
    outlets=[customer_asset]
) as dag:
    
    @task
    def update_asset_metadata(**context):
        """Update asset metadata after processing"""
        from airflow.models import Asset as AssetModel
        from airflow.utils.db import provide_session
        
        @provide_session
        def update_metadata(session=None):
            asset = session.query(AssetModel).filter(
                AssetModel.uri == customer_asset.uri
            ).first()
            
            if asset:
                # Update metadata
                asset.extra = {
                    **asset.extra,
                    "last_updated": datetime.now().isoformat(),
                    "updated_by_dag": context['dag'].dag_id,
                    "row_count": context['ti'].xcom_pull(key='row_count'),
                    "quality_score": context['ti'].xcom_pull(key='quality_score')
                }
                session.commit()
        
        update_metadata()
    
    update_asset_metadata()
```

## Best Practices

### 1. Asset Naming Conventions

```python
# ✅ GOOD: Clear, hierarchical naming
Asset("s3://bucket/layer/domain/entity/partition")
Asset("snowflake://database/schema/table")
Asset("postgres://host:port/database/schema/table")

# Examples
Asset("s3://datalake/raw/sales/transactions/2024-01-01/")
Asset("snowflake://prod/analytics/fact_orders")
Asset("kafka://cluster/topic/user-events")

# ❌ BAD: Unclear, non-hierarchical
Asset("my_data")
Asset("table1")
Asset("file")
```

### 2. Asset Granularity

```python
# ✅ GOOD: Right level of granularity
# Partition-level for large datasets
Asset("s3://lake/sales/date={{ ds }}/")

# Table-level for databases
Asset("snowflake://db/schema/fact_sales")

# ❌ BAD: Too granular (per-file)
Asset("s3://lake/sales/file001.parquet")
Asset("s3://lake/sales/file002.parquet")  # Don't do this!

# ❌ BAD: Too coarse (entire database)
Asset("snowflake://entire_database")  # Too broad!
```

### 3. Asset Metadata

```python
# ✅ GOOD: Rich metadata
asset = Asset(
    uri="snowflake://prod/analytics/customer_360",
    extra={
        "owner": "analytics-team@company.com",
        "sla_hours": 4,
        "update_frequency": "daily",
        "row_count_threshold": 100000,
        "schema_version": "v2.1",
        "pii": True,
        "retention_policy": "7 years",
        "cost_per_run": 2.50,
        "downstream_count": 5
    }
)

# ❌ BAD: No metadata
asset = Asset("snowflake://prod/analytics/customer_360")
```

### 4. Error Handling in Asset Watchers

```python
class RobustS3AssetWatcher(AssetWatcher):
    """S3 watcher with proper error handling"""
    
    def check(self):
        try:
            # Check S3
            response = self.s3.list_objects_v2(...)
            return self.process_response(response)
        
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                logger.error(f"Access denied to bucket: {self.bucket}")
            elif e.response['Error']['Code'] == '404':
                logger.warning(f"Bucket not found: {self.bucket}")
            else:
                logger.error(f"S3 error: {str(e)}")
            return []
        
        except Exception as e:
            logger.error(f"Unexpected error in S3 watcher: {str(e)}")
            return []
```

### 5. Testing Asset Dependencies

```python
# tests/test_asset_lineage.py
import pytest
from airflow.models import DagBag

def test_asset_lineage():
    """Test that asset dependencies are correct"""
    dagbag = DagBag(dag_folder='dags/')
    
    # Test producer-consumer relationship
    producer_dag = dagbag.get_dag('producer_extract_sales')
    consumer_dag = dagbag.get_dag('consumer_transform_sales')
    
    # Check producer outlets
    assert len(producer_dag.outlets) == 1
    assert producer_dag.outlets[0].uri == "s3://lake/raw/sales/"
    
    # Check consumer schedule
    assert len(consumer_dag.timetable.assets) == 1
    assert consumer_dag.timetable.assets[0].uri == "s3://lake/raw/sales/"

def test_no_circular_dependencies():
    """Ensure no circular asset dependencies"""
    dagbag = DagBag(dag_folder='dags/')
    
    # Build dependency graph
    asset_graph = {}
    for dag in dagbag.dags.values():
        if hasattr(dag, 'outlets') and hasattr(dag, 'timetable'):
            for outlet in dag.outlets:
                if outlet.uri not in asset_graph:
                    asset_graph[outlet.uri] = []
                for inlet in getattr(dag.timetable, 'assets', []):
                    asset_graph[outlet.uri].append(inlet.uri)
    
    # Check for cycles
    def has_cycle(graph, node, visited, rec_stack):
        visited.add(node)
        rec_stack.add(node)
        
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                if has_cycle(graph, neighbor, visited, rec_stack):
                    return True
