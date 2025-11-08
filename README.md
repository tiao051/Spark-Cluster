# Spark Cluster Setup

A Docker-based Apache Spark cluster with Hadoop infrastructure for distributed data processing and analysis.

## Prerequisites

- Docker
- Docker Compose
- PowerShell (for Windows) or Bash (for Linux/macOS)

## Architecture

This setup includes:
- **Spark Master** (3.3.0)
- **Spark Worker** (3.3.0)
- **Hadoop Namenode** (3.2.1)
- **Hadoop Datanode** (3.2.1)

All services communicate through a bridge network and share mounted volumes for code execution.

## Getting Started

### 1. Start the Cluster

Navigate to the project directory and start all services:

```powershell
docker-compose up -d
```

Verify all containers are running:

```powershell
docker ps
```

### 2. Access Spark UI

Once the cluster is running, access the Spark Master UI:

```
http://localhost:8080
```

Access Hadoop Namenode UI:

```
http://localhost:9870
```

## Running Code

### Option A: Interactive Scala Shell

Launch the Scala REPL connected to the Spark cluster:

```powershell
docker exec -it spark-master /spark/bin/spark-shell --master spark://spark-master:7077
```

Type your Scala code directly:

```scala
val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
rdd.collect().foreach(println)
```

Exit with `:quit`

### Option B: Submit Python Scripts

Create a Python script (e.g., `analysis.py`):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MyApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print(f"Result: {rdd.collect()}")
```

Submit it to the cluster:

```powershell
docker exec spark-master /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-java-options "-Dlog4j.configuration=file:/workspace/log4j.properties" `
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/workspace/log4j.properties" `
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/workspace/log4j.properties" `
  /workspace/analysis.py
```

### Option C: Load Scala Scripts in REPL

Create a Scala script file and load it in the Scala shell:

```scala
scala> :load /workspace/script.scala
```

## File Organization

All files in the project directory are automatically mounted to `/workspace` inside the containers, making them accessible to Spark jobs.

## Stopping the Cluster

To stop all services:

```powershell
docker-compose down
```

To stop and remove all data:

```powershell
docker-compose down -v
```

## Network

All services communicate through the `bigdata_network` bridge network. Services can reach each other using their container names (e.g., `spark-master:7077`).

